//! TLS for the MorletDB (PostgreSQL) connection.
//!
//! The connection string keeps libpq syntax (URL or `key=value`). The TLS
//! parameters are taken out of it here, because the PostgreSQL client only
//! knows `disable`/`prefer`/`require` and has no certificate options:
//!
//! - `sslmode`: `disable`, `require`, `verify-ca` or `verify-full` (`allow`
//!   and `prefer`, which silently fall back to plain text, are rejected).
//!   Without `sslmode`, loopback hosts and Unix sockets use `disable` (as
//!   before) and any other host uses `verify-full`.
//! - `sslrootcert`: PEM bundle of trusted CAs; `system` or absent means the
//!   platform trust store. As in libpq, `require` with an explicit
//!   `sslrootcert` verifies the chain like `verify-ca`; `require` without it
//!   encrypts but does not authenticate the server.
//! - `sslcert` + `sslkey`: client certificate (both or neither).
//!
//! One rustls (0.23, ring) as for MQTT and replay.

use crate::tls;
use rustls::DigitallySignedStruct;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    Disable,
    /// Encrypted; the server is not authenticated.
    Require,
    /// Certificate chain verified, host name not checked.
    VerifyCa,
    /// Chain and host name verified.
    VerifyFull,
}

impl SslMode {
    pub fn as_str(self) -> &'static str {
        match self {
            SslMode::Disable => "disable",
            SslMode::Require => "require",
            SslMode::VerifyCa => "verify-ca",
            SslMode::VerifyFull => "verify-full",
        }
    }
}

/// A parsed connection string: client config without TLS parameters, the
/// effective mode, and the rustls config when TLS is used.
pub struct PgConnect {
    pub config: postgres::Config,
    pub mode: SslMode,
    /// Whether `sslmode` was given (false: chosen from the host).
    pub explicit: bool,
    pub tls: Option<rustls::ClientConfig>,
}

impl std::fmt::Debug for PgConnect {
    // Never print the config: it can hold a password.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgConnect")
            .field("mode", &self.mode)
            .field("explicit", &self.explicit)
            .finish_non_exhaustive()
    }
}

impl PgConnect {
    pub fn parse(conninfo: &str) -> Result<Self, String> {
        let (rest, p) = extract(conninfo)?;
        let mut config: postgres::Config = rest
            .parse()
            .map_err(|e| format!("invalid MorletDB connection string: {e}"))?;
        let explicit = p.sslmode.is_some();
        let mode = match p.sslmode.as_deref() {
            None if local_only(&config) => SslMode::Disable,
            None => SslMode::VerifyFull,
            Some("disable") => SslMode::Disable,
            Some("require") => SslMode::Require,
            Some("verify-ca") => SslMode::VerifyCa,
            Some("verify-full") => SslMode::VerifyFull,
            Some(m @ ("allow" | "prefer")) => {
                return Err(format!(
                    "sslmode={m} may fall back to an unencrypted connection; use require, verify-ca, verify-full or disable"
                ));
            }
            Some(m) => return Err(format!("unknown sslmode {m:?}")),
        };
        if mode == SslMode::Disable {
            if p.sslrootcert.is_some() || p.sslcert.is_some() || p.sslkey.is_some() {
                return Err("sslrootcert/sslcert/sslkey given with sslmode=disable".into());
            }
            config.ssl_mode(postgres::config::SslMode::Disable);
            return Ok(PgConnect {
                config,
                mode,
                explicit,
                tls: None,
            });
        }
        if config
            .get_hosts()
            .iter()
            .any(|h| !matches!(h, postgres::config::Host::Tcp(_)))
        {
            return Err(format!(
                "sslmode={} needs a TCP host; Unix sockets do not use TLS (use sslmode=disable)",
                mode.as_str()
            ));
        }
        config.ssl_mode(postgres::config::SslMode::Require);
        let tls = client_config(mode, &p)?;
        Ok(PgConnect {
            config,
            mode,
            explicit,
            tls: Some(tls),
        })
    }

    pub fn connect(&self) -> Result<postgres::Client, postgres::Error> {
        match &self.tls {
            None => self.config.connect(postgres::NoTls),
            Some(c) => self
                .config
                .connect(tokio_postgres_rustls::MakeRustlsConnect::new(c.clone())),
        }
    }
}

/// Every host is loopback or a Unix socket (no host means the default
/// socket, as in libpq).
fn local_only(config: &postgres::Config) -> bool {
    config.get_hosts().iter().all(|h| match h {
        postgres::config::Host::Tcp(name) => {
            name.eq_ignore_ascii_case("localhost")
                || name
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        }
        #[cfg(unix)]
        postgres::config::Host::Unix(_) => true,
    })
}

#[derive(Debug, Default, PartialEq)]
struct TlsParams {
    sslmode: Option<String>,
    sslrootcert: Option<String>,
    sslcert: Option<String>,
    sslkey: Option<String>,
}

impl TlsParams {
    /// Returns false for parameters that are not ours.
    fn set(&mut self, key: &str, value: String) -> Result<bool, String> {
        let slot = match key {
            "sslmode" => &mut self.sslmode,
            "sslrootcert" => &mut self.sslrootcert,
            "sslcert" => &mut self.sslcert,
            "sslkey" => &mut self.sslkey,
            _ => return Ok(false),
        };
        if slot.is_some() {
            return Err(format!("{key} given twice in the connection string"));
        }
        *slot = Some(value);
        Ok(true)
    }
}

/// Split the TLS parameters off a libpq connection string.
fn extract(conninfo: &str) -> Result<(String, TlsParams), String> {
    let mut p = TlsParams::default();
    let s = conninfo.trim();
    if s.starts_with("postgres://") || s.starts_with("postgresql://") {
        let Some((base, query)) = s.split_once('?') else {
            return Ok((s.to_string(), p));
        };
        let mut keep = Vec::new();
        for pair in query.split('&').filter(|x| !x.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            if !p.set(&percent_decode(k)?, percent_decode(v)?)? {
                keep.push(pair);
            }
        }
        let rest = if keep.is_empty() {
            base.to_string()
        } else {
            format!("{base}?{}", keep.join("&"))
        };
        return Ok((rest, p));
    }
    // key=value form: values may be single-quoted with backslash escapes.
    let mut kept = String::new();
    let mut it = s.char_indices().peekable();
    while let Some(&(start, c)) = it.peek() {
        if c.is_whitespace() {
            it.next();
            continue;
        }
        let mut key = String::new();
        while let Some(&(_, c)) = it.peek() {
            if c == '=' || c.is_whitespace() {
                break;
            }
            key.push(c);
            it.next();
        }
        while it.peek().is_some_and(|(_, c)| c.is_whitespace()) {
            it.next();
        }
        if it.next().map(|(_, c)| c) != Some('=') {
            return Err(format!(
                "invalid MorletDB connection string: missing '=' after {key:?}"
            ));
        }
        while it.peek().is_some_and(|(_, c)| c.is_whitespace()) {
            it.next();
        }
        let mut value = String::new();
        if it.peek().is_some_and(|(_, c)| *c == '\'') {
            it.next();
            loop {
                match it.next() {
                    None => {
                        return Err("invalid MorletDB connection string: unterminated quote".into());
                    }
                    Some((_, '\'')) => break,
                    Some((_, '\\')) => match it.next() {
                        Some((_, c)) => value.push(c),
                        None => {
                            return Err("invalid MorletDB connection string: trailing '\\'".into());
                        }
                    },
                    Some((_, c)) => value.push(c),
                }
            }
        } else {
            while let Some(&(_, c)) = it.peek() {
                if c.is_whitespace() {
                    break;
                }
                it.next();
                if c == '\\' {
                    if let Some((_, c)) = it.next() {
                        value.push(c);
                    }
                } else {
                    value.push(c);
                }
            }
        }
        let end = it.peek().map_or(s.len(), |&(i, _)| i);
        if !p.set(&key, value)? {
            if !kept.is_empty() {
                kept.push(' ');
            }
            kept.push_str(&s[start..end]);
        }
    }
    Ok((kept, p))
}

fn percent_decode(s: &str) -> Result<String, String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = s
                .get(i + 1..i + 3)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or("invalid percent-encoding in the connection string")?;
            out.push(hex);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| "connection string parameter is not UTF-8".into())
}

fn client_config(mode: SslMode, p: &TlsParams) -> Result<rustls::ClientConfig, String> {
    let ca: Option<PathBuf> = match p.sslrootcert.as_deref() {
        None | Some("system") => None,
        Some(path) => Some(PathBuf::from(path)),
    };
    let builder = rustls::ClientConfig::builder_with_provider(tls::provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS setup: {e}"))?;
    // libpq: `require` with an explicit root certificate verifies like verify-ca.
    let mode = match mode {
        SslMode::Require if p.sslrootcert.is_some() => SslMode::VerifyCa,
        m => m,
    };
    let builder = match mode {
        SslMode::VerifyFull => builder.with_root_certificates(tls::root_store(ca.as_deref())?),
        SslMode::VerifyCa | SslMode::Require => {
            let verifier: Arc<dyn ServerCertVerifier> = if mode == SslMode::VerifyCa {
                let inner = rustls::client::WebPkiServerVerifier::builder_with_provider(
                    Arc::new(tls::root_store(ca.as_deref())?),
                    tls::provider(),
                )
                .build()
                .map_err(|e| format!("TLS setup: {e}"))?;
                Arc::new(ChainOnly(inner))
            } else {
                Arc::new(Unauthenticated(tls::provider()))
            };
            builder
                .dangerous()
                .with_custom_certificate_verifier(verifier)
        }
        SslMode::Disable => unreachable!("no TLS config for sslmode=disable"),
    };
    match (&p.sslcert, &p.sslkey) {
        (None, None) => Ok(builder.with_no_client_auth()),
        (Some(cert), Some(key)) => builder
            .with_client_auth_cert(
                tls::load_certs(cert.as_ref(), "sslcert")?,
                tls::load_key(key.as_ref(), "sslkey")?,
            )
            .map_err(|e| format!("client certificate: {e}")),
        _ => Err("sslcert and sslkey must be given together".into()),
    }
}

/// verify-ca: the chain must verify; a host name mismatch is accepted.
#[derive(Debug)]
struct ChainOnly(Arc<rustls::client::WebPkiServerVerifier>);

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
        {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForName
                | rustls::CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            r => r,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

/// require without a root certificate: encrypt, do not authenticate the
/// server (libpq semantics). Handshake signatures are still checked.
#[derive(Debug)]
struct Unauthenticated(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for Unauthenticated {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(conninfo: &str) -> (String, TlsParams) {
        extract(conninfo).unwrap()
    }

    #[test]
    fn url_parameters_are_split_off() {
        let (rest, p) = params(
            "postgresql://u:p%40ss@db.example:5432/morlet?sslmode=verify-full&application_name=x&sslrootcert=%2Fetc%2Fca.pem",
        );
        assert_eq!(
            rest,
            "postgresql://u:p%40ss@db.example:5432/morlet?application_name=x"
        );
        assert_eq!(p.sslmode.as_deref(), Some("verify-full"));
        assert_eq!(p.sslrootcert.as_deref(), Some("/etc/ca.pem"));
        let (rest, p) = params("postgres://h/db?sslmode=disable");
        assert_eq!(
            (rest.as_str(), p.sslmode.as_deref()),
            ("postgres://h/db", Some("disable"))
        );
    }

    #[test]
    fn key_value_parameters_are_split_off() {
        let (rest, p) = params(
            "host=db.example  sslmode = require password='a b\\'c' sslcert='/x y/c.pem' sslkey=/k\\ ey dbname=morlet",
        );
        assert_eq!(rest, "host=db.example password='a b\\'c' dbname=morlet");
        assert_eq!(p.sslmode.as_deref(), Some("require"));
        assert_eq!(p.sslcert.as_deref(), Some("/x y/c.pem"));
        assert_eq!(p.sslkey.as_deref(), Some("/k ey"));
        assert!(extract("host=h sslmode=require sslmode=disable").is_err());
        assert!(extract("host=h password='open").is_err());
    }

    #[test]
    fn default_mode_follows_the_host() {
        let m = |s: &str| PgConnect::parse(s).map(|c| (c.mode, c.explicit));
        assert_eq!(
            m("postgresql://u@127.0.0.1:5432/db").unwrap(),
            (SslMode::Disable, false)
        );
        assert_eq!(
            m("host=localhost dbname=db").unwrap(),
            (SslMode::Disable, false)
        );
        assert_eq!(
            m("postgresql://u@[::1]/db").unwrap(),
            (SslMode::Disable, false)
        );
        assert_eq!(
            m("host=/var/run/postgresql").unwrap(),
            (SslMode::Disable, false)
        );
        // A remote host defaults to verify-full (platform trust store).
        if let Ok(c) = m("postgresql://u@db.example/db") {
            assert_eq!(c, (SslMode::VerifyFull, false));
        }
        assert_eq!(
            m("postgresql://u@db.example/db?sslmode=disable").unwrap(),
            (SslMode::Disable, true)
        );
        assert!(m("postgresql://u@db.example/db?sslmode=prefer").is_err());
        assert!(m("postgresql://u@db.example/db?sslmode=bogus").is_err());
        assert!(m("host=/tmp sslmode=require").is_err());
        assert!(m("postgresql://u@h/db?sslmode=require&sslcert=/c.pem").is_err());
        assert!(m("postgresql://u@h/db?sslmode=disable&sslrootcert=/ca.pem").is_err());
        let c = PgConnect::parse("postgresql://u:secret@h/db?sslmode=require").unwrap();
        assert!(!format!("{c:?}").contains("secret"));
    }
}

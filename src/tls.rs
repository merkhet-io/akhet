//! PEM loading and rustls configuration shared by the MQTT adapter, the replay
//! client and the example receiver. One rustls (0.23, ring provider) for all.

use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::path::Path;
use std::sync::Arc;

pub fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// All certificates in a PEM file (at least one).
pub fn load_certs(path: &Path, what: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|e| format!("reading {what} {}: {e}", path.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("parsing {what} {}: {e}", path.display()))?;
    if certs.is_empty() {
        return Err(format!(
            "{what} {} contains no PEM certificates",
            path.display()
        ));
    }
    Ok(certs)
}

/// The first private key (PKCS#8, PKCS#1 or SEC1) in a PEM file.
pub fn load_key(path: &Path, what: &str) -> Result<PrivateKeyDer<'static>, String> {
    PrivateKeyDer::from_pem_file(path)
        .map_err(|e| format!("reading {what} {}: {e}", path.display()))
}

/// Trusted CA certificates: the PEM bundle at `ca_file`, or the platform
/// trust store (as loaded by `rustls-native-certs`).
pub fn trusted_certs(ca_file: Option<&Path>) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs = match ca_file {
        Some(path) => load_certs(path, "CA file")?,
        None => rustls_native_certs::load_native_certs().certs,
    };
    if certs.is_empty() {
        return Err("no trusted CA certificates available for TLS".into());
    }
    Ok(certs)
}

/// Trust anchors from [`trusted_certs`]. Unusable certificates in an explicit
/// CA file are an error; in the platform store they are skipped.
pub fn root_store(ca_file: Option<&Path>) -> Result<rustls::RootCertStore, String> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in trusted_certs(ca_file)? {
        if let Err(e) = roots.add(cert)
            && ca_file.is_some()
        {
            return Err(format!("adding CA certificate: {e}"));
        }
    }
    if roots.is_empty() {
        return Err("no trusted CA certificates available for TLS".into());
    }
    Ok(roots)
}

/// Client configuration with optional client certificate (mutual TLS).
pub fn client_config(
    ca_file: Option<&Path>,
    client_cert: Option<(&Path, &Path)>,
) -> Result<rustls::ClientConfig, String> {
    let roots = root_store(ca_file)?;
    let builder = rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS setup: {e}"))?
        .with_root_certificates(roots);
    Ok(match client_cert {
        None => builder.with_no_client_auth(),
        Some((cert, key)) => builder
            .with_client_auth_cert(
                load_certs(cert, "client cert")?,
                load_key(key, "client key")?,
            )
            .map_err(|e| format!("client certificate: {e}"))?,
    })
}

/// Server configuration for the example receiver. With `client_ca`, every
/// client must present a certificate chaining to it (mutual TLS).
pub fn server_config(
    cert: &Path,
    key: &Path,
    client_ca: Option<&Path>,
) -> Result<Arc<rustls::ServerConfig>, String> {
    let chain = load_certs(cert, "TLS certificate")?;
    let key = load_key(key, "TLS key")?;
    let builder = rustls::ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS setup: {e}"))?;
    let builder = match client_ca {
        None => builder.with_no_client_auth(),
        Some(ca) => {
            let roots = Arc::new(root_store(Some(ca))?);
            let verifier =
                rustls::server::WebPkiClientVerifier::builder_with_provider(roots, provider())
                    .build()
                    .map_err(|e| format!("client certificate verifier: {e}"))?;
            builder.with_client_cert_verifier(verifier)
        }
    };
    let cfg = builder
        .with_single_cert(chain, key)
        .map_err(|e| format!("TLS certificate/key: {e}"))?;
    Ok(Arc::new(cfg))
}

/// Whether a rustls error means the peer's or our own certificate was not
/// accepted. Such failures do not go away by retrying.
pub fn is_certificate_error(e: &rustls::Error) -> bool {
    use rustls::AlertDescription as A;
    match e {
        rustls::Error::InvalidCertificate(_) | rustls::Error::NoCertificatesPresented => true,
        rustls::Error::AlertReceived(a) => matches!(
            a,
            A::BadCertificate
                | A::UnsupportedCertificate
                | A::CertificateRevoked
                | A::CertificateExpired
                | A::CertificateUnknown
                | A::UnknownCA
                | A::AccessDenied
                | A::CertificateRequired
        ),
        _ => false,
    }
}

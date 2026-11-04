//! Settings file (`--config-file` / `AKHET_CONFIG_FILE`): TOML whose keys are
//! the long command-line options.
//!
//! ```toml
//! spool = "/var/lib/akhet/spool.sqlite"   # global options at the top
//!
//! [mqtt]                                  # one table per subcommand
//! host = "broker.local"
//! topic = ["plant/+/sensors", "plant/+/state"]
//! client-id = "akhet-edge-1"
//! tls = true
//!
//! [replay]
//! url = "https://collector.example"
//! sink = "central"
//! follow = true
//! ```
//!
//! The file is expanded into options before parsing, so every value goes
//! through the same validation as on the command line. Precedence: command
//! line, then the option's environment variable, then the file, then the
//! default. Tables for other subcommands are ignored, so one file can serve
//! the collector and its replay. Secrets (tokens, passwords, connection
//! strings) are read from the environment only and are not options, so they
//! cannot be put in the file.

use clap::{Arg, ArgAction, Command};
use std::ffi::{OsStr, OsString};

pub const FLAG: &str = "config-file";
pub const ENV: &str = "AKHET_CONFIG_FILE";

/// Expand the settings file (if one is given) into `args`. `cmd` is the
/// full command definition, used to find the subcommand and check keys.
pub fn expand(args: Vec<OsString>, cmd: &Command) -> Result<Vec<OsString>, String> {
    let Some(path) = config_path(&args) else {
        return Ok(args);
    };
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("config file {}: {e}", path.display()))?;
    let table: toml::Table = text
        .parse()
        .map_err(|e| format!("config file {}: {e}", path.display()))?;
    expand_table(args, cmd, &table).map_err(|e| format!("config file {}: {e}", path.display()))
}

fn config_path(args: &[OsString]) -> Option<std::path::PathBuf> {
    let flag = format!("--{FLAG}");
    let prefix = format!("--{FLAG}=");
    let mut it = args.iter().skip(1);
    while let Some(a) = it.next() {
        let s = a.to_string_lossy();
        if s == "--" {
            break;
        }
        if s == flag {
            return it.next().map(Into::into);
        }
        if let Some(v) = s.strip_prefix(&prefix) {
            return Some(v.into());
        }
    }
    std::env::var_os(ENV)
        .filter(|v| !v.is_empty())
        .map(Into::into)
}

/// Long names of the root's options that take a value (to skip their value
/// while looking for the subcommand).
fn takes_value(arg: &Arg) -> bool {
    matches!(arg.get_action(), ArgAction::Set | ArgAction::Append)
}

/// Index of the subcommand token in `args`, if any.
fn subcommand_index(args: &[OsString], cmd: &Command) -> Option<usize> {
    let valued: Vec<String> = cmd
        .get_arguments()
        .filter(|a| takes_value(a))
        .filter_map(|a| a.get_long().map(|l| format!("--{l}")))
        .collect();
    let mut i = 1;
    while i < args.len() {
        let s = args[i].to_string_lossy();
        if s == "--" {
            return None;
        }
        if s.starts_with('-') {
            if valued.iter().any(|v| *v == s) {
                i += 1;
            }
        } else if cmd.find_subcommand(OsStr::new(s.as_ref())).is_some() {
            return Some(i);
        } else {
            return None;
        }
        i += 1;
    }
    None
}

fn find_arg<'a>(root: &'a Command, sub: &'a Command, long: &str) -> Option<&'a Arg> {
    sub.get_arguments()
        .find(|a| a.get_long() == Some(long))
        .or_else(|| {
            root.get_arguments()
                .find(|a| a.is_global_set() && a.get_long() == Some(long))
        })
}

fn given_on_command_line(args: &[OsString], long: &str) -> bool {
    let flag = format!("--{long}");
    let prefix = format!("--{long}=");
    args.iter()
        .skip(1)
        .map(|a| a.to_string_lossy())
        .take_while(|s| s != "--")
        .any(|s| s == flag || s.starts_with(&prefix))
}

fn scalar(key: &str, v: &toml::Value) -> Result<String, String> {
    match v {
        toml::Value::String(s) => Ok(s.clone()),
        toml::Value::Integer(i) => Ok(i.to_string()),
        toml::Value::Float(f) => Ok(f.to_string()),
        _ => Err(format!("`{key}` must be a string or a number")),
    }
}

fn expand_table(
    args: Vec<OsString>,
    cmd: &Command,
    table: &toml::Table,
) -> Result<Vec<OsString>, String> {
    // Check the layout first, so a typo in another subcommand's table is
    // reported too.
    for (k, v) in table {
        if let toml::Value::Table(t) = v {
            let sub = cmd
                .find_subcommand(k)
                .ok_or_else(|| format!("unknown table [{k}] (not a subcommand)"))?;
            for (key, value) in t {
                check_key(cmd, sub, &format!("[{k}] {key}"), key, value)?;
            }
        }
    }
    let Some(at) = subcommand_index(&args, cmd) else {
        return Ok(args);
    };
    let name = args[at].to_string_lossy().into_owned();
    let sub = cmd.find_subcommand(&name).expect("checked");
    let globals: Vec<_> = table.iter().filter(|(_, v)| !v.is_table()).collect();
    let section = match table.get(&name) {
        Some(toml::Value::Table(t)) => Some(t),
        _ => None,
    };
    for &(key, value) in &globals {
        check_key(cmd, sub, key, key, value)?;
    }
    let mut extra: Vec<OsString> = Vec::new();
    for (key, value) in globals.into_iter().chain(section.into_iter().flatten()) {
        let long = key.replace('_', "-");
        let arg = find_arg(cmd, sub, &long).expect("checked");
        if given_on_command_line(&args, &long) {
            continue;
        }
        if arg
            .get_env()
            .and_then(std::env::var_os)
            .is_some_and(|v| !v.is_empty())
        {
            continue;
        }
        match value {
            toml::Value::Boolean(true) => extra.push(format!("--{long}").into()),
            toml::Value::Boolean(false) => {}
            toml::Value::Array(items) => {
                for item in items {
                    extra.push(format!("--{long}={}", scalar(key, item)?).into());
                }
            }
            v => extra.push(format!("--{long}={}", scalar(key, v)?).into()),
        }
    }
    let mut out = args;
    out.splice(at + 1..at + 1, extra);
    Ok(out)
}

/// The key names an option of the subcommand (or a global option), and the
/// value has a matching shape.
fn check_key(
    root: &Command,
    sub: &Command,
    shown: &str,
    key: &str,
    value: &toml::Value,
) -> Result<(), String> {
    let long = key.replace('_', "-");
    if long == FLAG {
        return Err(format!("`{shown}`: a config file cannot name another one"));
    }
    let arg = find_arg(root, sub, &long).ok_or_else(|| {
        format!(
            "unknown key `{shown}` (keys are long options; secrets are read from the environment only)"
        )
    })?;
    if long == "help" || long == "version" {
        return Err(format!("unknown key `{shown}`"));
    }
    let flag = matches!(arg.get_action(), ArgAction::SetTrue);
    match value {
        toml::Value::Boolean(_) if flag => Ok(()),
        _ if flag => Err(format!("`{shown}` is a switch: use true or false")),
        toml::Value::Boolean(_) => Err(format!("`{shown}` takes a value, not true/false")),
        toml::Value::Array(items) => {
            if !matches!(arg.get_action(), ArgAction::Append) {
                return Err(format!("`{shown}` takes one value, not a list"));
            }
            items.iter().try_for_each(|i| scalar(shown, i).map(drop))
        }
        toml::Value::Table(_) => Err(format!("`{shown}`: nested tables are not supported")),
        v => scalar(shown, v).map(drop),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    struct Cli {
        #[arg(long, global = true, default_value = "s.sqlite")]
        spool: String,
        #[arg(long, global = true)]
        config_file: Option<String>,
        #[command(subcommand)]
        cmd: Sub,
    }

    #[derive(clap::Subcommand, Debug, PartialEq)]
    enum Sub {
        Mqtt {
            #[arg(long, default_value = "localhost")]
            host: String,
            #[arg(long, required = true)]
            topic: Vec<String>,
            #[arg(long)]
            tls: bool,
            #[arg(long, default_value_t = 64)]
            max_batch: usize,
            #[arg(long, env = "AKHET_SETTINGS_TEST_PORT", default_value_t = 1883)]
            port: u16,
        },
        Status,
    }

    /// `run` reads the environment, and one test sets a variable.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn run(cli: &[&str], toml: &str) -> Result<Cli, String> {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        run_locked(cli, toml)
    }

    fn run_locked(cli: &[&str], toml: &str) -> Result<Cli, String> {
        use clap::CommandFactory;
        let args: Vec<OsString> = cli.iter().map(Into::into).collect();
        let table: toml::Table = toml.parse().map_err(|e| format!("{e}"))?;
        let out = expand_table(args, &Cli::command(), &table)?;
        Cli::try_parse_from(out).map_err(|e| e.to_string())
    }

    const FILE: &str = r#"
        spool = "/var/lib/akhet/s.sqlite"
        [mqtt]
        host = "broker"
        topic = ["a/#", "b/#"]
        tls = true
        max_batch = 8
        [replay]
    "#;

    #[test]
    fn file_values_fill_in_options() {
        // [replay] is not a subcommand of this test CLI.
        let err = run(&["akhet", "mqtt"], FILE).unwrap_err();
        assert!(err.contains("unknown table [replay]"), "{err}");
        let file = FILE.replace("[replay]", "");
        let cli = run(&["akhet", "mqtt"], &file).unwrap();
        assert_eq!(cli.spool, "/var/lib/akhet/s.sqlite");
        assert_eq!(
            cli.cmd,
            Sub::Mqtt {
                host: "broker".into(),
                topic: vec!["a/#".into(), "b/#".into()],
                tls: true,
                max_batch: 8,
                port: 1883,
            }
        );
    }

    #[test]
    fn command_line_wins_and_lists_are_replaced() {
        let file = FILE.replace("[replay]", "");
        let cli = run(
            &[
                "akhet",
                "--spool",
                "x.sqlite",
                "mqtt",
                "--topic",
                "c/#",
                "--max-batch=2",
            ],
            &file,
        )
        .unwrap();
        assert_eq!(cli.spool, "x.sqlite");
        let Sub::Mqtt {
            topic, max_batch, ..
        } = cli.cmd
        else {
            panic!()
        };
        assert_eq!(topic, vec!["c/#".to_string()]);
        assert_eq!(max_batch, 2);
    }

    #[test]
    fn other_subcommands_ignore_their_tables_but_take_globals() {
        let file = FILE.replace("[replay]", "");
        let cli = run(&["akhet", "status"], &file).unwrap();
        assert_eq!(cli.cmd, Sub::Status);
        assert_eq!(cli.spool, "/var/lib/akhet/s.sqlite");
    }

    #[test]
    fn mistakes_are_reported() {
        for (toml, want) in [
            ("[mqtt]\npasword = \"x\"", "unknown key `[mqtt] pasword`"),
            ("[mqtt]\ntls = \"yes\"", "is a switch"),
            ("[mqtt]\nhost = true", "takes a value"),
            ("[mqtt]\nhost = [\"a\", \"b\"]", "not a list"),
            ("[mqtt]\nhost = { a = 1 }", "nested tables"),
            ("config-file = \"other.toml\"", "cannot name another"),
            ("[nope]\nx = 1", "unknown table [nope]"),
        ] {
            let err = run(&["akhet", "mqtt", "--topic", "t"], toml).unwrap_err();
            assert!(err.contains(want), "{toml}: {err}");
        }
    }

    #[test]
    fn invalid_values_fail_like_on_the_command_line() {
        let err = run(&["akhet", "mqtt"], "[mqtt]\ntopic=[\"t\"]\nmax_batch = -1").unwrap_err();
        assert!(err.contains("max-batch"), "{err}");
    }

    #[test]
    fn switch_false_and_env_precedence() {
        // SAFETY: test-only variable, not read by other tests.
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe { std::env::set_var("AKHET_SETTINGS_TEST_PORT", "1999") };
        let cli = run_locked(
            &["akhet", "mqtt"],
            "[mqtt]\ntopic=[\"t\"]\ntls=false\nport=1884",
        )
        .unwrap();
        unsafe { std::env::remove_var("AKHET_SETTINGS_TEST_PORT") };
        let Sub::Mqtt { tls, port, .. } = cli.cmd else {
            panic!()
        };
        assert!(!tls);
        assert_eq!(port, 1999, "environment variable beats the file");
    }
}

#[cfg(test)]
mod shipped {
    use super::*;
    use clap::{CommandFactory, Parser};

    /// The example in deploy/systemd is valid for the commands it configures.
    #[test]
    fn deploy_example_parses() {
        let text = include_str!("../deploy/systemd/akhet.toml");
        let table: toml::Table = text.parse().unwrap();
        for sub in ["mqtt", "replay", "status"] {
            let args = vec![OsString::from("akhet"), sub.into()];
            let out = expand_table(args, &crate::Cli::command(), &table).unwrap();
            let cli =
                crate::Cli::try_parse_from(&out).unwrap_or_else(|e| panic!("{sub}: {e}\n{out:?}"));
            assert_eq!(cli.spool, Path::new("/var/lib/akhet/spool.sqlite"));
            assert_eq!(cli.max_db_bytes, 1 << 30);
        }
    }

    /// The CNC example parses for the commands it configures.
    #[test]
    fn cnc_example_parses() {
        let text = include_str!("../deploy/examples/cnc-sensor-hub.toml");
        let table: toml::Table = text.parse().unwrap();
        for sub in ["mqtt", "morlet", "status"] {
            let args = vec![OsString::from("akhet"), sub.into()];
            let out = expand_table(args, &crate::Cli::command(), &table).unwrap();
            crate::Cli::try_parse_from(&out).unwrap_or_else(|e| panic!("{sub}: {e}\n{out:?}"));
        }
    }

    use std::path::Path;
}

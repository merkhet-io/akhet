//! Decoding of spooled MQTT payloads into per-channel sample batches.
//!
//! The spool stores raw payload bytes and is never rewritten by decoding.
//! Decoding is a pure function of (decoder config, spooled event), so the raw
//! payload stays available for audit and a corrected config can be re-applied
//! to the same records.
//!
//! The supported payload shape is the common "batched signal groups" layout:
//! one message carries one or more groups; each group has one timestamp array
//! and one value array per signal, all of equal length:
//!
//! ```json
//! {"device": "hub-1",
//!  "groups": [
//!    {"name": "vib", "timestamps": [1000, 1250, 1500],
//!     "signals": {"x": [0.1, 0.2, 0.3], "y": [0.0, 0.1, 0.0]}}]}
//! ```
//!
//! A channel is identified by `(source, group, signal)`. Every validation
//! failure produces an explicit [`DecodeIssue`]; nothing is dropped silently.
//! A group with an issue contributes no channel batches at all, so a caller
//! never receives a partially decoded group.

use crate::event::{Event, EventData};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;

/// Decoder config format version understood by this build.
pub const DECODER_CONFIG_VERSION: u32 = 1;

/// Upper bound on samples per channel per message (guards memory use).
pub const MAX_SAMPLES_PER_BATCH: usize = 1 << 20;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecoderConfig {
    pub format_version: u32,
    pub rules: Vec<Rule>,
}

/// How to decode messages whose topic matches `topic`. The first matching
/// rule (in file order) applies.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    /// Rule name, recorded in issues.
    pub name: String,
    /// MQTT topic filter (`+` and `#` wildcards).
    pub topic: String,
    /// Source identity template. Placeholders: `{topic}`, `{topic.N}`
    /// (zero-based topic level) and `{/json/pointer}` into the message root.
    pub source: String,
    /// JSON pointer to an array of group objects. Absent: the message root is
    /// the only group.
    #[serde(default)]
    pub groups: Option<String>,
    /// Group name template. Same placeholders as `source`, plus
    /// `{group:/json/pointer}` into the group object.
    pub group: String,
    /// JSON pointer (within the group) to the timestamp array.
    pub timestamps: String,
    pub timestamp_unit: TimestampUnit,
    pub signals: SignalSpec,
    /// Unit per signal name. Used for every channel with that signal name.
    #[serde(default)]
    pub units: BTreeMap<String, String>,
    /// Unit for signals not listed in `units`. Absent: such signals are a
    /// decode issue (a channel never gets an unstated unit).
    #[serde(default)]
    pub default_unit: Option<String>,
    /// Static metadata attached to every channel decoded by this rule.
    #[serde(default)]
    pub metadata: serde_json::Map<String, Value>,
    /// How to place this rule's samples on a uniform grid (`akhet grid`).
    /// Absent: the default (estimated rate, 10% of a period tolerance).
    #[serde(default)]
    pub timing: Option<crate::timing::TimingSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampUnit {
    Ns,
    Us,
    Ms,
    S,
}

impl TimestampUnit {
    fn ns_per_unit(self) -> i64 {
        match self {
            TimestampUnit::Ns => 1,
            TimestampUnit::Us => 1_000,
            TimestampUnit::Ms => 1_000_000,
            TimestampUnit::S => 1_000_000_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SignalSpec {
    /// JSON pointer (within the group) to an object mapping signal name to
    /// value array. Every member is a signal.
    Map(String),
    /// Explicit signals: name and JSON pointer (within the group).
    List(Vec<SignalPointer>),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalPointer {
    pub name: String,
    pub pointer: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ChannelId {
    pub source: String,
    pub group: String,
    pub signal: String,
}

impl ChannelId {
    /// `source/group/signal`, with `%`, `/` and `@` in each part
    /// percent-encoded so the key is unambiguous.
    pub fn key(&self) -> String {
        let esc = |s: &str| {
            s.replace('%', "%25")
                .replace('/', "%2F")
                .replace('@', "%40")
        };
        format!(
            "{}/{}/{}",
            esc(&self.source),
            esc(&self.group),
            esc(&self.signal)
        )
    }
}

/// Samples of one channel from one message, validated: equal lengths,
/// finite values, strictly increasing timestamps.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ChannelBatch {
    pub channel: ChannelId,
    pub unit: String,
    pub metadata: serde_json::Map<String, Value>,
    /// Source timestamps, ns since the Unix epoch as stated by the publisher.
    pub timestamps_unix_ns: Vec<i64>,
    pub values: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueKind {
    /// No rule matches the topic.
    NoMatchingRule,
    /// Payload is not valid base64 in the event or not JSON.
    NotJson,
    /// A configured JSON pointer is missing or has the wrong type.
    BadStructure,
    /// A template placeholder could not be resolved.
    BadTemplate,
    /// Timestamp array and a value array differ in length.
    LengthMismatch,
    /// A value is not a finite number.
    NonFiniteValue,
    /// A timestamp is not an integer (or exceeds the i64 ns range).
    BadTimestamp,
    /// Timestamps within one group are not strictly increasing.
    NonMonotonicTimestamps,
    /// A signal has no configured unit and no default unit is set.
    NoUnit,
    /// A batch exceeds [`MAX_SAMPLES_PER_BATCH`].
    TooLarge,
    /// Two signals in one message resolve to the same channel.
    DuplicateChannel,
}

/// An explicit decode failure. The group it refers to produced no batches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DecodeIssue {
    pub kind: IssueKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule: Option<String>,
    pub topic: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<String>,
    /// Sample index within the group, when the issue is about one sample.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_index: Option<usize>,
    pub message: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Decoded {
    pub batches: Vec<ChannelBatch>,
    pub issues: Vec<DecodeIssue>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading decoder config {0}: {1}")]
    Io(String, std::io::Error),
    #[error("decoder config is invalid: {0}")]
    Invalid(String),
}

impl DecoderConfig {
    pub fn load(path: &Path) -> Result<DecoderConfig, ConfigError> {
        let text =
            std::fs::read(path).map_err(|e| ConfigError::Io(path.display().to_string(), e))?;
        Self::from_json(&text)
    }

    pub fn from_json(bytes: &[u8]) -> Result<DecoderConfig, ConfigError> {
        let cfg: DecoderConfig =
            serde_json::from_slice(bytes).map_err(|e| ConfigError::Invalid(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let bad = |m: String| Err(ConfigError::Invalid(m));
        if self.format_version != DECODER_CONFIG_VERSION {
            return bad(format!(
                "format_version {} is not supported (this build reads {DECODER_CONFIG_VERSION})",
                self.format_version
            ));
        }
        if self.rules.is_empty() {
            return bad("at least one rule is required".into());
        }
        let mut names = std::collections::BTreeSet::new();
        for r in &self.rules {
            if r.name.is_empty() || !names.insert(r.name.as_str()) {
                return bad(format!(
                    "rule names must be non-empty and unique: {:?}",
                    r.name
                ));
            }
            validate_filter(&r.topic)
                .map_err(|m| ConfigError::Invalid(format!("rule {:?}: {m}", r.name)))?;
            let mut pointers: Vec<&str> = vec![&r.timestamps];
            pointers.extend(r.groups.as_deref());
            match &r.signals {
                SignalSpec::Map(p) => pointers.push(p),
                SignalSpec::List(l) => {
                    if l.is_empty() {
                        return bad(format!("rule {:?}: signal list is empty", r.name));
                    }
                    let mut seen = std::collections::BTreeSet::new();
                    for s in l {
                        if s.name.is_empty() || !seen.insert(s.name.as_str()) {
                            return bad(format!(
                                "rule {:?}: signal names must be non-empty and unique",
                                r.name
                            ));
                        }
                        pointers.push(&s.pointer);
                    }
                }
            }
            for p in pointers {
                if !p.is_empty() && !p.starts_with('/') {
                    return bad(format!(
                        "rule {:?}: JSON pointer {p:?} must be empty or start with '/'",
                        r.name
                    ));
                }
            }
            if let Some(t) = &r.timing {
                t.validate()
                    .map_err(|m| ConfigError::Invalid(format!("rule {:?}: timing: {m}", r.name)))?;
            }
            for (what, t) in [("source", &r.source), ("group", &r.group)] {
                parse_template(t).map_err(|m| {
                    ConfigError::Invalid(format!("rule {:?}: {what} template: {m}", r.name))
                })?;
            }
            if parse_template(&r.source)
                .unwrap_or_default()
                .iter()
                .any(|p| matches!(p, Piece::Group(_)))
            {
                return bad(format!(
                    "rule {:?}: source template cannot use {{group:...}}",
                    r.name
                ));
            }
        }
        Ok(())
    }

    /// The first rule whose filter matches `topic`.
    pub fn rule_for(&self, topic: &str) -> Option<&Rule> {
        self.rules.iter().find(|r| topic_matches(&r.topic, topic))
    }

    /// Decode a spooled event. Returns `None` for events that are not MQTT
    /// messages (Modbus polls have their own register semantics).
    pub fn decode_event(&self, ev: &Event) -> Option<Decoded> {
        let EventData::MqttMessage(m) = &ev.data else {
            return None;
        };
        let payload = match base64::engine::general_purpose::STANDARD.decode(&m.payload_base64) {
            Ok(p) => p,
            Err(e) => {
                return Some(Decoded {
                    batches: vec![],
                    issues: vec![issue(
                        IssueKind::NotJson,
                        None,
                        &m.topic,
                        format!("stored payload is not valid base64: {e}"),
                    )],
                });
            }
        };
        Some(self.decode_payload(&m.topic, &payload))
    }

    /// Decode one raw payload published on `topic`.
    pub fn decode_payload(&self, topic: &str, payload: &[u8]) -> Decoded {
        let Some(rule) = self.rule_for(topic) else {
            return Decoded {
                batches: vec![],
                issues: vec![issue(
                    IssueKind::NoMatchingRule,
                    None,
                    topic,
                    "no decoder rule matches this topic".into(),
                )],
            };
        };
        rule.decode(topic, payload)
    }
}

fn issue(kind: IssueKind, rule: Option<&Rule>, topic: &str, message: String) -> DecodeIssue {
    DecodeIssue {
        kind,
        rule: rule.map(|r| r.name.clone()),
        topic: topic.to_string(),
        group_index: None,
        signal: None,
        sample_index: None,
        message,
    }
}

impl Rule {
    pub fn decode(&self, topic: &str, payload: &[u8]) -> Decoded {
        let mut out = Decoded::default();
        let root: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(e) => {
                out.issues.push(issue(
                    IssueKind::NotJson,
                    Some(self),
                    topic,
                    format!("payload is not JSON: {e}"),
                ));
                return out;
            }
        };
        let source = match render(&self.source, topic, &root, None) {
            Ok(s) => s,
            Err(m) => {
                out.issues.push(issue(
                    IssueKind::BadTemplate,
                    Some(self),
                    topic,
                    format!("source: {m}"),
                ));
                return out;
            }
        };
        let groups: Vec<&Value> = match &self.groups {
            None => vec![&root],
            Some(p) => match root.pointer(p) {
                Some(Value::Array(a)) => a.iter().collect(),
                other => {
                    out.issues.push(issue(
                        IssueKind::BadStructure,
                        Some(self),
                        topic,
                        format!(
                            "groups pointer {p:?}: expected an array, found {}",
                            kind_of(other)
                        ),
                    ));
                    return out;
                }
            },
        };
        let mut seen = std::collections::BTreeSet::new();
        for (gi, g) in groups.into_iter().enumerate() {
            match self.decode_group(topic, &root, g, &source) {
                Ok(batches) => {
                    let mut dup = None;
                    for b in &batches {
                        if !seen.insert(b.channel.clone()) {
                            dup = Some(b.channel.clone());
                            break;
                        }
                    }
                    if let Some(c) = dup {
                        out.issues.push(DecodeIssue {
                            group_index: Some(gi),
                            signal: Some(c.signal.clone()),
                            ..issue(
                                IssueKind::DuplicateChannel,
                                Some(self),
                                topic,
                                format!(
                                    "channel {}/{}/{} occurs more than once in this message",
                                    c.source, c.group, c.signal
                                ),
                            )
                        });
                    } else {
                        out.batches.extend(batches);
                    }
                }
                Err(mut e) => {
                    e.group_index = Some(gi);
                    out.issues.push(*e);
                }
            }
        }
        out
    }

    fn decode_group(
        &self,
        topic: &str,
        root: &Value,
        g: &Value,
        source: &str,
    ) -> Result<Vec<ChannelBatch>, Box<DecodeIssue>> {
        let fail = |kind, signal: Option<&str>, idx: Option<usize>, m: String| {
            Box::new(DecodeIssue {
                signal: signal.map(str::to_string),
                sample_index: idx,
                ..issue(kind, Some(self), topic, m)
            })
        };
        let group = render(&self.group, topic, root, Some(g))
            .map_err(|m| fail(IssueKind::BadTemplate, None, None, format!("group: {m}")))?;
        let ts = match g.pointer(&self.timestamps) {
            Some(Value::Array(a)) => a,
            other => {
                return Err(fail(
                    IssueKind::BadStructure,
                    None,
                    None,
                    format!(
                        "timestamps pointer {:?}: expected an array, found {}",
                        self.timestamps,
                        kind_of(other)
                    ),
                ));
            }
        };
        if ts.len() > MAX_SAMPLES_PER_BATCH {
            return Err(fail(
                IssueKind::TooLarge,
                None,
                None,
                format!(
                    "{} samples exceed the limit of {MAX_SAMPLES_PER_BATCH}",
                    ts.len()
                ),
            ));
        }
        let mul = self.timestamp_unit.ns_per_unit();
        let mut timestamps = Vec::with_capacity(ts.len());
        for (i, t) in ts.iter().enumerate() {
            let ns = t.as_i64().and_then(|v| v.checked_mul(mul)).ok_or_else(|| {
                fail(
                    IssueKind::BadTimestamp,
                    None,
                    Some(i),
                    format!(
                        "timestamp {t} is not an integer number of {:?} within range",
                        self.timestamp_unit
                    ),
                )
            })?;
            if let Some(&prev) = timestamps.last()
                && ns <= prev
            {
                return Err(fail(
                    IssueKind::NonMonotonicTimestamps,
                    None,
                    Some(i),
                    format!("timestamp {ns} ns does not follow {prev} ns"),
                ));
            }
            timestamps.push(ns);
        }

        let signals: Vec<(&str, Option<&Value>)> = match &self.signals {
            SignalSpec::Map(p) => match g.pointer(p) {
                Some(Value::Object(m)) => m.iter().map(|(k, v)| (k.as_str(), Some(v))).collect(),
                other => {
                    return Err(fail(
                        IssueKind::BadStructure,
                        None,
                        None,
                        format!(
                            "signals pointer {p:?}: expected an object, found {}",
                            kind_of(other)
                        ),
                    ));
                }
            },
            SignalSpec::List(l) => l
                .iter()
                .map(|s| (s.name.as_str(), g.pointer(&s.pointer)))
                .collect(),
        };
        let mut batches = Vec::with_capacity(signals.len());
        for (name, v) in signals {
            let Some(Value::Array(vals)) = v else {
                return Err(fail(
                    IssueKind::BadStructure,
                    Some(name),
                    None,
                    format!(
                        "signal {name:?}: expected a value array, found {}",
                        kind_of(v)
                    ),
                ));
            };
            if vals.len() != timestamps.len() {
                return Err(fail(
                    IssueKind::LengthMismatch,
                    Some(name),
                    None,
                    format!(
                        "signal {name:?} has {} values for {} timestamps",
                        vals.len(),
                        timestamps.len()
                    ),
                ));
            }
            let unit = match self.units.get(name).or(self.default_unit.as_ref()) {
                Some(u) => u.clone(),
                None => {
                    return Err(fail(
                        IssueKind::NoUnit,
                        Some(name),
                        None,
                        format!(
                            "signal {name:?} has no configured unit and no default_unit is set"
                        ),
                    ));
                }
            };
            let mut values = Vec::with_capacity(vals.len());
            for (i, x) in vals.iter().enumerate() {
                match x.as_f64() {
                    Some(f) if f.is_finite() => values.push(f),
                    _ => {
                        return Err(fail(
                            IssueKind::NonFiniteValue,
                            Some(name),
                            Some(i),
                            format!("signal {name:?} value {x} is not a finite number"),
                        ));
                    }
                }
            }
            batches.push(ChannelBatch {
                channel: ChannelId {
                    source: source.to_string(),
                    group: group.clone(),
                    signal: name.to_string(),
                },
                unit,
                metadata: self.metadata.clone(),
                timestamps_unix_ns: timestamps.clone(),
                values,
            });
        }
        Ok(batches)
    }
}

fn kind_of(v: Option<&Value>) -> &'static str {
    match v {
        None => "nothing",
        Some(Value::Null) => "null",
        Some(Value::Bool(_)) => "a boolean",
        Some(Value::Number(_)) => "a number",
        Some(Value::String(_)) => "a string",
        Some(Value::Array(_)) => "an array",
        Some(Value::Object(_)) => "an object",
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Piece {
    Text(String),
    Topic,
    TopicLevel(usize),
    Root(String),
    Group(String),
}

fn parse_template(t: &str) -> Result<Vec<Piece>, String> {
    let mut out = Vec::new();
    let mut rest = t;
    while let Some(open) = rest.find('{') {
        if open > 0 {
            out.push(Piece::Text(rest[..open].to_string()));
        }
        let close = rest[open..]
            .find('}')
            .ok_or_else(|| format!("unclosed '{{' in {t:?}"))?
            + open;
        let inner = &rest[open + 1..close];
        out.push(if inner == "topic" {
            Piece::Topic
        } else if let Some(n) = inner.strip_prefix("topic.") {
            Piece::TopicLevel(
                n.parse()
                    .map_err(|_| format!("bad topic level {inner:?}"))?,
            )
        } else if let Some(p) = inner.strip_prefix("group:") {
            if !p.starts_with('/') {
                return Err(format!("group pointer {p:?} must start with '/'"));
            }
            Piece::Group(p.to_string())
        } else if inner.starts_with('/') {
            Piece::Root(inner.to_string())
        } else {
            return Err(format!("unknown placeholder {{{inner}}}"));
        });
        rest = &rest[close + 1..];
    }
    if rest.contains('}') {
        return Err(format!("unmatched '}}' in {t:?}"));
    }
    if !rest.is_empty() {
        out.push(Piece::Text(rest.to_string()));
    }
    if out.is_empty() {
        return Err("template is empty".into());
    }
    Ok(out)
}

fn scalar(v: Option<&Value>, what: &str) -> Result<String, String> {
    match v {
        Some(Value::String(s)) if !s.is_empty() => Ok(s.clone()),
        Some(Value::Number(n)) => Ok(n.to_string()),
        other => Err(format!(
            "{what}: expected a non-empty string or number, found {}",
            kind_of(other)
        )),
    }
}

fn render(t: &str, topic: &str, root: &Value, group: Option<&Value>) -> Result<String, String> {
    let mut s = String::new();
    for p in parse_template(t)? {
        match p {
            Piece::Text(x) => s.push_str(&x),
            Piece::Topic => s.push_str(topic),
            Piece::TopicLevel(n) => s.push_str(
                topic
                    .split('/')
                    .nth(n)
                    .filter(|l| !l.is_empty())
                    .ok_or_else(|| format!("topic {topic:?} has no non-empty level {n}"))?,
            ),
            Piece::Root(ptr) => s.push_str(&scalar(root.pointer(&ptr), &ptr)?),
            Piece::Group(ptr) => {
                let g = group.ok_or("group placeholder outside a group")?;
                s.push_str(&scalar(g.pointer(&ptr), &format!("group:{ptr}"))?)
            }
        }
    }
    Ok(s)
}

/// Validate an MQTT topic filter (MQTT 3.1.1 section 4.7).
pub fn validate_filter(filter: &str) -> Result<(), String> {
    if filter.is_empty() {
        return Err("topic filter must be non-empty".into());
    }
    let levels: Vec<&str> = filter.split('/').collect();
    for (i, l) in levels.iter().enumerate() {
        if l.contains('#') && (*l != "#" || i != levels.len() - 1) {
            return Err(format!("'#' must be a whole, last level in {filter:?}"));
        }
        if l.contains('+') && *l != "+" {
            return Err(format!("'+' must be a whole level in {filter:?}"));
        }
    }
    Ok(())
}

/// MQTT topic filter matching with `+` (one level) and `#` (rest, including
/// the parent level). Topics starting with `$` do not match leading wildcards.
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && (filter.starts_with('+') || filter.starts_with('#')) {
        return false;
    }
    let mut f = filter.split('/');
    let mut t = topic.split('/');
    loop {
        match (f.next(), t.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> DecoderConfig {
        DecoderConfig::from_json(
            br#"{"format_version":1,"rules":[{
                "name":"hub","topic":"sensors/+/batch","source":"{topic.1}",
                "groups":"/groups","group":"{group:/name}",
                "timestamps":"/timestamps","timestamp_unit":"us",
                "signals":{"map":"/signals"},
                "units":{"x":"g","y":"g"},
                "metadata":{"site":"lab"}}]}"#,
        )
        .unwrap()
    }

    #[test]
    fn decodes_multiple_groups() {
        let p = br#"{"groups":[
            {"name":"vib","timestamps":[1,2,3],"signals":{"x":[0.5,1,-2],"y":[0,0,0]}},
            {"name":"slow","timestamps":[10],"signals":{"x":[7]}}]}"#;
        let d = cfg().decode_payload("sensors/hub-1/batch", p);
        assert!(d.issues.is_empty(), "{:?}", d.issues);
        assert_eq!(d.batches.len(), 3);
        let b = &d.batches[0];
        assert_eq!(b.channel.source, "hub-1");
        assert_eq!(b.channel.group, "vib");
        assert_eq!(b.channel.signal, "x");
        assert_eq!(b.unit, "g");
        assert_eq!(b.timestamps_unix_ns, vec![1000, 2000, 3000]);
        assert_eq!(b.values, vec![0.5, 1.0, -2.0]);
        assert_eq!(b.metadata["site"], "lab");
        assert_eq!(d.batches[2].channel.group, "slow");
    }

    #[test]
    fn bad_group_is_reported_and_others_decode() {
        let p = br#"{"groups":[
            {"name":"a","timestamps":[1,2],"signals":{"x":[1]}},
            {"name":"b","timestamps":[1,2],"signals":{"x":[1,2]}}]}"#;
        let d = cfg().decode_payload("sensors/h/batch", p);
        assert_eq!(d.batches.len(), 1);
        assert_eq!(d.batches[0].channel.group, "b");
        assert_eq!(d.issues.len(), 1);
        assert_eq!(d.issues[0].kind, IssueKind::LengthMismatch);
        assert_eq!(d.issues[0].group_index, Some(0));
        assert_eq!(d.issues[0].signal.as_deref(), Some("x"));
    }

    #[test]
    fn every_failure_is_explicit() {
        let c = cfg();
        let k = |topic: &str, p: &[u8]| {
            let d = c.decode_payload(topic, p);
            assert!(d.batches.is_empty());
            d.issues[0].kind.clone()
        };
        let g = |body: &str| format!(r#"{{"groups":[{{"name":"g",{body}}}]}}"#);
        assert_eq!(k("other", b"{}"), IssueKind::NoMatchingRule);
        assert_eq!(k("sensors/h/batch", b"nope"), IssueKind::NotJson);
        assert_eq!(k("sensors/h/batch", b"{}"), IssueKind::BadStructure);
        let cases = [
            (
                r#""timestamps":[1,1],"signals":{"x":[1,2]}"#,
                IssueKind::NonMonotonicTimestamps,
            ),
            (
                r#""timestamps":[2,1],"signals":{"x":[1,2]}"#,
                IssueKind::NonMonotonicTimestamps,
            ),
            (
                r#""timestamps":[1.5],"signals":{"x":[1]}"#,
                IssueKind::BadTimestamp,
            ),
            (
                r#""timestamps":["1"],"signals":{"x":[1]}"#,
                IssueKind::BadTimestamp,
            ),
            (
                r#""timestamps":[1],"signals":{"x":[null]}"#,
                IssueKind::NonFiniteValue,
            ),
            (
                r#""timestamps":[1],"signals":{"x":["1"]}"#,
                IssueKind::NonFiniteValue,
            ),
            (r#""timestamps":[1],"signals":{"z":[1]}"#, IssueKind::NoUnit),
            (r#""timestamps":[1],"signals":[1]"#, IssueKind::BadStructure),
            (
                r#""timestamps":[9223372036854775807],"signals":{"x":[1]}"#,
                IssueKind::BadTimestamp,
            ),
        ];
        for (body, kind) in cases {
            assert_eq!(k("sensors/h/batch", g(body).as_bytes()), kind, "{body}");
        }
        // Group name missing.
        let d = c.decode_payload(
            "sensors/h/batch",
            br#"{"groups":[{"timestamps":[1],"signals":{"x":[1]}}]}"#,
        );
        assert_eq!(d.issues[0].kind, IssueKind::BadTemplate);
        let i = &d.issues[0];
        assert_eq!(i.rule.as_deref(), Some("hub"));
        assert_eq!(i.topic, "sensors/h/batch");
    }

    #[test]
    fn duplicate_channel_in_one_message() {
        let p = br#"{"groups":[
            {"name":"g","timestamps":[1],"signals":{"x":[1]}},
            {"name":"g","timestamps":[2],"signals":{"x":[2]}}]}"#;
        let d = cfg().decode_payload("sensors/h/batch", p);
        assert_eq!(d.batches.len(), 1);
        assert_eq!(d.issues[0].kind, IssueKind::DuplicateChannel);
    }

    #[test]
    fn list_signals_root_group_and_default_unit() {
        let c = DecoderConfig::from_json(
            br#"{"format_version":1,"rules":[{
                "name":"r","topic":"m/#","source":"{/dev}","group":"main",
                "timestamps":"/t","timestamp_unit":"ms",
                "signals":{"list":[{"name":"load","pointer":"/v/0"},{"name":"speed","pointer":"/v/1"}]},
                "default_unit":"raw"}]}"#,
        )
        .unwrap();
        let d = c.decode_payload("m/a/b", br#"{"dev":7,"t":[5,6],"v":[[1,2],[3,4]]}"#);
        assert!(d.issues.is_empty(), "{:?}", d.issues);
        assert_eq!(d.batches.len(), 2);
        assert_eq!(
            d.batches[1].channel,
            ChannelId {
                source: "7".into(),
                group: "main".into(),
                signal: "speed".into()
            }
        );
        assert_eq!(d.batches[1].unit, "raw");
        assert_eq!(d.batches[0].timestamps_unix_ns, vec![5_000_000, 6_000_000]);
    }

    #[test]
    fn values_roundtrip_bit_exact() {
        let vals = [
            0.1,
            -0.0,
            1e-300,
            123456.789012345,
            f64::MAX,
            f64::MIN_POSITIVE,
        ];
        let body = serde_json::json!({"groups":[{"name":"g","timestamps":(1..=vals.len() as i64).collect::<Vec<_>>(),"signals":{"x":vals}}]});
        let d = cfg().decode_payload("sensors/h/batch", &serde_json::to_vec(&body).unwrap());
        let got = &d.batches[0].values;
        for (a, b) in got.iter().zip(vals) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn config_validation() {
        let base = |rule: &str| {
            DecoderConfig::from_json(
                format!(r#"{{"format_version":1,"rules":[{rule}]}}"#).as_bytes(),
            )
        };
        let ok = r#"{"name":"r","topic":"a/#","source":"s","group":"g","timestamps":"/t","timestamp_unit":"ns","signals":{"map":"/s"}}"#;
        assert!(base(ok).is_ok());
        assert!(base(&ok.replace("a/#", "a/#/b")).is_err());
        assert!(base(&ok.replace("\"/t\"", "\"t\"")).is_err());
        assert!(base(&ok.replace("\"source\":\"s\"", "\"source\":\"{group:/x}\"")).is_err());
        assert!(base(&ok.replace("\"group\":\"g\"", "\"group\":\"{nope}\"")).is_err());
        assert!(base(&ok.replace("\"ns\"", "\"min\"")).is_err());
        assert!(base(&ok.replace("{\"name\"", "{\"extra\":1,\"name\"")).is_err());
        assert!(DecoderConfig::from_json(br#"{"format_version":2,"rules":[]}"#).is_err());
    }

    #[test]
    fn topic_filter_matching() {
        assert!(topic_matches("a/+/c", "a/b/c"));
        assert!(!topic_matches("a/+/c", "a/b/d"));
        assert!(topic_matches("a/#", "a"));
        assert!(topic_matches("a/#", "a/b/c"));
        assert!(topic_matches("#", "x/y"));
        assert!(!topic_matches("#", "$SYS/x"));
        assert!(!topic_matches("a/b", "a/b/c"));
        assert!(topic_matches("+/+", "/x"));
        assert!(validate_filter("a/+/#").is_ok());
        assert!(validate_filter("a/b+").is_err());
    }
}

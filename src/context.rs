//! Context timelines for irregular controller signals.
//!
//! Signals such as program number, tool number, part count or feed override
//! change irregularly and are not uniform waveforms. A [`ContextTracker`]
//! turns their observations into change events with last-value-held
//! semantics: a value is in effect from its change timestamp until the next
//! change.
//!
//! - A repeated observation of the value in effect produces no event.
//! - An observation with a timestamp after every earlier one and a different
//!   value is a change event carrying the publisher's timestamp (`clock:
//!   "source"`), the collector timestamp of the spooled message and the
//!   previous value.
//! - An observation at or before the latest timestamp (late or out of order)
//!   never rewrites the timeline. If it agrees with the value in effect at
//!   that time it is counted as consistent; otherwise it is reported as a
//!   `late_conflict` issue, or `late_unknown` when it predates the retained
//!   history.
//!
//! The output is a deterministic function of the spooled records in order,
//! so the timeline can be rebuilt exactly from the spool. Values compare as
//! JSON values: `1` and `1.0` are different values.

use crate::decode::ChannelId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;

/// Change events retained per channel to judge late observations.
pub const DEFAULT_HISTORY: usize = 4096;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextChange {
    pub channel: ChannelId,
    /// Publisher timestamp of the first observation of the new value.
    pub unix_ns: i64,
    /// Which clock `unix_ns` comes from. Always `source`.
    pub clock: String,
    /// Collector clock when the message carrying this observation arrived.
    pub collected_unix_ns: i64,
    pub value: Value,
    /// Value in effect before; absent for the first observation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextIssueKind {
    /// A late observation disagrees with the value in effect at its time.
    LateConflict,
    /// A late observation predates the retained history.
    LateUnknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextIssue {
    pub kind: ContextIssueKind,
    pub channel: ChannelId,
    pub unix_ns: i64,
    pub value: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_in_effect: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContextOutput {
    Change(ContextChange),
    Issue(ContextIssue),
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ContextReport {
    pub observations: u64,
    pub changes: u64,
    /// In-order observations equal to the value in effect.
    pub repeats: u64,
    /// Late observations that agree with the value in effect at their time.
    pub late_consistent: u64,
    pub late_conflicts: u64,
    pub late_unknown: u64,
}

/// Last-value-held timeline of one context channel. Serializable so a
/// consumer can persist it with its own progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextTracker {
    channel: ChannelId,
    history: VecDeque<(i64, Value)>,
    max_history: usize,
    /// True once history entries have been dropped.
    truncated: bool,
    latest: Option<i64>,
    report: ContextReport,
}

impl ContextTracker {
    pub fn new(channel: ChannelId) -> Self {
        Self::with_history(channel, DEFAULT_HISTORY)
    }

    pub fn with_history(channel: ChannelId, max_history: usize) -> Self {
        ContextTracker {
            channel,
            history: VecDeque::new(),
            max_history: max_history.max(1),
            truncated: false,
            latest: None,
            report: ContextReport::default(),
        }
    }

    pub fn report(&self) -> &ContextReport {
        &self.report
    }

    /// The value in effect at `unix_ns`, if the retained history covers it.
    pub fn value_at(&self, unix_ns: i64) -> Option<&Value> {
        self.history
            .iter()
            .rev()
            .find(|(t, _)| *t <= unix_ns)
            .map(|(_, v)| v)
    }

    /// Feed one batch of observations (as produced by the decoder) from a
    /// message the collector received at `collected_unix_ns`.
    pub fn push(
        &mut self,
        timestamps: &[i64],
        values: &[Value],
        collected_unix_ns: i64,
    ) -> Vec<ContextOutput> {
        assert_eq!(
            timestamps.len(),
            values.len(),
            "decoder guarantees equal lengths"
        );
        let mut out = Vec::new();
        for (&t, v) in timestamps.iter().zip(values) {
            self.report.observations += 1;
            match self.latest {
                Some(latest) if t <= latest => self.late(t, v, &mut out),
                _ => {
                    self.latest = Some(t);
                    let current = self.history.back().map(|(_, v)| v);
                    if current == Some(v) {
                        self.report.repeats += 1;
                        continue;
                    }
                    self.report.changes += 1;
                    out.push(ContextOutput::Change(ContextChange {
                        channel: self.channel.clone(),
                        unix_ns: t,
                        clock: "source".into(),
                        collected_unix_ns,
                        value: v.clone(),
                        previous: current.cloned(),
                    }));
                    self.history.push_back((t, v.clone()));
                    if self.history.len() > self.max_history {
                        self.history.pop_front();
                        self.truncated = true;
                    }
                }
            }
        }
        out
    }

    fn late(&mut self, t: i64, v: &Value, out: &mut Vec<ContextOutput>) {
        let covered = !self.truncated || self.history.front().is_some_and(|(t0, _)| *t0 <= t);
        let in_effect = self.value_at(t).cloned();
        let kind = if !covered {
            self.report.late_unknown += 1;
            ContextIssueKind::LateUnknown
        } else if in_effect.as_ref() == Some(v) {
            self.report.late_consistent += 1;
            return;
        } else {
            self.report.late_conflicts += 1;
            ContextIssueKind::LateConflict
        };
        out.push(ContextOutput::Issue(ContextIssue {
            kind,
            channel: self.channel.clone(),
            unix_ns: t,
            value: v.clone(),
            value_in_effect: if covered { in_effect } else { None },
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ch() -> ChannelId {
        ChannelId {
            source: "m1".into(),
            group: "ctrl".into(),
            signal: "program".into(),
        }
    }

    fn changes(o: &[ContextOutput]) -> Vec<(i64, Value)> {
        o.iter()
            .filter_map(|x| match x {
                ContextOutput::Change(c) => Some((c.unix_ns, c.value.clone())),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn repeated_values_produce_no_events() {
        let mut c = ContextTracker::new(ch());
        let o = c.push(
            &[1, 2, 3, 4, 5, 6],
            &[
                json!(11),
                json!(11),
                json!(11),
                json!(2011),
                json!(2011),
                json!(11),
            ],
            100,
        );
        assert_eq!(
            changes(&o),
            vec![(1, json!(11)), (4, json!(2011)), (6, json!(11))]
        );
        // Next message repeats the value in effect: nothing new.
        assert!(c.push(&[7, 8], &[json!(11), json!(11)], 200).is_empty());
        let r = c.report();
        assert_eq!((r.observations, r.changes, r.repeats), (8, 3, 5));
        let ContextOutput::Change(first) = &o[1] else {
            panic!()
        };
        assert_eq!(first.previous, Some(json!(11)));
        assert_eq!(first.clock, "source");
        assert_eq!(first.collected_unix_ns, 100);
        assert_eq!(c.value_at(5), Some(&json!(2011)));
        assert_eq!(c.value_at(0), None);
    }

    #[test]
    fn late_observations_never_rewrite_the_timeline() {
        let mut c = ContextTracker::new(ch());
        c.push(&[10, 20, 30], &[json!("A"), json!("B"), json!("C")], 1);
        // Late but consistent with the value in effect at 25 ("B").
        assert!(c.push(&[25], &[json!("B")], 2).is_empty());
        // Late and contradicting: reported, timeline unchanged.
        let o = c.push(&[15, 30], &[json!("X"), json!("Z")], 3);
        assert_eq!(o.len(), 2);
        let ContextOutput::Issue(i) = &o[0] else {
            panic!()
        };
        assert_eq!(i.kind, ContextIssueKind::LateConflict);
        assert_eq!(i.value_in_effect, Some(json!("A")));
        assert_eq!(c.value_at(15), Some(&json!("A")));
        assert_eq!(c.value_at(30), Some(&json!("C")));
        // Before the first observation: nothing was in effect.
        let o = c.push(&[5], &[json!("A")], 4);
        let ContextOutput::Issue(i) = &o[0] else {
            panic!()
        };
        assert_eq!(i.kind, ContextIssueKind::LateConflict);
        assert_eq!(i.value_in_effect, None);
        let r = c.report();
        assert_eq!((r.late_consistent, r.late_conflicts), (1, 3));
    }

    #[test]
    fn late_beyond_history_is_unknown() {
        let mut c = ContextTracker::with_history(ch(), 2);
        c.push(&[1, 2, 3], &[json!(1), json!(2), json!(3)], 0);
        let o = c.push(&[1], &[json!(1)], 0);
        let ContextOutput::Issue(i) = &o[0] else {
            panic!()
        };
        assert_eq!(i.kind, ContextIssueKind::LateUnknown);
        assert!(c.push(&[2], &[json!(2)], 0).is_empty(), "still covered");
    }

    #[test]
    fn number_kinds_are_distinct_and_state_roundtrips() {
        let mut c = ContextTracker::new(ch());
        let o = c.push(&[1, 2], &[json!(1), json!(1.0)], 0);
        assert_eq!(changes(&o).len(), 2);
        let saved = serde_json::to_string(&c).unwrap();
        let mut c2: ContextTracker = serde_json::from_str(&saved).unwrap();
        assert!(c2.push(&[3], &[json!(1.0)], 0).is_empty());
        assert_eq!(
            changes(&c2.push(&[4], &[json!(true)], 0)),
            vec![(4, json!(true))]
        );
    }
}

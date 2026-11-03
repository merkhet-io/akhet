//! Placement of timestamped samples on a uniform sample grid.
//!
//! A uniformly sampled recording is described by a sample rate, an anchor
//! timestamp for sample 0 and integer sample offsets. Replacing per-sample
//! timestamps by that grid is only honest if every timestamp lies within a
//! stated tolerance of its grid position. [`ChannelGrid`] enforces that:
//!
//! - A sample within `tolerance` of the next grid position is placed there.
//! - A sample that lands within tolerance of a *later* grid position ends the
//!   recording (`gap`, with the number of missing grid positions) and starts a
//!   new one. Missing values are never invented.
//! - Any other deviation ends the recording (`off_grid`, or `rate_change` when
//!   the re-estimated rate differs) and starts a new one at that sample.
//! - A timestamp at or before the last one seen is never reordered: an exact
//!   repeat (same timestamp, same value bits) within the dedup window is
//!   skipped as a `duplicate`; the same timestamp with a different value is a
//!   `conflict`; anything else is `out_of_order`. All three are counted, and
//!   conflicts and out-of-order samples are reported as issues.
//!
//! Samples are never interpolated, resampled or reordered. Each emitted chunk
//! carries timing evidence: the original timestamp of its first sample and the
//! maximum and RMS deviation of its samples from their grid positions. The
//! reconstructed timestamp of sample `k` is
//! `anchor_unix_ns + round(k * 1e9 / rate_hz)`.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Timing configuration for the channels of one decoder rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimingSpec {
    /// Nominal sample rate. Absent: estimated per recording from the median
    /// timestamp spacing of its first samples.
    #[serde(default)]
    pub rate_hz: Option<f64>,
    /// Estimated rates are rounded to a multiple of this (default 1 Hz).
    #[serde(default = "default_resolution")]
    pub rate_resolution_hz: f64,
    /// Samples used to estimate a rate (default 32, minimum 2).
    #[serde(default = "default_estimate_samples")]
    pub estimate_samples: usize,
    /// Allowed deviation from the grid, in ns. Exclusive with
    /// `tolerance_fraction`.
    #[serde(default)]
    pub tolerance_ns: Option<i64>,
    /// Allowed deviation as a fraction of the sample period (default 0.1).
    #[serde(default)]
    pub tolerance_fraction: Option<f64>,
    /// Recently placed samples remembered to recognise repeats (default 8192).
    #[serde(default = "default_dedup_window")]
    pub dedup_window_samples: usize,
    /// Largest chunk emitted (default 32768 samples).
    #[serde(default = "default_max_chunk")]
    pub max_chunk_samples: usize,
}

fn default_resolution() -> f64 {
    1.0
}
fn default_estimate_samples() -> usize {
    32
}
fn default_dedup_window() -> usize {
    8192
}
fn default_max_chunk() -> usize {
    32768
}

impl Default for TimingSpec {
    fn default() -> Self {
        TimingSpec {
            rate_hz: None,
            rate_resolution_hz: default_resolution(),
            estimate_samples: default_estimate_samples(),
            tolerance_ns: None,
            tolerance_fraction: None,
            dedup_window_samples: default_dedup_window(),
            max_chunk_samples: default_max_chunk(),
        }
    }
}

impl TimingSpec {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(r) = self.rate_hz
            && !(r.is_finite() && r > 0.0 && r <= 1e9)
        {
            return Err(format!("rate_hz {r} must be in (0, 1e9]"));
        }
        if !(self.rate_resolution_hz.is_finite() && self.rate_resolution_hz > 0.0) {
            return Err("rate_resolution_hz must be positive".into());
        }
        if self.estimate_samples < 2 {
            return Err("estimate_samples must be at least 2".into());
        }
        match (self.tolerance_ns, self.tolerance_fraction) {
            (Some(_), Some(_)) => {
                return Err("set tolerance_ns or tolerance_fraction, not both".into());
            }
            (Some(ns), None) if ns < 0 => return Err("tolerance_ns must be >= 0".into()),
            (None, Some(f)) if !(f.is_finite() && (0.0..0.5).contains(&f)) => {
                return Err("tolerance_fraction must be in [0, 0.5)".into());
            }
            _ => {}
        }
        if self.max_chunk_samples == 0 {
            return Err("max_chunk_samples must be positive".into());
        }
        Ok(())
    }

    fn tolerance_for(&self, period_ns: f64) -> f64 {
        match (self.tolerance_ns, self.tolerance_fraction) {
            (Some(ns), _) => ns as f64,
            (None, Some(f)) => f * period_ns,
            (None, None) => 0.1 * period_ns,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SplitReason {
    /// The sample lies on a later grid position: samples are missing.
    Gap,
    /// The sample is off the grid by more than the tolerance.
    OffGrid,
    /// Off the grid, and the re-estimated rate differs from the previous one.
    RateChange,
}

/// Why a recording started where it did, relative to the previous one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Discontinuity {
    pub reason: SplitReason,
    pub previous_recording: String,
    pub previous_rate_hz: f64,
    /// Original timestamp of the previous recording's last sample.
    pub previous_last_unix_ns: i64,
    /// Time from that sample to this recording's first sample.
    pub gap_ns: i64,
    /// For `gap`: grid positions with no sample.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing_samples: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordingStart {
    /// Deterministic key: `<channel key>@<anchor_unix_ns>`.
    pub recording: String,
    pub rate_hz: f64,
    /// Original timestamp of sample 0.
    pub anchor_unix_ns: i64,
    pub rate_estimated: bool,
    pub tolerance_ns: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discontinuity: Option<Discontinuity>,
}

/// Timing evidence for one chunk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkEvidence {
    /// Original timestamp of the chunk's first sample.
    pub first_unix_ns: i64,
    /// Original timestamp of the chunk's last sample.
    pub last_unix_ns: i64,
    /// Largest |original - reconstructed| timestamp in the chunk.
    pub max_abs_deviation_ns: f64,
    pub rms_deviation_ns: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Chunk {
    pub recording: String,
    pub start_sample: u64,
    pub values: Vec<f64>,
    pub evidence: ChunkEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimingIssueKind {
    /// Same timestamp as an earlier sample but a different value.
    Conflict,
    /// Timestamp at or before the latest one, not an exact repeat.
    OutOfOrder,
    /// Too few samples to estimate a rate when the stream was finished.
    Unplaced,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimingIssue {
    pub kind: TimingIssueKind,
    pub unix_ns: i64,
    pub samples: u64,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GridOutput {
    Start(RecordingStart),
    Chunk(Chunk),
    End {
        recording: String,
        samples: u64,
        last_unix_ns: i64,
    },
    Issue(TimingIssue),
}

/// Per-channel totals.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GridReport {
    pub samples_in: u64,
    pub samples_placed: u64,
    pub duplicates_skipped: u64,
    pub conflicts: u64,
    pub out_of_order_rejected: u64,
    pub unplaced: u64,
    pub recordings: u64,
    pub splits_gap: u64,
    pub splits_off_grid: u64,
    pub splits_rate_change: u64,
    pub missing_samples_in_gaps: u64,
    /// Largest |original - reconstructed| timestamp over all placed samples.
    pub max_reconstruction_error_ns: f64,
    pub rms_reconstruction_error_ns: f64,
    /// Placed samples whose deviation exceeded the tolerance. Zero by
    /// construction; reported as a check.
    pub samples_beyond_tolerance: u64,
    #[serde(default)]
    sum_sq_dev: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Active {
    key: String,
    anchor: i64,
    rate_hz: f64,
    period_ns: f64,
    tol_ns: f64,
    next: u64,
    last_ts: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpenChunk {
    start: u64,
    values: Vec<f64>,
    first_ts: i64,
    last_ts: i64,
    max_dev: f64,
    sum_sq: f64,
}

impl OpenChunk {
    fn into_chunk(self, recording: String) -> Chunk {
        let n = self.values.len() as f64;
        Chunk {
            recording,
            start_sample: self.start,
            evidence: ChunkEvidence {
                first_unix_ns: self.first_ts,
                last_unix_ns: self.last_ts,
                max_abs_deviation_ns: self.max_dev,
                rms_deviation_ns: (self.sum_sq / n).sqrt(),
            },
            values: self.values,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Pending {
    samples: Vec<(i64, f64)>,
    discontinuity: Option<Discontinuity>,
}

/// Grid placement state of one channel. Serializable so a consumer can
/// persist it atomically with its own progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelGrid {
    channel_key: String,
    spec: TimingSpec,
    active: Option<Active>,
    pending: Option<Pending>,
    chunk: Option<OpenChunk>,
    /// Recently placed (timestamp, value) pairs, ascending timestamps.
    recent: VecDeque<(i64, f64)>,
    last_seen: Option<i64>,
    last_rate: Option<f64>,
    report: GridReport,
    /// Chunks completed mid-batch (above `max_chunk_samples`), emitted at
    /// the next flush. Always empty between calls.
    #[serde(skip)]
    overflow: Vec<GridOutput>,
}

fn ideal(anchor: i64, period_ns: f64, k: u64) -> i64 {
    anchor + (k as f64 * period_ns).round() as i64
}

impl ChannelGrid {
    pub fn new(channel_key: impl Into<String>, spec: TimingSpec) -> Result<Self, String> {
        spec.validate()?;
        Ok(ChannelGrid {
            channel_key: channel_key.into(),
            spec,
            active: None,
            pending: None,
            chunk: None,
            recent: VecDeque::new(),
            last_seen: None,
            last_rate: None,
            report: GridReport::default(),
            overflow: Vec::new(),
        })
    }

    pub fn report(&self) -> &GridReport {
        &self.report
    }

    pub fn channel_key(&self) -> &str {
        &self.channel_key
    }

    /// Feed one batch of timestamps and values (as produced by the decoder).
    /// Chunks are flushed at the end of the batch. With an estimated rate,
    /// the first samples of a recording are held until `estimate_samples`
    /// have arrived (possibly over several batches) or `finish` is called.
    pub fn push(&mut self, timestamps: &[i64], values: &[f64]) -> Vec<GridOutput> {
        assert_eq!(
            timestamps.len(),
            values.len(),
            "decoder guarantees equal lengths"
        );
        let mut out = Vec::new();
        for (&t, &v) in timestamps.iter().zip(values) {
            self.report.samples_in += 1;
            self.feed(t, v, &mut out);
        }
        self.flush_chunk(&mut out);
        out
    }

    /// True when no samples are held back and no chunk is open.
    pub fn is_flushed(&self) -> bool {
        self.pending.is_none() && self.chunk.is_none()
    }

    /// End the stream: flush, end the active recording and report samples
    /// that could not be placed.
    pub fn finish(&mut self) -> Vec<GridOutput> {
        let mut out = Vec::new();
        if let Some(p) = &self.pending
            && p.samples.len() >= 2
        {
            self.resolve_pending(&mut out);
        }
        if let Some(p) = self.pending.take() {
            let n = p.samples.len() as u64;
            self.report.unplaced += n;
            out.push(GridOutput::Issue(TimingIssue {
                kind: TimingIssueKind::Unplaced,
                unix_ns: p.samples[0].0,
                samples: n,
                message: format!("{n} sample(s) too few to estimate a sample rate"),
            }));
        }
        self.end_active(&mut out);
        out
    }

    fn feed(&mut self, t: i64, v: f64, out: &mut Vec<GridOutput>) {
        if let Some(last) = self.last_seen
            && t <= last
        {
            self.reject_old(t, v, out);
            return;
        }
        self.last_seen = Some(t);
        if self.active.is_some() {
            self.place(t, v, out);
            return;
        }
        let discontinuity = self.pending.as_mut().and_then(|p| p.discontinuity.take());
        self.begin(t, v, discontinuity, out);
    }

    /// Start a recording at (t, v), or hold the sample until a rate can be
    /// estimated.
    fn begin(
        &mut self,
        t: i64,
        v: f64,
        discontinuity: Option<Discontinuity>,
        out: &mut Vec<GridOutput>,
    ) {
        if let Some(rate) = self.spec.rate_hz {
            self.start(t, rate, false, discontinuity, out);
            self.accept(t, v, 0.0);
            return;
        }
        let p = self.pending.get_or_insert(Pending {
            samples: Vec::new(),
            discontinuity: None,
        });
        if p.discontinuity.is_none() {
            p.discontinuity = discontinuity;
        }
        p.samples.push((t, v));
        if p.samples.len() >= self.spec.estimate_samples {
            self.resolve_pending(out);
        }
    }

    fn resolve_pending(&mut self, out: &mut Vec<GridOutput>) {
        let Some(p) = self.pending.take() else { return };
        let mut diffs: Vec<i64> = p.samples.windows(2).map(|w| w[1].0 - w[0].0).collect();
        diffs.sort_unstable();
        let median = diffs[diffs.len() / 2] as f64;
        let res = self.spec.rate_resolution_hz;
        let mut rate = (1e9 / median / res).round() * res;
        if rate <= 0.0 {
            rate = res;
        }
        let mut disc = p.discontinuity;
        if let Some(d) = &mut disc
            && d.reason == SplitReason::OffGrid
            && self.last_rate.is_some_and(|r| r != rate)
        {
            d.reason = SplitReason::RateChange;
            self.report.splits_off_grid -= 1;
            self.report.splits_rate_change += 1;
        }
        let (t0, v0) = p.samples[0];
        self.start(t0, rate, true, disc, out);
        self.accept(t0, v0, 0.0);
        for &(t, v) in &p.samples[1..] {
            if self.active.is_some() {
                self.place(t, v, out);
            } else {
                // A split inside the held samples: keep estimating from there.
                let d = self.pending.as_mut().and_then(|p| p.discontinuity.take());
                self.begin(t, v, d, out);
            }
        }
    }

    fn start(
        &mut self,
        t: i64,
        rate: f64,
        estimated: bool,
        discontinuity: Option<Discontinuity>,
        out: &mut Vec<GridOutput>,
    ) {
        let period_ns = 1e9 / rate;
        let tol = self.spec.tolerance_for(period_ns);
        let key = format!("{}@{}", self.channel_key, t);
        self.report.recordings += 1;
        out.push(GridOutput::Start(RecordingStart {
            recording: key.clone(),
            rate_hz: rate,
            anchor_unix_ns: t,
            rate_estimated: estimated,
            tolerance_ns: tol,
            discontinuity,
        }));
        self.last_rate = Some(rate);
        self.active = Some(Active {
            key,
            anchor: t,
            rate_hz: rate,
            period_ns,
            tol_ns: tol,
            next: 0,
            last_ts: t,
        });
    }

    fn place(&mut self, t: i64, v: f64, out: &mut Vec<GridOutput>) {
        let a = self.active.as_ref().expect("active recording");
        let dev = (t - ideal(a.anchor, a.period_ns, a.next)) as f64;
        if dev.abs() <= a.tol_ns {
            self.accept(t, v, dev);
            return;
        }
        let m = ((t - a.anchor) as f64 / a.period_ns).round();
        let (reason, missing) = if m > a.next as f64
            && m < 9.0e15
            && ((t - ideal(a.anchor, a.period_ns, m as u64)) as f64).abs() <= a.tol_ns
        {
            (SplitReason::Gap, Some(m as u64 - a.next))
        } else {
            (SplitReason::OffGrid, None)
        };
        match reason {
            SplitReason::Gap => {
                self.report.splits_gap += 1;
                self.report.missing_samples_in_gaps += missing.unwrap_or(0);
            }
            _ => self.report.splits_off_grid += 1,
        }
        let (prev_key, prev_rate, prev_last) = (a.key.clone(), a.rate_hz, a.last_ts);
        self.end_active(out);
        let d = Discontinuity {
            reason,
            previous_recording: prev_key,
            previous_rate_hz: prev_rate,
            previous_last_unix_ns: prev_last,
            gap_ns: t - prev_last,
            missing_samples: missing,
        };
        self.begin(t, v, Some(d), out);
    }

    fn accept(&mut self, t: i64, v: f64, dev: f64) {
        let max_chunk = self.spec.max_chunk_samples;
        let a = self.active.as_mut().expect("active recording");
        if dev.abs() > a.tol_ns {
            self.report.samples_beyond_tolerance += 1;
        }
        let k = a.next;
        a.next += 1;
        a.last_ts = t;
        let r = &mut self.report;
        r.samples_placed += 1;
        r.max_reconstruction_error_ns = r.max_reconstruction_error_ns.max(dev.abs());
        r.sum_sq_dev += dev * dev;
        r.rms_reconstruction_error_ns = (r.sum_sq_dev / r.samples_placed as f64).sqrt();
        let c = self.chunk.get_or_insert(OpenChunk {
            start: k,
            values: Vec::new(),
            first_ts: t,
            last_ts: t,
            max_dev: 0.0,
            sum_sq: 0.0,
        });
        c.values.push(v);
        c.last_ts = t;
        c.max_dev = c.max_dev.max(dev.abs());
        c.sum_sq += dev * dev;
        let full = c.values.len() >= max_chunk;
        self.recent.push_back((t, v));
        while self.recent.len() > self.spec.dedup_window_samples {
            self.recent.pop_front();
        }
        if full {
            let c = self.chunk.take().expect("open chunk");
            let key = self.active.as_ref().expect("active").key.clone();
            self.overflow.push(GridOutput::Chunk(c.into_chunk(key)));
        }
    }

    fn flush_chunk(&mut self, out: &mut Vec<GridOutput>) {
        out.append(&mut self.overflow);
        if let (Some(c), Some(a)) = (self.chunk.take(), self.active.as_ref()) {
            out.push(GridOutput::Chunk(c.into_chunk(a.key.clone())));
        }
    }

    fn end_active(&mut self, out: &mut Vec<GridOutput>) {
        self.flush_chunk(out);
        if let Some(a) = self.active.take() {
            out.push(GridOutput::End {
                recording: a.key,
                samples: a.next,
                last_unix_ns: a.last_ts,
            });
        }
    }

    fn reject_old(&mut self, t: i64, v: f64, out: &mut Vec<GridOutput>) {
        let held = self.pending.iter().flat_map(|p| p.samples.iter());
        let found = self
            .recent
            .iter()
            .chain(held)
            .find(|(ts, _)| *ts == t)
            .map(|(_, val)| *val);
        match found {
            Some(prev) if prev.to_bits() == v.to_bits() => {
                self.report.duplicates_skipped += 1;
            }
            Some(prev) => {
                self.report.conflicts += 1;
                out.push(GridOutput::Issue(TimingIssue {
                    kind: TimingIssueKind::Conflict,
                    unix_ns: t,
                    samples: 1,
                    message: format!(
                        "value {v} differs from {prev} already placed at this timestamp"
                    ),
                }));
            }
            None => {
                self.report.out_of_order_rejected += 1;
                out.push(GridOutput::Issue(TimingIssue {
                    kind: TimingIssueKind::OutOfOrder,
                    unix_ns: t,
                    samples: 1,
                    message: format!(
                        "timestamp is not after the latest one ({} ns) and is not a known repeat",
                        self.last_seen.unwrap_or_default()
                    ),
                }));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed(rate: f64, tol_ns: i64) -> ChannelGrid {
        ChannelGrid::new(
            "s/g/x",
            TimingSpec {
                rate_hz: Some(rate),
                tolerance_ns: Some(tol_ns),
                ..Default::default()
            },
        )
        .unwrap()
    }

    fn starts(o: &[GridOutput]) -> Vec<&RecordingStart> {
        o.iter()
            .filter_map(|x| match x {
                GridOutput::Start(s) => Some(s),
                _ => None,
            })
            .collect()
    }

    fn chunks(o: &[GridOutput]) -> Vec<&Chunk> {
        o.iter()
            .filter_map(|x| match x {
                GridOutput::Chunk(c) => Some(c),
                _ => None,
            })
            .collect()
    }

    fn issues(o: &[GridOutput]) -> Vec<TimingIssueKind> {
        o.iter()
            .filter_map(|x| match x {
                GridOutput::Issue(i) => Some(i.kind),
                _ => None,
            })
            .collect()
    }

    /// Reconstruct (timestamp, value) of every placed sample from the outputs.
    fn reconstruct(o: &[GridOutput]) -> Vec<(i64, f64)> {
        let mut rec = std::collections::HashMap::new();
        let mut out = Vec::new();
        for x in o {
            match x {
                GridOutput::Start(s) => {
                    rec.insert(s.recording.clone(), (s.anchor_unix_ns, 1e9 / s.rate_hz));
                }
                GridOutput::Chunk(c) => {
                    let (a, p) = rec[&c.recording];
                    for (i, v) in c.values.iter().enumerate() {
                        out.push((ideal(a, p, c.start_sample + i as u64), *v));
                    }
                }
                _ => {}
            }
        }
        out
    }

    #[test]
    fn regular_grid_across_batches_is_one_recording() {
        let mut g = fixed(1000.0, 0);
        let mut all = Vec::new();
        for b in 0..5i64 {
            let ts: Vec<i64> = (0..100)
                .map(|i| 1_000_000_000 + (b * 100 + i) * 1_000_000)
                .collect();
            let vs: Vec<f64> = (0..100).map(|i| (b * 100 + i) as f64).collect();
            all.extend(g.push(&ts, &vs));
        }
        all.extend(g.finish());
        assert_eq!(starts(&all).len(), 1);
        let c = chunks(&all);
        assert_eq!(c.len(), 5);
        assert_eq!(c[3].start_sample, 300);
        assert_eq!(c[3].evidence.first_unix_ns, 1_300_000_000);
        let r = g.report();
        assert_eq!(r.samples_placed, 500);
        assert_eq!(r.max_reconstruction_error_ns, 0.0);
        assert_eq!(r.samples_beyond_tolerance, 0);
        let rec = reconstruct(&all);
        assert_eq!(rec[499], (1_499_000_000, 499.0));
    }

    #[test]
    fn jitter_within_tolerance_is_kept_and_reported() {
        let mut g = fixed(1000.0, 50_000);
        let jit = [0i64, 30_000, -20_000, 49_000, -49_000, 0];
        let ts: Vec<i64> = jit
            .iter()
            .enumerate()
            .map(|(i, j)| i as i64 * 1_000_000 + j)
            .collect();
        let vs = vec![1.0; ts.len()];
        let mut o = g.push(&ts, &vs);
        o.extend(g.finish());
        assert_eq!(starts(&o).len(), 1);
        let c = chunks(&o);
        assert_eq!(c[0].evidence.max_abs_deviation_ns, 49_000.0);
        assert!(c[0].evidence.rms_deviation_ns > 0.0);
        assert_eq!(g.report().max_reconstruction_error_ns, 49_000.0);
        // Reconstruction error per sample equals the stated jitter.
        for ((t, _), (rt, _)) in ts.iter().zip(&vs).zip(reconstruct(&o)) {
            assert!((t - rt).abs() <= 49_000);
        }
    }

    #[test]
    fn jitter_beyond_tolerance_splits() {
        let mut g = fixed(1000.0, 10_000);
        let ts = [0, 1_000_000, 2_020_000, 3_020_000];
        let o = g.push(&ts, &[1.0, 2.0, 3.0, 4.0]);
        let s = starts(&o);
        assert_eq!(s.len(), 2);
        let d = s[1].discontinuity.as_ref().unwrap();
        assert_eq!(d.reason, SplitReason::OffGrid);
        assert_eq!(d.previous_last_unix_ns, 1_000_000);
        assert_eq!(s[1].anchor_unix_ns, 2_020_000);
        assert_eq!(g.report().samples_beyond_tolerance, 0);
    }

    #[test]
    fn gap_ends_recording_without_inventing_samples() {
        let mut g = fixed(1000.0, 0);
        let ts = [0, 1_000_000, 2_000_000, 7_000_000, 8_000_000];
        let mut o = g.push(&ts, &[1.0, 2.0, 3.0, 8.0, 9.0]);
        o.extend(g.finish());
        let s = starts(&o);
        assert_eq!(s.len(), 2);
        let d = s[1].discontinuity.as_ref().unwrap();
        assert_eq!(d.reason, SplitReason::Gap);
        assert_eq!(d.missing_samples, Some(4));
        assert_eq!(d.gap_ns, 5_000_000);
        assert_eq!(g.report().missing_samples_in_gaps, 4);
        let rec = reconstruct(&o);
        assert_eq!(rec.len(), 5, "no samples invented");
        assert_eq!(rec[3], (7_000_000, 8.0));
        let ends = o
            .iter()
            .filter(|x| matches!(x, GridOutput::End { .. }))
            .count();
        assert_eq!(ends, 2);
    }

    #[test]
    fn duplicates_conflicts_and_out_of_order() {
        let mut g = fixed(1000.0, 0);
        let ts: Vec<i64> = (0..10).map(|i| i * 1_000_000).collect();
        let vs: Vec<f64> = (0..10).map(|i| i as f64).collect();
        let mut o = g.push(&ts, &vs);
        // Redelivery of the whole batch: all skipped as duplicates.
        o.extend(g.push(&ts, &vs));
        assert_eq!(g.report().duplicates_skipped, 10);
        // Same timestamp, different value: conflict.
        o.extend(g.push(&[5_000_000], &[99.0]));
        // A timestamp never seen, in the past: out of order.
        o.extend(g.push(&[5_500_000], &[1.0]));
        // Stream continues unaffected.
        o.extend(g.push(&[10_000_000], &[10.0]));
        o.extend(g.finish());
        assert_eq!(
            issues(&o),
            vec![TimingIssueKind::Conflict, TimingIssueKind::OutOfOrder]
        );
        assert_eq!(starts(&o).len(), 1);
        let r = g.report();
        assert_eq!((r.conflicts, r.out_of_order_rejected), (1, 1));
        assert_eq!(r.samples_placed, 11);
        assert_eq!(r.samples_in, 23);
        let rec = reconstruct(&o);
        assert_eq!(rec.len(), 11);
        assert!(rec.windows(2).all(|w| w[0].0 < w[1].0), "never reordered");
    }

    #[test]
    fn estimated_rate_and_rate_change() {
        let mut g = ChannelGrid::new(
            "k",
            TimingSpec {
                estimate_samples: 8,
                ..Default::default()
            },
        )
        .unwrap();
        // 25.6 kHz: period 39062.5 ns, timestamps rounded to whole ns.
        let p1 = 1e9 / 25_600.0;
        let ts1: Vec<i64> = (0..200).map(|i| (i as f64 * p1).round() as i64).collect();
        let t_switch = ts1[199] + 100_000;
        // Then 10 kHz.
        let ts2: Vec<i64> = (0..200).map(|i| t_switch + i * 100_000).collect();
        let mut o = Vec::new();
        // Small batches: estimation spans batches.
        for c in ts1.chunks(3).chain(ts2.chunks(3)) {
            o.extend(g.push(c, &vec![0.5; c.len()]));
        }
        o.extend(g.finish());
        let s = starts(&o);
        assert_eq!(s.len(), 2, "{s:?}");
        assert_eq!(s[0].rate_hz, 25_600.0);
        assert!(s[0].rate_estimated);
        assert_eq!(s[1].rate_hz, 10_000.0);
        assert_eq!(
            s[1].discontinuity.as_ref().unwrap().reason,
            SplitReason::RateChange
        );
        let r = g.report();
        assert_eq!(r.samples_placed, 400);
        assert!(r.max_reconstruction_error_ns <= 0.5);
        assert_eq!((r.splits_rate_change, r.splits_off_grid), (1, 0));
    }

    #[test]
    fn too_few_samples_to_estimate_is_reported() {
        let mut g = ChannelGrid::new("k", TimingSpec::default()).unwrap();
        let mut o = g.push(&[5], &[1.0]);
        o.extend(g.finish());
        assert_eq!(issues(&o), vec![TimingIssueKind::Unplaced]);
        assert_eq!(g.report().unplaced, 1);
    }

    #[test]
    fn chunks_respect_max_size_and_state_roundtrips() {
        let spec = TimingSpec {
            rate_hz: Some(1000.0),
            max_chunk_samples: 4,
            ..Default::default()
        };
        let mut g = ChannelGrid::new("k", spec).unwrap();
        let ts: Vec<i64> = (0..10).map(|i| i * 1_000_000).collect();
        let o = g.push(&ts, &[1.0; 10]);
        let sizes: Vec<(u64, usize)> = chunks(&o)
            .iter()
            .map(|c| (c.start_sample, c.values.len()))
            .collect();
        assert_eq!(sizes, vec![(0, 4), (4, 4), (8, 2)]);
        // Persist and resume: continues the same recording.
        let saved = serde_json::to_string(&g).unwrap();
        let mut g2: ChannelGrid = serde_json::from_str(&saved).unwrap();
        let o2 = g2.push(&[10_000_000], &[2.0]);
        assert!(starts(&o2).is_empty());
        assert_eq!(chunks(&o2)[0].start_sample, 10);
        assert_eq!(g2.push(&[10_000_000], &[2.0]).len(), 0);
        assert_eq!(g2.report().duplicates_skipped, 1);
    }

    #[test]
    fn spec_validation() {
        let bad = [
            TimingSpec {
                rate_hz: Some(0.0),
                ..Default::default()
            },
            TimingSpec {
                tolerance_ns: Some(1),
                tolerance_fraction: Some(0.1),
                ..Default::default()
            },
            TimingSpec {
                tolerance_fraction: Some(0.5),
                ..Default::default()
            },
            TimingSpec {
                estimate_samples: 1,
                ..Default::default()
            },
            TimingSpec {
                max_chunk_samples: 0,
                ..Default::default()
            },
        ];
        for s in bad {
            assert!(s.validate().is_err(), "{s:?}");
        }
    }
}

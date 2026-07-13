//! Alerting: the rule model plus the *pure* evaluation and state-transition
//! logic. Sans-I/O by construction — this module never runs a query, reads a
//! clock, or touches the network. The `vdg` shell supplies the measured value
//! and the current time; here we only decide firing/OK and when the state
//! changed. That keeps the decision logic deterministic and unit-testable, and
//! lets the same code drive both production and (later) simulation.

use serde::{Deserialize, Serialize};

/// How a measured value is compared against the rule's threshold.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Comparator {
    /// value > threshold
    Gt,
    /// value >= threshold
    Ge,
    /// value < threshold
    Lt,
    /// value <= threshold
    Le,
}

impl Comparator {
    /// Does the comparison hold for this measured value?
    pub fn holds(self, value: f64, threshold: f64) -> bool {
        match self {
            Comparator::Gt => value > threshold,
            Comparator::Ge => value >= threshold,
            Comparator::Lt => value < threshold,
            Comparator::Le => value <= threshold,
        }
    }

    /// Human-readable operator for display (`value > 1000`).
    pub fn symbol(self) -> &'static str {
        match self {
            Comparator::Gt => ">",
            Comparator::Ge => ">=",
            Comparator::Lt => "<",
            Comparator::Le => "<=",
        }
    }
}

fn default_severity() -> String {
    "warning".to_string()
}
fn default_true() -> bool {
    true
}

/// A user-defined alert rule. The `sql` must return a single numeric value — the
/// first row's `v` column if present, else its first numeric column (the shell
/// enforces this). `value <comparator> threshold` decides firing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AlertRule {
    pub id: String,
    pub name: String,
    /// A SQL query that yields one number, e.g.
    /// `SELECT count(*) AS v FROM logs WHERE level = 'ERROR'`.
    pub sql: String,
    pub comparator: Comparator,
    pub threshold: f64,
    /// `critical` | `warning` | `info` — display/paging severity, never price.
    #[serde(default = "default_severity")]
    pub severity: String,
    /// Optional webhook the shell POSTs to on fire/resolve transitions.
    #[serde(default)]
    pub webhook: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

/// Current firing state of a rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Ok,
    Firing,
}

/// Evaluation state that persists across ticks. `since_ms` marks when the rule
/// *entered* its current state, so the UI can show "firing for 3m".
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct AlertStatus {
    pub state: State,
    /// The most recent measured value.
    pub value: f64,
    /// Logical ms when the current `state` was entered.
    pub since_ms: u64,
    /// Logical ms of the last evaluation (0 = never evaluated).
    pub last_eval_ms: u64,
}

impl AlertStatus {
    /// A freshly-created rule starts OK, unevaluated.
    pub fn initial(now: u64) -> Self {
        Self {
            state: State::Ok,
            value: 0.0,
            since_ms: now,
            last_eval_ms: 0,
        }
    }
}

/// A rule together with its evaluation state — the unit we persist.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Alert {
    pub rule: AlertRule,
    pub status: AlertStatus,
}

/// The persisted alerting document (one per table).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AlertsDoc {
    #[serde(default)]
    pub alerts: Vec<Alert>,
}

/// What happened at an evaluation — drives whether the shell notifies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transition {
    /// OK → Firing.
    Fired,
    /// Firing → OK.
    Resolved,
    /// State held.
    Unchanged,
}

/// Pure evaluation step: given the previous status, whether the rule fires on the
/// freshly-measured `value`, and the current time, return the next status and the
/// transition. `since_ms` only resets when the state actually changes, so
/// "firing for how long" is preserved across ticks that stay firing.
pub fn step(prev: &AlertStatus, firing: bool, value: f64, now: u64) -> (AlertStatus, Transition) {
    let new_state = if firing { State::Firing } else { State::Ok };
    let (since_ms, transition) = if new_state != prev.state {
        let t = if firing {
            Transition::Fired
        } else {
            Transition::Resolved
        };
        (now, t)
    } else {
        (prev.since_ms, Transition::Unchanged)
    };
    (
        AlertStatus {
            state: new_state,
            value,
            since_ms,
            last_eval_ms: now,
        },
        transition,
    )
}

/// Convenience: evaluate a rule against a measured value and advance its status.
pub fn evaluate(
    rule: &AlertRule,
    prev: &AlertStatus,
    value: f64,
    now: u64,
) -> (AlertStatus, Transition) {
    let firing = rule.comparator.holds(value, rule.threshold);
    step(prev, firing, value, now)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(cmp: Comparator, threshold: f64) -> AlertRule {
        AlertRule {
            id: "a1".into(),
            name: "test".into(),
            sql: "SELECT 1 AS v".into(),
            comparator: cmp,
            threshold,
            severity: "warning".into(),
            webhook: None,
            enabled: true,
        }
    }

    #[test]
    fn comparators_hold_as_expected() {
        assert!(Comparator::Gt.holds(5.0, 1.0));
        assert!(!Comparator::Gt.holds(1.0, 1.0));
        assert!(Comparator::Ge.holds(1.0, 1.0));
        assert!(Comparator::Lt.holds(0.0, 1.0));
        assert!(Comparator::Le.holds(1.0, 1.0));
        assert!(!Comparator::Le.holds(2.0, 1.0));
    }

    #[test]
    fn ok_to_firing_records_a_fired_transition_and_resets_since() {
        let prev = AlertStatus::initial(100);
        let (next, t) = evaluate(&rule(Comparator::Gt, 10.0), &prev, 42.0, 500);
        assert_eq!(next.state, State::Firing);
        assert_eq!(t, Transition::Fired);
        assert_eq!(next.since_ms, 500); // reset to now on transition
        assert_eq!(next.value, 42.0);
        assert_eq!(next.last_eval_ms, 500);
    }

    #[test]
    fn staying_firing_preserves_since_and_reports_unchanged() {
        let firing = AlertStatus {
            state: State::Firing,
            value: 42.0,
            since_ms: 500,
            last_eval_ms: 500,
        };
        let (next, t) = evaluate(&rule(Comparator::Gt, 10.0), &firing, 99.0, 1500);
        assert_eq!(next.state, State::Firing);
        assert_eq!(t, Transition::Unchanged);
        assert_eq!(next.since_ms, 500); // NOT reset — still the original fire time
        assert_eq!(next.value, 99.0);
        assert_eq!(next.last_eval_ms, 1500);
    }

    #[test]
    fn firing_to_ok_records_a_resolved_transition() {
        let firing = AlertStatus {
            state: State::Firing,
            value: 42.0,
            since_ms: 500,
            last_eval_ms: 500,
        };
        let (next, t) = evaluate(&rule(Comparator::Gt, 10.0), &firing, 3.0, 2000);
        assert_eq!(next.state, State::Ok);
        assert_eq!(t, Transition::Resolved);
        assert_eq!(next.since_ms, 2000);
    }

    #[test]
    fn doc_round_trips_through_json() {
        let doc = AlertsDoc {
            alerts: vec![Alert {
                rule: rule(Comparator::Ge, 500.0),
                status: AlertStatus::initial(0),
            }],
        };
        let bytes = serde_json::to_vec(&doc).unwrap();
        let back: AlertsDoc = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.alerts.len(), 1);
        assert_eq!(back.alerts[0].rule.comparator, Comparator::Ge);
        assert_eq!(back.alerts[0].rule.threshold, 500.0);
    }
}

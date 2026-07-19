//! Manifest-size gate for the trigram index (issue #5).
//!
//! The trigram presence bitmap used to dominate a manifest entry: ~8.4 KB of
//! ~8.9 KB, a fixed 6,332-byte bitmap holding a few hundred set bits. Sparse
//! encoding stores the indices instead. This test defends the resulting shrink
//! as a number in CI rather than a claim in a PR body.
//!
//! **Group by tier, not by level.** A trigram set is per *file*, and files are
//! partitioned by the severity routing (`RoutingConfig::tier_for`), so a real
//! file holds one tier's levels — never all four. Measuring a set built from
//! every severity pooled together understates the shrink badly (5.7x vs 14x),
//! because it unions four disjoint message vocabularies that never share a file.
//! That mistake is what the `mixed_severity_understates_the_shrink` case below
//! pins, so nobody re-derives the wrong number from a plausible-looking script.
//!
//! Thresholds are floors with headroom, not the measured values: this is a
//! regression gate, and it should fail on a real encoding regression, not on
//! ordinary drift in the generator's fixtures.

use verdigris_core::config::RoutingConfig;
use verdigris_core::model::Tier;
use verdigris_core::text::TrigramSet;

/// Base64 length of the old fixed-size dense bitmap — the baseline we shrink from.
const DENSE_B64: usize = 6332usize.div_ceil(3) * 4; // 8_444

struct Measured {
    trigrams: usize,
    percent_full: f64,
    encoded_bytes: usize,
    shrink: f64,
}

fn measure<'a>(messages: impl Iterator<Item = &'a str>) -> Measured {
    let mut t = TrigramSet::new();
    for m in messages {
        t.insert_text(m);
    }
    let encoded_bytes = t.to_base64().len();
    Measured {
        trigrams: t.len(),
        percent_full: t.len() as f64 / 50_653.0 * 100.0,
        encoded_bytes,
        shrink: DENSE_B64 as f64 / encoded_bytes as f64,
    }
}

/// Records grouped the way ingest actually writes them: one bucket per tier.
/// Keyed by `Tier::index()`, the codebase's own idiom for a `[_; 3]` per tier
/// (`Tier` is deliberately not `Ord`, and this test is no reason to make it so).
fn by_tier(n: usize, seed: u64) -> [Vec<String>; 3] {
    let routing = RoutingConfig::default();
    let mut out: [Vec<String>; 3] = Default::default();
    for r in verdigris_ingest::generate::generate(n, seed, 0) {
        out[routing.tier_for(r.level).index()].push(r.message);
    }
    out
}

#[test]
fn per_tier_manifest_entries_shrink_by_at_least_8x() {
    let buckets = by_tier(20_000, 7);

    for tier in Tier::ALL {
        let msgs = &buckets[tier.index()];
        assert!(!msgs.is_empty(), "{tier:?} tier had no records to measure");
        let m = measure(msgs.iter().map(String::as_str));
        println!(
            "{:?}: {} trigrams ({:.2}% full) | {}B -> {}B = {:.1}x",
            tier, m.trigrams, m.percent_full, DENSE_B64, m.encoded_bytes, m.shrink
        );

        // The issue measured 0.2-0.7% fullness on real data; the bundled
        // generator lands in the same band once files are tier-partitioned.
        assert!(
            m.percent_full < 1.0,
            "{tier:?}: {:.2}% full — sparse encoding only pays while bitmaps are \
             sparse. If real messages got this varied, revisit the encoding.",
            m.percent_full
        );

        // Floor, not the measured value. Measured at time of writing:
        // Hot (ERROR) 13.9x, Warm (WARN+INFO) ~9x, Cold (DEBUG) 29.7x.
        assert!(
            m.shrink >= 8.0,
            "{tier:?}: only {:.1}x shrink ({}B). The trigram bitmap is the bulk \
             of a manifest entry; a regression here is a manifest-scaling \
             regression.",
            m.shrink,
            m.encoded_bytes
        );
    }
}

/// The hot tier is the one that matters most — it is queried interactively and
/// holds the most files — and it is the composition issue #5's ~14x refers to.
#[test]
fn hot_tier_meets_the_headline_shrink() {
    let buckets = by_tier(20_000, 7);
    let hot = &buckets[Tier::Hot.index()];
    assert!(!hot.is_empty(), "hot tier had no records");
    let m = measure(hot.iter().map(String::as_str));
    assert!(
        m.shrink >= 12.0,
        "hot tier shrink fell to {:.1}x (was 13.9x); issue #5's acceptance is ~14x",
        m.shrink
    );
}

/// Pins the measurement mistake itself.
///
/// Building one set from every severity pooled together reports ~5.7x and looks
/// like the encoding underdelivers. It does not — that composition never reaches
/// disk, because severity routing puts each level in a different tier's files.
/// If this assertion ever fails because mixed and per-tier converge, the routing
/// changed and the per-tier thresholds above need rechecking.
#[test]
fn mixed_severity_understates_the_shrink() {
    let buckets = by_tier(20_000, 7);
    let mixed = measure(buckets.iter().flatten().map(String::as_str));
    let hot = measure(buckets[Tier::Hot.index()].iter().map(String::as_str));
    assert!(
        mixed.shrink < hot.shrink,
        "pooling all severities ({:.1}x) should look worse than a real \
         tier-partitioned file ({:.1}x) — if it no longer does, severity routing \
         changed and these thresholds need revisiting",
        mixed.shrink,
        hot.shrink
    );
}

/// A saturated bitmap must fall back to the dense form rather than inflating.
#[test]
fn dense_sets_never_encode_larger_than_the_old_format() {
    let mut t = TrigramSet::new();
    // Enough varied text to push well past the sparse break-even (3,164 trigrams).
    for a in 'a'..='z' {
        for b in 'a'..='z' {
            for c in 'a'..='z' {
                t.insert_text(&format!("{a}{b}{c}"));
            }
        }
    }
    assert!(
        t.to_base64().len() <= DENSE_B64,
        "a dense set encoded to {}B, larger than the {}B dense baseline",
        t.to_base64().len(),
        DENSE_B64
    );
}

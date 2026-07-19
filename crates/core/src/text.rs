//! Free-text ("grep") pruning: per-file character-trigram presence sets.
//!
//! The search DSL's free-text term compiles to `message ILIKE '%term%'` — a
//! case-insensitive **substring** match. A word/token index cannot prune that
//! safely: `auth` must match a message containing "authentication", but a token
//! index records only whole words and would prune the file as "auth"-free —
//! silently dropping real matches. Character trigrams can prune it: if `term`
//! occurs anywhere in a message, every trigram of `term` occurs in that message,
//! so a file whose recorded trigram set is missing ANY trigram of the term
//! provably contains no match and can be skipped before Parquet is opened.
//!
//! The alphabet is collapsed to 37 symbols (a-z, 0-9, one "other" bucket for
//! everything else, after ASCII-lowercasing). Text and query collapse the same
//! way, so a real occurrence always survives the collapse — pruning stays
//! conservative. 37³ = 50,653 possible trigrams fit an exact 6,332-byte bitmap:
//! no hash collisions, no false negatives by construction, deterministic
//! (DST-safe), dependency-free. The bitmap serializes as base64 in the manifest.
//!
//! False positives (a file kept that turns out matchless) only cost a scan the
//! estimator already priced; false negatives are impossible by construction —
//! the same "provably free or don't prune" rule as the `service`/`level` stats.

use serde::{Deserialize, Serialize};

/// Collapsed alphabet size: a-z (26) + 0-9 (10) + other (1).
const SYMBOLS: usize = 37;
const NUM_TRIGRAMS: usize = SYMBOLS * SYMBOLS * SYMBOLS; // 50_653
/// Bitmap size: one bit per possible trigram.
pub const BITMAP_BYTES: usize = NUM_TRIGRAMS.div_ceil(8); // 6_332

/// Leading byte marking the sparse wire format. Chosen so a truncated or
/// zero-filled buffer (`0x00`) is not mistaken for a valid sparse payload.
const SPARSE_TAG: u8 = 0x01;
/// Tag + u16 count.
const SPARSE_HEADER: usize = 3;

/// Map a char to its collapsed symbol. Every char maps somewhere (multi-byte
/// chars land in "other"), so text and query stay aligned position-for-position.
fn sym(c: char) -> u16 {
    match c {
        'a'..='z' => c as u16 - 'a' as u16,
        'A'..='Z' => c as u16 - 'A' as u16,
        '0'..='9' => 26 + (c as u16 - '0' as u16),
        _ => 36,
    }
}

fn trigram_index(a: u16, b: u16, c: u16) -> usize {
    a as usize * SYMBOLS * SYMBOLS + b as usize * SYMBOLS + c as usize
}

/// The set of character trigrams present in a file's `message` column.
#[derive(Clone, PartialEq, Eq)]
pub struct TrigramSet {
    bits: Vec<u8>, // always BITMAP_BYTES long
}

impl Default for TrigramSet {
    fn default() -> Self {
        Self::new()
    }
}

impl TrigramSet {
    pub fn new() -> Self {
        Self {
            bits: vec![0u8; BITMAP_BYTES],
        }
    }

    fn set(&mut self, idx: usize) {
        self.bits[idx / 8] |= 1 << (idx % 8);
    }

    fn get(&self, idx: usize) -> bool {
        self.bits[idx / 8] & (1 << (idx % 8)) != 0
    }

    /// Record every trigram of `text` (collapsed alphabet).
    pub fn insert_text(&mut self, text: &str) {
        let mut win = [0u16; 3];
        let mut n = 0usize;
        for c in text.chars() {
            win[0] = win[1];
            win[1] = win[2];
            win[2] = sym(c);
            n += 1;
            if n >= 3 {
                self.set(trigram_index(win[0], win[1], win[2]));
            }
        }
    }

    /// Can a message containing `term` as a substring exist in this file?
    ///
    /// - `Some(false)` — provably not: some trigram of `term` was never recorded.
    /// - `Some(true)` — possibly: every trigram of `term` is present.
    /// - `None` — can't judge: `term` is shorter than one trigram. Callers must
    ///   treat `None` as "may match" (never prune on it).
    pub fn contains_term(&self, term: &str) -> Option<bool> {
        let syms: Vec<u16> = term.chars().map(sym).collect();
        if syms.len() < 3 {
            return None;
        }
        Some(
            syms.windows(3)
                .all(|w| self.get(trigram_index(w[0], w[1], w[2]))),
        )
    }

    /// Fold `other` into this set.
    ///
    /// Used to build a file-level set from its row-group sets: a file's trigrams
    /// are exactly the union of its row groups', so the two levels are recorded
    /// once and derived rather than accumulated twice over the same rows. That
    /// also makes them consistent by construction — a file-level set that had
    /// drifted from the union of its parts could prune a file whose row groups
    /// still claim a match.
    pub fn union_with(&mut self, other: &TrigramSet) {
        for (dst, src) in self.bits.iter_mut().zip(other.bits.iter()) {
            *dst |= *src;
        }
    }

    /// Number of trigrams recorded.
    pub fn len(&self) -> usize {
        self.bits.iter().map(|b| b.count_ones() as usize).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Wire encoding. Two formats; the decoder tells them apart by length alone.
    ///
    /// Real bitmaps are 0.2–0.7% full — logs are templated, so the same trigrams
    /// recur — and a dense 6,332-byte bitmap to hold ~250 set bits dominated the
    /// manifest entry (~8.4 KB of ~8.9 KB). Storing the set indices instead costs
    /// 2 bytes each.
    ///
    /// Note this is exactly what a Roaring bitmap would do here and no more:
    /// `NUM_TRIGRAMS` (50,653) is below 65,536, so the whole space is a single
    /// Roaring *array container* — a sorted `u16` array. Pulling in the crate
    /// would add a dependency to core (which is deliberately dependency-free)
    /// and produce the same bytes.
    ///
    /// Sparse payload: `[0x01][u16 count LE][u16 index LE]…`, so its length is
    /// `3 + 2n` — always odd, and the dense form is always 6,332 (even). The two
    /// can never be confused, and old manifests still decode.
    pub fn to_base64(&self) -> String {
        let n = self.len();
        // Only when it actually wins; a dense set falls back to raw, never worse.
        if SPARSE_HEADER + 2 * n < BITMAP_BYTES {
            let mut out = Vec::with_capacity(SPARSE_HEADER + 2 * n);
            out.push(SPARSE_TAG);
            out.extend_from_slice(&(n as u16).to_le_bytes());
            for idx in 0..NUM_TRIGRAMS {
                if self.get(idx) {
                    out.extend_from_slice(&(idx as u16).to_le_bytes());
                }
            }
            base64_encode(&out)
        } else {
            base64_encode(&self.bits)
        }
    }

    /// Decode from base64; `None` on malformed input (a corrupt stat must read as
    /// "not recorded", never as a pruning license — a wrongly-decoded bitmap
    /// could prune away real matches).
    pub fn from_base64(s: &str) -> Option<Self> {
        let raw = base64_decode(s)?;

        // Legacy/dense: exactly one bit per possible trigram.
        if raw.len() == BITMAP_BYTES {
            return Some(Self { bits: raw });
        }

        // Sparse: tag, count, then that many u16 indices — and nothing trailing.
        if raw.len() < SPARSE_HEADER || raw[0] != SPARSE_TAG {
            return None;
        }
        let n = u16::from_le_bytes([raw[1], raw[2]]) as usize;
        if raw.len() != SPARSE_HEADER + 2 * n {
            return None;
        }
        let mut set = Self::new();
        for chunk in raw[SPARSE_HEADER..].chunks_exact(2) {
            let idx = u16::from_le_bytes([chunk[0], chunk[1]]) as usize;
            // An out-of-range index means corruption. Refuse rather than clamp:
            // silently dropping it would under-record the set, and an
            // under-recorded set prunes files that may hold real matches.
            if idx >= NUM_TRIGRAMS {
                return None;
            }
            set.set(idx);
        }
        Some(set)
    }
}

impl std::fmt::Debug for TrigramSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let set: u32 = self.bits.iter().map(|b| b.count_ones()).sum();
        write!(f, "TrigramSet({set} trigrams)")
    }
}

impl Serialize for TrigramSet {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_base64())
    }
}

impl<'de> Deserialize<'de> for TrigramSet {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        TrigramSet::from_base64(&s)
            .ok_or_else(|| serde::de::Error::custom("malformed trigram bitmap"))
    }
}

// Minimal standard base64 (with padding) — core stays dependency-free.

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b1 = chunk[0] as u32;
        let b2 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b3 = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (b1 << 16) | (b2 << 8) | b3;
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            B64[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

fn b64_val(c: u8) -> Option<u32> {
    match c {
        b'A'..=b'Z' => Some((c - b'A') as u32),
        b'a'..=b'z' => Some((c - b'a') as u32 + 26),
        b'0'..=b'9' => Some((c - b'0') as u32 + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let s = s.as_bytes();
    if !s.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    for chunk in s.chunks(4) {
        // Padding is only valid in the final chunk's tail positions.
        let pad = chunk.iter().rev().take_while(|&&c| c == b'=').count();
        if pad > 2 || chunk[..4 - pad].contains(&b'=') {
            return None;
        }
        let mut n = 0u32;
        for &c in chunk {
            n = (n << 6) | if c == b'=' { 0 } else { b64_val(c)? };
        }
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set_of(texts: &[&str]) -> TrigramSet {
        let mut t = TrigramSet::new();
        for s in texts {
            t.insert_text(s);
        }
        t
    }

    // The safety property: any substring (≥3 chars) of any recorded message must
    // never be judged absent — a false negative here would drop real search hits.
    #[test]
    fn sparse_encoding_shrinks_a_realistic_bitmap() {
        // Templated log lines: the shape the 0.2-0.7% fullness figure comes from.
        let mut t = TrigramSet::new();
        for i in 0..200 {
            t.insert_text(&format!("GET /v1/query status=200 latency_ms={i}"));
        }
        let encoded = t.to_base64();
        let dense = base64_encode(&t.bits).len();
        assert!(
            encoded.len() * 4 < dense,
            "expected a large shrink, got {} vs dense {} ({} trigrams)",
            encoded.len(),
            dense,
            t.len()
        );
    }

    #[test]
    fn both_encodings_round_trip_and_prune_identically() {
        let mut t = TrigramSet::new();
        for i in 0..50 {
            t.insert_text(&format!("connection refused to shard-{i}"));
        }

        // Sparse (what to_base64 picks) and dense must decode to the same set.
        let sparse = TrigramSet::from_base64(&t.to_base64()).expect("sparse decodes");
        let dense = TrigramSet::from_base64(&base64_encode(&t.bits)).expect("dense decodes");
        assert_eq!(sparse, t, "sparse round-trip changed the set");
        assert_eq!(dense, t, "dense round-trip changed the set");

        // The property that actually matters: identical pruning decisions.
        for term in [
            "connection",
            "refused",
            "shard-7",
            "absent-term",
            "zzz",
            "sh",
        ] {
            assert_eq!(
                sparse.contains_term(term),
                t.contains_term(term),
                "sparse disagreed on {term}"
            );
            assert_eq!(
                dense.contains_term(term),
                t.contains_term(term),
                "dense disagreed on {term}"
            );
        }
    }

    #[test]
    fn dense_bitmap_falls_back_to_raw_never_worse() {
        // A saturated set must not be inflated by the sparse path.
        let mut t = TrigramSet::new();
        for idx in 0..NUM_TRIGRAMS {
            t.set(idx);
        }
        assert_eq!(
            t.to_base64(),
            base64_encode(&t.bits),
            "a full bitmap should encode densely"
        );
    }

    #[test]
    fn legacy_dense_manifests_still_decode() {
        // Forward compatibility: manifests written before the sparse format.
        let mut t = TrigramSet::new();
        t.insert_text("legacy payload");
        let legacy = base64_encode(&t.bits);
        assert_eq!(TrigramSet::from_base64(&legacy).as_ref(), Some(&t));
    }

    #[test]
    fn corrupt_sparse_payloads_read_as_not_recorded() {
        // Never decode a corrupt stat into a usable set: an under-recorded set
        // would prune files that may hold real matches.
        let bad_tag = base64_encode(&[0x02, 1, 0, 5, 0]);
        assert_eq!(TrigramSet::from_base64(&bad_tag), None, "wrong tag");

        let short = base64_encode(&[SPARSE_TAG, 9, 0, 5, 0]);
        assert_eq!(
            TrigramSet::from_base64(&short),
            None,
            "count exceeds payload"
        );

        let mut oor = vec![SPARSE_TAG, 1, 0];
        oor.extend_from_slice(&(NUM_TRIGRAMS as u16).to_le_bytes());
        assert_eq!(
            TrigramSet::from_base64(&base64_encode(&oor)),
            None,
            "out-of-range index"
        );
    }

    #[test]
    fn no_false_negatives_for_any_recorded_substring() {
        let messages = [
            "connection timeout to db-primary after 3000ms",
            "java.lang.NullPointerException at AuthFilter.doFilter(AuthFilter.java:42)",
            "TLS handshake failed: x509 certificate expired!",
        ];
        let t = set_of(&messages);
        for msg in &messages {
            let chars: Vec<char> = msg.chars().collect();
            for start in 0..chars.len() {
                for end in (start + 3)..=chars.len() {
                    let term: String = chars[start..end].iter().collect();
                    assert_ne!(
                        t.contains_term(&term),
                        Some(false),
                        "false negative for substring {term:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn absent_terms_are_provably_absent() {
        let t = set_of(&["connection timeout to db-primary"]);
        assert_eq!(t.contains_term("kubelet"), Some(false));
        assert_eq!(t.contains_term("zzzqqq"), Some(false));
        // Present-as-substring inside a word — the case token indexes get wrong.
        assert_eq!(t.contains_term("nnect"), Some(true));
    }

    #[test]
    fn matching_is_case_insensitive_like_ilike() {
        let t = set_of(&["Java.Lang.NullPointerException"]);
        assert_eq!(t.contains_term("nullpointer"), Some(true));
        assert_eq!(t.contains_term("NULLPOINTER"), Some(true));
    }

    #[test]
    fn short_terms_cannot_judge() {
        let t = set_of(&["hello world"]);
        assert_eq!(t.contains_term("he"), None);
        assert_eq!(t.contains_term(""), None);
    }

    #[test]
    fn punctuation_collapses_consistently() {
        let t = set_of(&["error: disk full"]);
        // Punctuation collapses to one "other" symbol, so a term punctuated
        // differently ("error?") still passes the filter — a safe false positive
        // the engine re-checks against real rows.
        assert_eq!(t.contains_term("error:"), Some(true));
        assert_eq!(t.contains_term("error?"), Some(true));
        // But a term that isn't an ILIKE substring of the text ("error disk" has
        // one separator where the text has two) is correctly proven absent —
        // matching exactly what `message ILIKE '%error disk%'` would return.
        assert_eq!(t.contains_term("error disk"), Some(false));
    }

    #[test]
    fn base64_roundtrip_preserves_the_set() {
        let t = set_of(&["the quick brown fox 0123456789"]);
        let b64 = t.to_base64();
        let back = TrigramSet::from_base64(&b64).expect("roundtrip");
        assert_eq!(t, back);
        assert_eq!(back.contains_term("quick brown"), Some(true));
        assert_eq!(back.contains_term("kubelet"), Some(false));
    }

    #[test]
    fn malformed_base64_is_rejected_not_trusted() {
        assert!(TrigramSet::from_base64("not base64 at all!").is_none());
        assert!(TrigramSet::from_base64("AAAA").is_none()); // wrong size
        assert!(TrigramSet::from_base64("A=AA").is_none()); // interior padding
    }
}

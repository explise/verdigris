//! The seam-discipline gate (ADR-001, issue #31).
//!
//! ADR-001 says the control plane reads time and randomness only through the
//! `Clock` and `Rng` seams. That rule is prose until something enforces it, and
//! prose rots: `RealClock` sat `#[allow(dead_code)]` for weeks while the shell
//! called `SystemTime::now()` in fifteen places.
//!
//! So: a grep, as a test. Every hit must either go through a seam or appear in
//! the exemption table below with a stated reason. Adding an unlisted call fails
//! the build.
//!
//! This is deliberately a source-text check rather than a lint. It is crude, but
//! it cannot be satisfied by a type that merely *looks* like a clock, and it
//! costs nothing to run.

use std::path::{Path, PathBuf};

/// Patterns that must not appear outside the exemptions below.
const BANNED: &[(&str, &str)] = &[
    ("SystemTime::now", "wall clock — use the Clock seam"),
    ("Instant::now", "wall clock — use the Clock seam"),
    ("thread_rng", "unseeded entropy — use the Rng seam"),
    ("rand::random", "unseeded entropy — use the Rng seam"),
    (
        "std::thread::spawn",
        "raw thread — the simulator cannot schedule it",
    ),
    (
        "std::thread::sleep",
        "raw sleep — use Clock::sleep so sim time can drive it",
    ),
];

/// The complete list of legitimate exceptions, as (file, symbol-or-context,
/// why). Anything not listed here is a failure.
///
/// Keep this table short. A growing exemption list means the seam is losing.
const EXEMPT: &[(&str, &str)] = &[
    // The seam implementations themselves. These files exist precisely to be
    // the one place a real time source is read.
    (
        "crates/vdg/src/realclock.rs",
        "IS the production Clock impl — reading the wall clock is its job",
    ),
    // Security: a 256-bit auth token must come from OS entropy. Drawing it from
    // the injected seeded Rng would make tokens reproducible from a seed, which
    // is a vulnerability, not a determinism win. Never "fix" this one.
    (
        "crates/vdg/src/serve.rs:gen_secret",
        "auth tokens require OS entropy; a seeded Rng would make them predictable",
    ),
];

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is crates/vdg
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("repo root")
        .to_path_buf()
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            // Skip build output; it contains vendored sources we do not own.
            if p.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            rust_sources(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// Strip the parts of a line that mention a pattern without calling it, so the
/// remaining text is code only. The ADR and many doc comments name these calls
/// in order to forbid them, and naming is not calling — an earlier version
/// matched only lines *starting* with `//`, so a trailing explanatory comment
/// or an error string containing the pattern failed the build.
fn code_only(line: &str) -> String {
    let t = line.trim_start();
    // Whole-line comments, including block-comment openers and continuations.
    if t.starts_with("//") || t.starts_with("/*") || t.starts_with('*') || t.starts_with("#!") {
        return String::new();
    }
    // Trailing `//` comment.
    let head = match line.find("//") {
        Some(i) => &line[..i],
        None => line,
    };
    // String literals. Crude but adequate: drop everything between double
    // quotes, so `bail!("do not call thread_rng")` no longer trips the gate.
    let mut out = String::with_capacity(head.len());
    let mut in_str = false;
    let mut prev_backslash = false;
    for ch in head.chars() {
        match ch {
            '"' if !prev_backslash => in_str = !in_str,
            _ if !in_str => out.push(ch),
            _ => {}
        }
        prev_backslash = ch == '\\' && !prev_backslash;
    }
    out
}

#[test]
fn no_wall_clock_or_entropy_outside_the_seams() {
    let root = repo_root();
    let mut files = Vec::new();
    rust_sources(&root.join("crates"), &mut files);
    assert!(!files.is_empty(), "found no Rust sources to scan");

    // The `gen_secret` exemption is function-scoped, not file-scoped: the rest
    // of serve.rs must stay clean. Track which function each hit sits in.
    let mut violations: Vec<String> = Vec::new();

    for path in &files {
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");

        // Tests may use real time freely — they are not the control plane.
        if rel.contains("/tests/") || rel.contains("/examples/") {
            continue;
        }

        let file_exempt = EXEMPT.iter().any(|(k, _)| *k == rel);
        if file_exempt {
            continue;
        }

        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        let mut current_fn = String::new();
        // Brace depth at which the current function body started. `current_fn`
        // is cleared when we return to it, so a `static`/`const`/macro at module
        // level after an exempt function does NOT inherit that exemption.
        let mut fn_depth: Option<i32> = None;
        let mut depth: i32 = 0;

        for (i, line) in text.lines().enumerate() {
            let code = code_only(line);
            let trimmed = line.trim_start();
            if fn_depth.is_none()
                && (trimmed.starts_with("fn ")
                    || trimmed.starts_with("pub fn ")
                    || trimmed.starts_with("async fn ")
                    || trimmed.starts_with("pub async fn ")
                    || trimmed.starts_with("pub(crate) fn ")
                    || trimmed.starts_with("pub async fn "))
            {
                current_fn = trimmed
                    .split('(')
                    .next()
                    .unwrap_or("")
                    .rsplit(' ')
                    .next()
                    .unwrap_or("")
                    .trim_end_matches('<')
                    .to_string();
                fn_depth = Some(depth);
            }

            for (pat, why) in BANNED {
                if !code.contains(pat) {
                    continue;
                }
                let scoped = format!("{rel}:{current_fn}");
                if EXEMPT.iter().any(|(k, _)| *k == scoped) {
                    continue;
                }
                violations.push(format!(
                    "{}:{} in `{}` — {} ({})",
                    rel,
                    i + 1,
                    if current_fn.is_empty() {
                        "<module level>"
                    } else {
                        current_fn.as_str()
                    },
                    pat,
                    why
                ));
            }

            depth += code.matches('{').count() as i32;
            depth -= code.matches('}').count() as i32;
            if let Some(d) = fn_depth {
                if depth <= d {
                    fn_depth = None;
                    current_fn.clear();
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "seam discipline violated (ADR-001). Route these through a seam, or add \
         a justified entry to EXEMPT in this file:\n  {}",
        violations.join("\n  ")
    );
}

/// The exemption table is the risk surface, so it gets its own assertion: if it
/// grows, that should be a deliberate, reviewed act rather than a quiet drift.
#[test]
fn exemption_list_stays_small() {
    // Pinned exactly, not bounded: with a `<=` bound an exemption could be added
    // without any test failing, which is precisely the quiet drift this guards
    // against. Changing the list must show up in the diff as a changed number.
    assert_eq!(
        EXEMPT.len(),
        2,
        "the exemption list changed — each entry is a hole in the determinism \
         claim, so update this count deliberately and justify it in review"
    );
}

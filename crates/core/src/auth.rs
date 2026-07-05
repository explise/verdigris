//! Authn/authz model: per-user API tokens with roles. Pure and sans-I/O — the
//! `vdg` shell persists the [`TokensDoc`] (as hashes, never raw secrets), reads
//! the presented bearer token, and calls [`TokensDoc::authenticate`]. Tokens are
//! stored as SHA-256 hashes so a leaked catalog can't be replayed.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Access level, ordered least → most privileged so `role >= required` is the
/// authorization check. `ReadOnly` can query; `ReadWrite` can also ingest and
/// manage alerts; `Admin` can also issue/revoke tokens.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    ReadOnly,
    ReadWrite,
    Admin,
}

impl Role {
    /// Does a token with `self` role satisfy an endpoint needing `required`?
    pub fn permits(self, required: Role) -> bool {
        self >= required
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Role::ReadOnly => "readonly",
            Role::ReadWrite => "readwrite",
            Role::Admin => "admin",
        }
    }
}

/// SHA-256 hex of a token secret. We persist this, never the secret itself.
pub fn hash_token(secret: &str) -> String {
    let digest = Sha256::digest(secret.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// An issued API token. The `hash` is the SHA-256 of the secret handed to the
/// user once at creation; the secret is never stored or logged.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiToken {
    pub id: String,
    pub name: String,
    pub role: Role,
    pub hash: String,
    pub created_ms: u64,
    #[serde(default)]
    pub revoked: bool,
}

/// The persisted token catalog (one per deployment).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TokensDoc {
    #[serde(default)]
    pub tokens: Vec<ApiToken>,
}

impl TokensDoc {
    /// Resolve a presented secret to its active (non-revoked) token, or `None`.
    pub fn authenticate(&self, secret: &str) -> Option<&ApiToken> {
        let h = hash_token(secret);
        // Constant-ish: we compare the already-hashed value; a wrong secret just
        // fails to match any stored hash.
        self.tokens
            .iter()
            .find(|t| !t.revoked && t.hash == h)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_ordering_gates_access() {
        assert!(Role::Admin.permits(Role::ReadWrite));
        assert!(Role::ReadWrite.permits(Role::ReadOnly));
        assert!(Role::ReadOnly.permits(Role::ReadOnly));
        assert!(!Role::ReadOnly.permits(Role::ReadWrite));
        assert!(!Role::ReadWrite.permits(Role::Admin));
    }

    #[test]
    fn hash_is_deterministic_and_not_the_secret() {
        let h = hash_token("s3cret");
        assert_eq!(h, hash_token("s3cret"));
        assert_ne!(h, hash_token("other"));
        assert!(!h.contains("s3cret"));
        assert_eq!(h.len(), 64); // 32 bytes hex
    }

    #[test]
    fn authenticate_matches_only_active_tokens() {
        let doc = TokensDoc {
            tokens: vec![
                ApiToken {
                    id: "1".into(),
                    name: "ci".into(),
                    role: Role::ReadWrite,
                    hash: hash_token("good"),
                    created_ms: 0,
                    revoked: false,
                },
                ApiToken {
                    id: "2".into(),
                    name: "old".into(),
                    role: Role::Admin,
                    hash: hash_token("revoked-secret"),
                    created_ms: 0,
                    revoked: true,
                },
            ],
        };
        assert_eq!(doc.authenticate("good").unwrap().role, Role::ReadWrite);
        assert!(doc.authenticate("revoked-secret").is_none()); // revoked
        assert!(doc.authenticate("nope").is_none()); // unknown
    }
}

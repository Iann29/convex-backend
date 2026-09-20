//! Primitives for the AMAGE verified-deploy gate (`amage.verified-deploy/1`)
//! that need crates `local_backend` does not depend on.
use std::time::SystemTime;

use aws_lc_rs::signature::{
    UnparsedPublicKey,
    ED25519,
};

/// True when `signature` is a valid Ed25519 signature of `message` under the
/// 32 raw bytes of `public_key`.
pub fn verify_ed25519(public_key: &[u8; 32], message: &[u8], signature: &[u8]) -> bool {
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(message, signature)
        .is_ok()
}

/// Parses an RFC 3339 timestamp such as `2026-09-20T18:00:00Z`.
pub fn parse_rfc3339(timestamp: &str) -> Option<SystemTime> {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .ok()
        .map(SystemTime::from)
}

/// Test-only signer: the real private key lives in the external supervisor.
#[cfg(any(test, feature = "testing"))]
pub struct Ed25519Signer(aws_lc_rs::signature::Ed25519KeyPair);

#[cfg(any(test, feature = "testing"))]
impl Ed25519Signer {
    pub fn generate() -> Self {
        Self(aws_lc_rs::signature::Ed25519KeyPair::generate().expect("Ed25519 key generation"))
    }

    pub fn public_key(&self) -> [u8; 32] {
        use aws_lc_rs::signature::KeyPair;
        self.0
            .public_key()
            .as_ref()
            .try_into()
            .expect("Ed25519 public keys are 32 bytes")
    }

    pub fn sign(&self, message: &[u8]) -> Vec<u8> {
        self.0.sign(message).as_ref().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{
        Duration,
        SystemTime,
    };

    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn rfc8032_test_vector_1() {
        let seed = hex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60");
        let public_key: [u8; 32] =
            hex("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
                .try_into()
                .unwrap();
        let signature = hex(
            "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b",
        );
        // The seed really derives that public key.
        aws_lc_rs::signature::Ed25519KeyPair::from_seed_and_public_key(&seed, &public_key).unwrap();
        assert!(verify_ed25519(&public_key, b"", &signature));
        assert!(!verify_ed25519(&public_key, b"x", &signature));
        let mut flipped = signature.clone();
        flipped[0] ^= 1;
        assert!(!verify_ed25519(&public_key, b"", &flipped));
        assert!(!verify_ed25519(&public_key, b"", &signature[..63]));
        let mut other_key = public_key;
        other_key[31] ^= 1;
        assert!(!verify_ed25519(&other_key, b"", &signature));
    }

    #[test]
    fn generated_signer_round_trips() {
        let signer = Ed25519Signer::generate();
        let signature = signer.sign(b"receipt");
        assert!(verify_ed25519(&signer.public_key(), b"receipt", &signature));
        assert!(!verify_ed25519(
            &Ed25519Signer::generate().public_key(),
            b"receipt",
            &signature
        ));
    }

    #[test]
    fn parses_rfc3339_utc() {
        assert_eq!(
            parse_rfc3339("2026-09-20T18:00:00Z"),
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_789_927_200))
        );
        assert_eq!(
            parse_rfc3339("2026-09-20T15:00:00-03:00"),
            parse_rfc3339("2026-09-20T18:00:00Z")
        );
        assert_eq!(parse_rfc3339("2026-09-20 18:00:00"), None);
        assert_eq!(parse_rfc3339("1789927200"), None);
        assert_eq!(parse_rfc3339(""), None);
    }
}

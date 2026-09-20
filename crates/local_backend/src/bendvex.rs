//! AMAGE verified-deploy gate (`amage.verified-deploy/1`).
//!
//! With `AMAGE_VERIFIED_DEPLOY_PUBKEY` set the deployment is *protected*: code
//! only reaches the runtime through `start_push` carrying an `amageVerified`
//! receipt signed by the external verifier, and `finish_push` only accepts the
//! exact response of the last verified `start_push`. Without it the backend
//! behaves like upstream Convex. The private key never lives here.
use std::{
    borrow::Cow,
    sync::OnceLock,
    time::{
        Duration,
        SystemTime,
    },
};

use anyhow::Context;
use application::deploy_config::StartPushRequest;
use common::{
    http::{
        extract::MtState,
        HttpResponseError,
    },
    sha256::{
        Sha256,
        Sha256Digest,
    },
};
use errors::ErrorMetadata;
use keybroker::verified_deploy::{
    parse_rfc3339,
    verify_ed25519,
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::{
    json,
    Value,
};
use tokio::sync::{
    Mutex,
    MutexGuard,
};

use crate::LocalAppState;

pub const PROTOCOL: &str = "amage.verified-deploy/1";
const PUBKEY_ENV: &str = "AMAGE_VERIFIED_DEPLOY_PUBKEY";
const SUPERVISOR_URL_ENV: &str = "AMAGE_VERIFIED_DEPLOY_SUPERVISOR_URL";
/// Tolerated clock skew for `issuedAt`.
const MAX_CLOCK_SKEW: Duration = Duration::from_secs(5 * 60);
/// Longest receipt lifetime the backend accepts.
const MAX_LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, PartialEq, Eq)]
struct Protection {
    public_key: [u8; 32],
    supervisor_url: Option<String>,
}

static PROTECTION: OnceLock<Option<Protection>> = OnceLock::new();
// ponytail: one pending deploy per process; use durable attestations for
// multiple writers.
static PENDING: Mutex<Option<Sha256Digest>> = Mutex::const_new(None);

pub fn initialize() -> anyhow::Result<()> {
    let protection = match std::env::var(PUBKEY_ENV) {
        Ok(hex) => Some(Protection {
            public_key: decode_public_key(&hex)?,
            supervisor_url: match std::env::var(SUPERVISOR_URL_ENV) {
                Ok(url) => Some(url),
                Err(std::env::VarError::NotPresent) => None,
                Err(error) => return Err(error).context(SUPERVISOR_URL_ENV),
            },
        }),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => return Err(error).context(PUBKEY_ENV),
    };
    let existing = PROTECTION.get_or_init(|| protection.clone());
    anyhow::ensure!(
        existing == &protection,
        "verified-deploy configuration cannot change in-process"
    );
    Ok(())
}

fn decode_public_key(hex: &str) -> anyhow::Result<[u8; 32]> {
    anyhow::ensure!(
        hex.len() == 64 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')),
        "{PUBKEY_ENV} must be 64 lowercase hex digits (a raw 32-byte Ed25519 public key)"
    );
    let mut key = [0u8; 32];
    for (byte, pair) in key.iter_mut().zip(hex.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(key)
}

fn protection() -> anyhow::Result<Option<&'static Protection>> {
    Ok(PROTECTION
        .get()
        .context("verified-deploy gate not initialized")?
        .as_ref())
}

fn required() -> anyhow::Error {
    ErrorMetadata::bad_request(
        "VerifiedDeployRequired",
        format!(
            "This deployment only accepts verified deploys ({PROTOCOL}): every push must carry an \
             `amageVerified` receipt issued by the AMAGE supervisor. Run `npx convex dev` or `npx \
             convex deploy` with the AMAGE (C7) Convex CLI. Older Convex CLIs cannot deploy here, \
             and the deployment admin key alone does not authorize a deploy."
        ),
    )
    .into()
}

fn rejected(msg: impl Into<Cow<'static, str>>) -> anyhow::Error {
    ErrorMetadata::bad_request("VerifiedDeployRejected", msg).into()
}

pub fn reject_legacy() -> anyhow::Result<()> {
    if protection()?.is_some() {
        return Err(rejected(
            "legacy push path is disabled on protected deployments",
        ));
    }
    Ok(())
}

/// `GET /api/amage/verified_deploy`: the target speaks for itself.
pub async fn capability_handler(
    MtState(st): MtState<LocalAppState>,
) -> Result<axum::Json<Value>, HttpResponseError> {
    Ok(axum::Json(capability_for(&st.instance_name, protection()?)))
}

fn capability_for(instance_name: &str, protection: Option<&Protection>) -> Value {
    let mut capability = json!({
        "protocol": PROTOCOL,
        "supported": true,
        "required": protection.is_some(),
        "target": {"instanceName": instance_name},
    });
    if let Some(protection) = protection {
        capability["verifierKeyId"] = json!(Sha256::hash(&protection.public_key).as_hex());
        if let Some(url) = &protection.supervisor_url {
            capability["supervisorUrl"] = json!(url);
        }
    }
    capability
}

/// Canonical JSON shared by the backend, the supervisor and the CLI: keys
/// sorted by code point, no whitespace, UTF-8 written raw (`ensure_ascii=False`
/// in Python), and every number hashed as the double JavaScript would send
/// back (`1.0` → `1`, `-0.0` → `0`).
pub fn canonical_json_bytes(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        Value::Number(n) => write_number(n.as_f64().unwrap_or(f64::NAN), out),
        Value::String(s) => write_json(s, out),
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_canonical(item, out);
            }
            out.push(b']');
        },
        Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            out.push(b'{');
            for (i, (key, item)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_json(key, out);
                out.push(b':');
                write_canonical(item, out);
            }
            out.push(b'}');
        },
    }
}

fn write_number(f: f64, out: &mut Vec<u8>) {
    // JavaScript parses every JSON number as an IEEE-754 double and
    // JSON.stringify writes integral doubles below 1e21 without a fraction or
    // an exponent, so hash the digits a Node round trip would send back.
    if f == 0.0 {
        out.push(b'0');
    } else if f.is_finite() && f.fract() == 0.0 && f.abs() < 1e21 {
        out.extend_from_slice(f.to_string().as_bytes());
    } else {
        write_json(&f, out);
    }
}

fn write_json(value: &impl Serialize, out: &mut Vec<u8>) {
    serde_json::to_writer(out, value).expect("in-memory JSON serialization cannot fail");
}

fn digest(value: &Value) -> Sha256Digest {
    Sha256::hash(&canonical_json_bytes(value))
}

fn response_digest(response: &impl Serialize) -> anyhow::Result<Sha256Digest> {
    Ok(digest(&serde_json::to_value(response)?))
}

/// Splits a raw `start_push` body into the signed request (everything but
/// `adminKey` and `amageVerified`) and the receipt envelope, when present.
fn split_request(mut raw: Value) -> anyhow::Result<(Value, Option<Value>)> {
    let request = raw
        .as_object_mut()
        .context("Deploy request must be an object")?;
    request.remove("adminKey");
    let envelope = request.remove("amageVerified");
    Ok((raw, envelope))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Receipt {
    format: u64,
    protocol: String,
    target: Target,
    request_sha256: String,
    issued_at: String,
    expires_at: String,
    signature: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Target {
    instance_name: String,
}

fn check_start(
    raw: Value,
    req: &StartPushRequest,
    instance_name: &str,
    public_key: &[u8; 32],
    now: SystemTime,
) -> anyhow::Result<()> {
    let (request, envelope) = split_request(raw)?;
    let Some(mut envelope) = envelope else {
        return Err(required());
    };
    let receipt: Receipt = serde_json::from_value(envelope.clone())
        .map_err(|e| rejected(format!("receipt envelope is malformed: {e}")))?;
    anyhow::ensure!(
        receipt.protocol == PROTOCOL,
        rejected(format!(
            "receipt protocol {:?} is not {PROTOCOL}",
            receipt.protocol
        ))
    );
    anyhow::ensure!(
        receipt.format == 1,
        rejected(format!(
            "receipt format {} is not supported (expected 1)",
            receipt.format
        ))
    );
    anyhow::ensure!(
        receipt.target.instance_name == instance_name,
        rejected(format!(
            "receipt targets instance {}, this is {instance_name}",
            receipt.target.instance_name
        ))
    );
    anyhow::ensure!(
        receipt.request_sha256 == digest(&request).as_hex(),
        rejected("request digest does not match receipt")
    );
    check_window(&receipt.issued_at, &receipt.expires_at, now)?;
    let signature = base64::decode(&receipt.signature)
        .map_err(|_| rejected("receipt signature is not valid base64"))?;
    envelope
        .as_object_mut()
        .context("receipt envelope must be an object")?
        .remove("signature");
    anyhow::ensure!(
        verify_ed25519(public_key, &canonical_json_bytes(&envelope), &signature),
        rejected("receipt signature invalid")
    );
    check_shape(req)
}

fn check_window(issued_at: &str, expires_at: &str, now: SystemTime) -> anyhow::Result<()> {
    let parse = |field: &str, value: &str| {
        parse_rfc3339(value).ok_or_else(|| {
            rejected(format!(
                "receipt {field} {value:?} is not an RFC 3339 timestamp"
            ))
        })
    };
    let issued = parse("issuedAt", issued_at)?;
    let expires = parse("expiresAt", expires_at)?;
    anyhow::ensure!(
        issued <= now + MAX_CLOCK_SKEW,
        rejected(format!(
            "receipt issued at {issued_at} is more than 5 minutes in the future"
        ))
    );
    anyhow::ensure!(
        expires >= now,
        rejected(format!("receipt expired at {expires_at}"))
    );
    let lifetime = expires.duration_since(issued).map_err(|_| {
        rejected(format!(
            "receipt expires at {expires_at}, before it was issued at {issued_at}"
        ))
    })?;
    anyhow::ensure!(
        lifetime <= MAX_LIFETIME,
        rejected(format!(
            "receipt validity window from {issued_at} to {expires_at} exceeds 24 hours"
        ))
    );
    Ok(())
}

fn check_shape(req: &StartPushRequest) -> anyhow::Result<()> {
    let refused = if !req.component_definitions.is_empty() {
        "components are not allowed on protected deployments"
    } else if !req.node_dependencies.is_empty() || req.node_version.is_some() {
        "Node.js actions are not allowed on protected deployments"
    } else if req.dry_run {
        "dry runs are not allowed on protected deployments"
    } else if req.for_codegen {
        "codegen-only pushes are not allowed on protected deployments"
    } else if req.app_definition.definition.is_some() || !req.app_definition.dependencies.is_empty()
    {
        "component definitions (convex.config) are not allowed on protected deployments"
    } else if !req.app_definition.unchanged_module_hashes.is_empty() {
        "incremental pushes are not allowed on protected deployments; send every module"
    } else if req
        .app_definition
        .changed_modules
        .iter()
        .any(|m| m.environment.as_deref() != Some("isolate"))
    {
        "every module must run in the isolate environment on protected deployments"
    } else {
        return Ok(());
    };
    Err(rejected(refused))
}

pub async fn start(
    raw: Value,
    req: &StartPushRequest,
    instance_name: &str,
) -> anyhow::Result<Option<MutexGuard<'static, Option<Sha256Digest>>>> {
    let Some(protection) = protection()? else {
        return Ok(None);
    };
    let mut pending = PENDING.lock().await;
    // A new attempt invalidates any previous response, including on rejection.
    *pending = None;
    check_start(
        raw,
        req,
        instance_name,
        &protection.public_key,
        SystemTime::now(),
    )?;
    Ok(Some(pending))
}

pub fn remember(
    guard: &mut Option<MutexGuard<'static, Option<Sha256Digest>>>,
    response: &impl Serialize,
) -> anyhow::Result<()> {
    if let Some(pending) = guard {
        **pending = Some(response_digest(response)?);
    }
    Ok(())
}

pub async fn finish(
    response: &impl Serialize,
) -> anyhow::Result<Option<MutexGuard<'static, Option<Sha256Digest>>>> {
    if protection()?.is_none() {
        return Ok(None);
    }
    let mut pending = PENDING.lock().await;
    consume(&mut pending, response)?;
    Ok(Some(pending))
}

fn consume(pending: &mut Option<Sha256Digest>, response: &impl Serialize) -> anyhow::Result<()> {
    let Some(expected) = pending.take() else {
        return Err(rejected(
            "finish_push has no verified start_push pending; run start_push again",
        ));
    };
    anyhow::ensure!(
        expected == response_digest(response)?,
        rejected("finish_push payload does not match the verified start_push response")
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use errors::ErrorMetadataAnyhowExt;
    use keybroker::verified_deploy::Ed25519Signer;

    use super::*;

    fn canonical(value: &Value) -> String {
        String::from_utf8(canonical_json_bytes(value)).unwrap()
    }

    #[test]
    fn canonical_json_sorts_keys_and_normalizes_numbers() {
        let a = json!({"b": 1.0, "a": {"y": -0.0, "x": [1, 2]}, "c": 2f64.powi(60)});
        let b = json!({"a": {"x": [1, 2], "y": 0}, "c": 1152921504606847000u64, "b": 1});
        assert_eq!(canonical(&a), canonical(&b));
        assert_eq!(
            canonical(&b),
            r#"{"a":{"x":[1,2],"y":0},"b":1,"c":1152921504606847000}"#
        );
        assert_eq!(
            canonical(&json!([0.5, 1e21, 1e-7, -3, 1e20])),
            "[0.5,1e+21,1e-7,-3,100000000000000000000]"
        );
        assert_ne!(canonical(&json!([1, 2])), canonical(&json!([2, 1])));
        assert_eq!(canonical(&json!({})), "{}");
    }

    #[test]
    fn canonical_json_matches_python() {
        // python3 -c 'import json,hashlib; v={"ç":"ação 🧪","nested":{"é":1,"A":"\n\t\"\x01\x7f\\/"},"list":["β","東京",True,None,2.5,-3]}; s=json.dumps(v,sort_keys=True,separators=(",",":"),ensure_ascii=False); print(s); print(hashlib.sha256(s.encode()).hexdigest())'
        let value = json!({
            "ç": "ação 🧪",
            "nested": {"é": 1, "A": "\n\t\"\u{1}\u{7f}\\/"},
            "list": ["β", "東京", true, null, 2.5, -3],
        });
        assert_eq!(
            canonical(&value),
            concat!(
                "{\"list\":[\"β\",\"東京\",true,null,2.5,-3],",
                "\"nested\":{\"A\":\"\\n\\t\\\"\\u0001\u{7f}\\\\/\",\"é\":1},",
                "\"ç\":\"ação 🧪\"}"
            )
        );
        assert_eq!(
            digest(&value).as_hex(),
            "81b9bac30c9b939dae437d0c3b69a8b1278207ebe6a6a813eda6416c429aa0ff"
        );
    }

    #[test]
    fn request_digest_ignores_admin_key_and_envelope_only() -> anyhow::Result<()> {
        let (a, envelope) = split_request(
            json!({"adminKey": "secret", "amageVerified": {"format": 1}, "functions": "convex/"}),
        )?;
        let (b, none) = split_request(json!({"functions": "convex/", "adminKey": "other"}))?;
        assert_eq!(digest(&a), digest(&b));
        assert_eq!(envelope, Some(json!({"format": 1})));
        assert_eq!(none, None);
        let (c, _) = split_request(json!({"functions": "other/"}))?;
        assert_ne!(digest(&a), digest(&c));
        Ok(())
    }

    #[test]
    fn public_key_must_be_64_lowercase_hex() {
        let hex = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
        assert_eq!(decode_public_key(hex).unwrap()[..2], [0xd7, 0x5a]);
        assert!(decode_public_key(&hex.to_uppercase()).is_err());
        assert!(decode_public_key(&hex[..62]).is_err());
        assert!(decode_public_key(&format!("{hex}00")).is_err());
    }

    #[test]
    fn capability_shapes() {
        assert_eq!(
            capability_for("inst", None),
            json!({
                "protocol": "amage.verified-deploy/1",
                "supported": true,
                "required": false,
                "target": {"instanceName": "inst"},
            })
        );
        let protection = Protection {
            public_key: [7; 32],
            supervisor_url: Some("http://127.0.0.1:47000".into()),
        };
        assert_eq!(
            capability_for("inst", Some(&protection)),
            json!({
                "protocol": "amage.verified-deploy/1",
                "supported": true,
                "required": true,
                "target": {"instanceName": "inst"},
                "verifierKeyId": Sha256::hash(&[7; 32]).as_hex(),
                "supervisorUrl": "http://127.0.0.1:47000",
            })
        );
        let unannounced = Protection {
            supervisor_url: None,
            ..protection
        };
        assert!(capability_for("inst", Some(&unannounced))
            .get("supervisorUrl")
            .is_none());
    }

    const NOW: SystemTime = SystemTime::UNIX_EPOCH;
    const INSTANCE: &str = "c7smoke";

    fn request(functions: &str, environment: &str) -> Value {
        json!({
            "adminKey": "c7smoke|redacted",
            "functions": functions,
            "appDefinition": {
                "definition": null,
                "dependencies": [],
                "schema": null,
                "changedModules": [{
                    "path": "a.js",
                    "source": "export const x = 1;",
                    "sourceMap": null,
                    "environment": environment,
                }],
                "unchangedModuleHashes": [],
                "udfServerVersion": "1.44.0",
            },
            "componentDefinitions": [],
            "nodeDependencies": [],
        })
    }

    fn receipt(
        signer: &Ed25519Signer,
        request: &Value,
        instance: &str,
        issued_at: &str,
        expires_at: &str,
    ) -> Value {
        let (signed_request, _) = split_request(request.clone()).unwrap();
        let mut envelope = json!({
            "format": 1,
            "protocol": PROTOCOL,
            "target": {"instanceName": instance, "project": "p", "deployment": "d"},
            "requestSha256": digest(&signed_request).as_hex(),
            "policy": {"id": "c7", "version": 3, "sha256": "ab"},
            "candidateId": "cand-1",
            "receiptSha256": "cd",
            "issuedAt": issued_at,
            "expiresAt": expires_at,
        });
        let signature = signer.sign(&canonical_json_bytes(&envelope));
        envelope["signature"] = json!(base64::encode(signature));
        envelope
    }

    fn check(raw: Value, public_key: &[u8; 32]) -> anyhow::Result<()> {
        let req: StartPushRequest = serde_json::from_value(raw.clone())?;
        check_start(raw, &req, INSTANCE, public_key, NOW)
    }

    fn with_receipt(mut raw: Value, envelope: Value) -> Value {
        raw["amageVerified"] = envelope;
        raw
    }

    #[track_caller]
    fn assert_rejected(result: anyhow::Result<()>, code: &str, needle: &str) {
        let error = result.expect_err("gate must refuse");
        assert_eq!(error.short_msg(), code, "{error:#}");
        assert!(error.msg().contains(needle), "{error:#}");
        assert!(error.is_bad_request(), "{error:#}");
    }

    #[test]
    fn valid_receipt_is_accepted() -> anyhow::Result<()> {
        let signer = Ed25519Signer::generate();
        let raw = request("convex/", "isolate");
        let envelope = receipt(
            &signer,
            &raw,
            INSTANCE,
            "1970-01-01T00:00:00Z",
            "1970-01-01T00:15:00Z",
        );
        check(with_receipt(raw, envelope), &signer.public_key())
    }

    #[test]
    fn missing_receipt_is_required_error() {
        assert_rejected(
            check(request("convex/", "isolate"), &[0; 32]),
            "VerifiedDeployRequired",
            "AMAGE (C7) Convex CLI",
        );
    }

    #[test]
    fn forged_or_tampered_receipts_are_rejected() {
        let signer = Ed25519Signer::generate();
        let raw = request("convex/", "isolate");
        let good = receipt(
            &signer,
            &raw,
            INSTANCE,
            "1970-01-01T00:00:00Z",
            "1970-01-01T00:15:00Z",
        );
        let pk = signer.public_key();

        assert_rejected(
            check(
                with_receipt(raw.clone(), good.clone()),
                &Ed25519Signer::generate().public_key(),
            ),
            "VerifiedDeployRejected",
            "receipt signature invalid",
        );
        let mut tampered_envelope = good.clone();
        tampered_envelope["candidateId"] = json!("cand-2");
        assert_rejected(
            check(with_receipt(raw.clone(), tampered_envelope), &pk),
            "VerifiedDeployRejected",
            "receipt signature invalid",
        );
        let mut bad_base64 = good.clone();
        bad_base64["signature"] = json!("not base64!");
        assert_rejected(
            check(with_receipt(raw.clone(), bad_base64), &pk),
            "VerifiedDeployRejected",
            "not valid base64",
        );
        let mut tampered_request = raw.clone();
        tampered_request["functions"] = json!("evil/");
        assert_rejected(
            check(with_receipt(tampered_request, good.clone()), &pk),
            "VerifiedDeployRejected",
            "request digest does not match receipt",
        );
        let other_instance = receipt(
            &signer,
            &raw,
            "other-instance",
            "1970-01-01T00:00:00Z",
            "1970-01-01T00:15:00Z",
        );
        assert_rejected(
            check(with_receipt(raw.clone(), other_instance), &pk),
            "VerifiedDeployRejected",
            "receipt targets instance other-instance, this is c7smoke",
        );
        assert_rejected(
            check(with_receipt(raw.clone(), json!("receipt")), &pk),
            "VerifiedDeployRejected",
            "receipt envelope is malformed",
        );
        let mut wrong_protocol = good.clone();
        wrong_protocol["protocol"] = json!("amage.verified-deploy/2");
        assert_rejected(
            check(with_receipt(raw.clone(), wrong_protocol), &pk),
            "VerifiedDeployRejected",
            "is not amage.verified-deploy/1",
        );
        let mut wrong_format = good;
        wrong_format["format"] = json!(2);
        assert_rejected(
            check(with_receipt(raw, wrong_format), &pk),
            "VerifiedDeployRejected",
            "receipt format 2 is not supported",
        );
    }

    #[test]
    fn receipt_time_window_is_enforced() {
        let signer = Ed25519Signer::generate();
        let raw = request("convex/", "isolate");
        let pk = signer.public_key();
        let case = |issued: &str, expires: &str| {
            check(
                with_receipt(
                    raw.clone(),
                    receipt(&signer, &raw, INSTANCE, issued, expires),
                ),
                &pk,
            )
        };
        // Skew tolerance: issued up to 5 minutes ahead is fine.
        case("1970-01-01T00:05:00Z", "1970-01-01T00:20:00Z").unwrap();
        // Expiring exactly now is still valid; a full 24 h window is the ceiling.
        case("1969-12-31T23:50:00Z", "1970-01-01T00:00:00Z").unwrap();
        case("1969-12-31T00:00:00Z", "1970-01-01T00:00:00Z").unwrap();
        assert_rejected(
            case("1970-01-01T00:05:01Z", "1970-01-01T00:20:00Z"),
            "VerifiedDeployRejected",
            "receipt issued at 1970-01-01T00:05:01Z is more than 5 minutes in the future",
        );
        assert_rejected(
            case("1969-12-31T23:40:00Z", "1969-12-31T23:59:59Z"),
            "VerifiedDeployRejected",
            "receipt expired at 1969-12-31T23:59:59Z",
        );
        assert_rejected(
            case("1969-12-30T23:59:59Z", "1970-01-01T00:00:00Z"),
            "VerifiedDeployRejected",
            "exceeds 24 hours",
        );
        assert_rejected(
            case("1970-01-01T00:04:00Z", "1970-01-01T00:03:00Z"),
            "VerifiedDeployRejected",
            "before it was issued",
        );
        assert_rejected(
            case("1970-01-01 00:00:00", "1970-01-01T00:15:00Z"),
            "VerifiedDeployRejected",
            "receipt issuedAt \"1970-01-01 00:00:00\" is not an RFC 3339 timestamp",
        );
    }

    #[test]
    fn structural_refusals_still_apply_to_signed_requests() {
        let signer = Ed25519Signer::generate();
        let pk = signer.public_key();
        let signed = |raw: Value| {
            let envelope = receipt(
                &signer,
                &raw,
                INSTANCE,
                "1970-01-01T00:00:00Z",
                "1970-01-01T00:15:00Z",
            );
            check(with_receipt(raw, envelope), &pk)
        };
        assert_rejected(
            signed(request("convex/", "node")),
            "VerifiedDeployRejected",
            "isolate environment",
        );
        let mut dry_run = request("convex/", "isolate");
        dry_run["dryRun"] = json!(true);
        assert_rejected(
            signed(dry_run),
            "VerifiedDeployRejected",
            "dry runs are not allowed",
        );
        let mut components = request("convex/", "isolate");
        components["componentDefinitions"] = json!([{
            "definitionPath": "x",
            "definition": {"path": "x.js", "source": "", "sourceMap": null, "environment": "isolate"},
            "dependencies": [],
            "schema": null,
            "functions": [],
            "udfServerVersion": "1.44.0",
        }]);
        assert_rejected(
            signed(components),
            "VerifiedDeployRejected",
            "components are not allowed",
        );
        let mut node = request("convex/", "isolate");
        node["nodeVersion"] = json!("22");
        assert_rejected(
            signed(node),
            "VerifiedDeployRejected",
            "Node.js actions are not allowed",
        );
        let mut incremental = request("convex/", "isolate");
        incremental["appDefinition"]["unchangedModuleHashes"] =
            json!([{"path": "b.js", "environment": "isolate", "sha256": "00"}]);
        assert_rejected(
            signed(incremental),
            "VerifiedDeployRejected",
            "incremental pushes are not allowed",
        );
    }

    #[test]
    fn finish_requires_the_exact_pending_start_and_consumes_it() -> anyhow::Result<()> {
        let mut pending = None;
        assert_rejected(
            consume(&mut pending, &json!({"app": 1})),
            "VerifiedDeployRejected",
            "no verified start_push pending",
        );
        pending = Some(response_digest(&json!({"app": {"n": 1.0}, "z": []}))?);
        assert_rejected(
            consume(&mut pending.clone(), &json!({"app": {"n": 2}, "z": []})),
            "VerifiedDeployRejected",
            "does not match the verified start_push response",
        );
        // What Node's JSON.parse/JSON.stringify sends back still matches.
        consume(&mut pending, &json!({"z": [], "app": {"n": 1}}))?;
        assert_rejected(
            consume(&mut pending, &json!({"z": [], "app": {"n": 1}})),
            "VerifiedDeployRejected",
            "no verified start_push pending",
        );
        Ok(())
    }
}

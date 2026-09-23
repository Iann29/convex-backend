//! AMAGE verified-deploy gate (`amage.verified-deploy/2`).
//!
//! With `AMAGE_VERIFIED_DEPLOY_PUBKEY` set the deployment is *protected*: the
//! supervisor's signed install receipt, sent as a token in the `adminKey` of
//! `start_push` / `wait_for_schema` / `finish_push`, is the only deploy
//! credential. The token becomes an admin identity limited to
//! `DeploymentOp::Deploy` (§5), every push is checked online with the
//! supervisor before it starts and before it commits (§8), and the audit log
//! keeps a provenance line per installation (§7) that `GET
//! /api/amage/installations` reads back (§9). Without the variable the backend
//! behaves like upstream Convex. Private keys never live here.
//! Contract: `bendvex_project/docs/c7-hardening-contract.md`.
use std::{
    borrow::Cow,
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        Mutex,
        MutexGuard,
        OnceLock,
        PoisonError,
    },
    time::{
        Duration,
        SystemTime,
        UNIX_EPOCH,
    },
};

use anyhow::Context;
use application::deploy_config::StartPushRequest;
use authentication::access_token_auth::{
    AccessTokenAuth,
    NullAccessTokenAuth,
};
use axum::http::HeaderMap;
use common::{
    http::{
        extract::{
            MtState,
            Path,
        },
        HttpResponseError,
    },
    query::{
        IndexRange,
        IndexRangeExpression,
        Order,
        Query,
    },
    sha256::{
        Sha256,
        Sha256Digest,
    },
    types::MemberId,
};
use database::{
    query::{
        PaginationOptions,
        TableFilter,
    },
    ResolvedQuery,
};
use errors::ErrorMetadata;
use keybroker::{
    verified_deploy::{
        parse_rfc3339,
        verify_ed25519,
    },
    AdminIdentity,
    AdminIdentityPrincipal,
    DeploymentOp,
    Identity,
};
use model::deployment_audit_log::{
    ACTION_FIELD,
    AUDIT_LOG_INDEX_BY_ACTION,
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::{
    json,
    Value,
};
use value::{
    ConvexValue,
    TableNamespace,
};

use crate::{
    admin::must_be_admin,
    LocalAppState,
};

pub const PROTOCOL: &str = "amage.verified-deploy/2";
const FORMAT: u64 = 2;
const TOKEN_PREFIX: &str = "amage-receipt/2.";
/// Any `amage-receipt/…` string is treated as a token (and judged by §4);
/// everything else is a foreign credential (`VerifiedDeployRequired`).
const TOKEN_FAMILY: &str = "amage-receipt/";
const PUSH_ACTION: &str = "push_config_with_components";
const PUBKEY_ENV: &str = "AMAGE_VERIFIED_DEPLOY_PUBKEY";
const SUPERVISOR_URL_ENV: &str = "AMAGE_VERIFIED_DEPLOY_SUPERVISOR_URL";
/// Tolerated clock skew for `issuedAt` and `checkedAt`.
const MAX_CLOCK_SKEW: Duration = Duration::from_secs(5 * 60);
/// Longest receipt lifetime the backend accepts.
const MAX_LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_TOKEN_BYTES: usize = 8192;
const MAX_TRUST_ENTRIES: usize = 8;
const MAX_INSTALLATIONS: usize = 1000;
const INSTALLATION_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const ONLINE_TIMEOUT: Duration = Duration::from_secs(5);
const AUDIT_LOG_LIMIT: usize = 100;
/// Pushes a replay check reads back to a receipt's `issuedAt` before failing
/// closed; receipts live at most `MAX_LIFETIME`.
const FINISH_SCAN_LIMIT: usize = 10_000;

// --- configuration ----------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
struct TrustedKey {
    kid: String,
    public_key: [u8; 32],
    /// `Some` for a key in retirement: accepted only while `now < notAfter`.
    not_after: Option<(String, SystemTime)>,
}

/// The keys the backend trusts, in `AMAGE_VERIFIED_DEPLOY_PUBKEY` order.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Trust(Vec<TrustedKey>);

impl Trust {
    /// `<hex64>[@<RFC 3339>]` entries separated by commas; a single `<hex64>`
    /// (the v1 format) is still valid.
    fn parse(text: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(!text.is_empty(), "{PUBKEY_ENV} is empty");
        let entries: Vec<&str> = text.split(',').collect();
        anyhow::ensure!(
            entries.len() <= MAX_TRUST_ENTRIES,
            "{PUBKEY_ENV} has more than {MAX_TRUST_ENTRIES} keys"
        );
        let mut keys: Vec<TrustedKey> = Vec::with_capacity(entries.len());
        for entry in entries {
            let (hex, not_after) = match entry.split_once('@') {
                Some((hex, timestamp)) => (hex, Some(timestamp)),
                None => (entry, None),
            };
            let public_key = decode_public_key(hex)?;
            let kid = Sha256::hash(&public_key).as_hex();
            anyhow::ensure!(
                keys.iter().all(|key| key.kid != kid),
                "{PUBKEY_ENV} lists the same key twice (kid {kid})"
            );
            let not_after = not_after
                .map(|timestamp| {
                    parse_rfc3339(timestamp)
                        .map(|at| (timestamp.to_string(), at))
                        .with_context(|| {
                            format!(
                                "{PUBKEY_ENV}: notAfter {timestamp:?} is not an RFC 3339 timestamp"
                            )
                        })
                })
                .transpose()?;
            keys.push(TrustedKey {
                kid,
                public_key,
                not_after,
            });
        }
        Ok(Self(keys))
    }

    fn key(&self, kid: &str) -> Option<&TrustedKey> {
        self.0.iter().find(|key| key.kid == kid)
    }

    fn capability_keys(&self) -> Value {
        Value::Array(
            self.0
                .iter()
                .map(|key| {
                    let mut entry = json!({"kid": key.kid});
                    if let Some((not_after, _)) = &key.not_after {
                        entry["notAfter"] = json!(not_after);
                    }
                    entry
                })
                .collect(),
        )
    }
}

fn decode_public_key(hex: &str) -> anyhow::Result<[u8; 32]> {
    anyhow::ensure!(
        is_hex(hex, 64),
        "{PUBKEY_ENV} entry must be 64 lowercase hex digits (a raw 32-byte Ed25519 public key)"
    );
    let mut key = [0u8; 32];
    for (byte, pair) in key.iter_mut().zip(hex.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(key)
}

fn is_hex(text: &str, len: usize) -> bool {
    text.len() == len && text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Config {
    trust: Trust,
    supervisor_url: String,
}

impl Config {
    fn from_env() -> anyhow::Result<Option<Self>> {
        let text = match std::env::var(PUBKEY_ENV) {
            Ok(text) => text,
            Err(std::env::VarError::NotPresent) => return Ok(None),
            Err(error) => return Err(error).context(PUBKEY_ENV),
        };
        let trust = Trust::parse(&text)?;
        let supervisor_url = match std::env::var(SUPERVISOR_URL_ENV) {
            Ok(url) if !url.is_empty() => url,
            Ok(_) | Err(std::env::VarError::NotPresent) => anyhow::bail!(
                "{SUPERVISOR_URL_ENV} is required when {PUBKEY_ENV} is set (the supervisor is \
                 consulted online on every protected deploy)"
            ),
            Err(error) => return Err(error).context(SUPERVISOR_URL_ENV),
        };
        let parsed = url::Url::parse(&supervisor_url)
            .ok()
            .filter(|url| matches!(url.scheme(), "http" | "https") && url.host().is_some());
        anyhow::ensure!(
            parsed.is_some(),
            "{SUPERVISOR_URL_ENV} must be an http(s) origin, got {supervisor_url:?}"
        );
        Ok(Some(Self {
            trust,
            supervisor_url: supervisor_url.trim_end_matches('/').to_string(),
        }))
    }
}

struct Protection {
    config: Config,
    client: reqwest::Client,
}

impl Protection {
    fn new(config: Config) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(ONLINE_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("verified-deploy HTTP client")?;
        Ok(Self { config, client })
    }
}

static PROTECTION: OnceLock<Option<Protection>> = OnceLock::new();

pub fn initialize() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let existing = match PROTECTION.get() {
        Some(existing) => existing,
        None => {
            let protection = config.clone().map(Protection::new).transpose()?;
            PROTECTION.get_or_init(|| protection)
        },
    };
    anyhow::ensure!(
        existing.as_ref().map(|p| &p.config) == config.as_ref(),
        "verified-deploy configuration cannot change in-process"
    );
    Ok(())
}

fn protection() -> anyhow::Result<Option<&'static Protection>> {
    Ok(PROTECTION
        .get()
        .context("verified-deploy gate not initialized")?
        .as_ref())
}

// --- errors -----------------------------------------------------------------

fn required() -> anyhow::Error {
    ErrorMetadata::bad_request(
        "VerifiedDeployRequired",
        format!(
            "This deployment only accepts verified deploys ({PROTOCOL}): the deployment admin key \
             does not install code here; only a supervisor receipt does. Run `npx convex dev` or \
             `npx convex deploy` with the AMAGE (C7) Convex CLI, which sends the supervisor's \
             receipt token as the deploy credential."
        ),
    )
    .into()
}

fn rejected(msg: impl Into<Cow<'static, str>>) -> anyhow::Error {
    ErrorMetadata::bad_request("VerifiedDeployRejected", msg).into()
}

fn unauthorized(msg: impl Into<Cow<'static, str>>) -> anyhow::Error {
    ErrorMetadata::unauthenticated("VerifiedDeployUnauthorized", msg).into()
}

fn bad_admin_key() -> anyhow::Error {
    ErrorMetadata::unauthenticated(
        "BadAdminKey",
        "The provided admin key was invalid for this instance",
    )
    .into()
}

pub fn reject_legacy() -> anyhow::Result<()> {
    if protection()?.is_some() {
        return Err(rejected(
            "legacy push path is disabled on protected deployments",
        ));
    }
    Ok(())
}

// --- capability (§2) --------------------------------------------------------

/// `GET /api/amage/verified_deploy`: the target speaks for itself.
pub async fn capability_handler(
    MtState(st): MtState<LocalAppState>,
) -> Result<axum::Json<Value>, HttpResponseError> {
    Ok(axum::Json(capability_for(
        &st.instance_name,
        protection()?.map(|p| &p.config),
    )))
}

fn capability_for(instance_name: &str, config: Option<&Config>) -> Value {
    let mut capability = json!({
        "protocol": PROTOCOL,
        "supported": true,
        "required": config.is_some(),
        "target": {"instanceName": instance_name},
    });
    if let Some(config) = config {
        capability["verifierKeys"] = config.trust.capability_keys();
        capability["supervisorUrl"] = json!(config.supervisor_url);
    }
    capability
}

// --- canonical JSON ---------------------------------------------------------

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
        Value::Object(map) => write_object(map.iter(), out),
    }
}

fn write_object<'a>(entries: impl Iterator<Item = (&'a String, &'a Value)>, out: &mut Vec<u8>) {
    let mut entries: Vec<_> = entries.collect();
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

/// sha256(canonical(start_push body − root `adminKey`)): the artifact identity
/// the supervisor sealed. `adminKey` is the only excluded field (§3.1).
fn request_digest(raw: &Value) -> Sha256Digest {
    let mut out = Vec::new();
    match raw.as_object() {
        Some(map) => write_object(
            map.iter().filter(|(key, _)| key.as_str() != "adminKey"),
            &mut out,
        ),
        None => write_canonical(raw, &mut out),
    }
    Sha256::hash(&out)
}

// --- tokens and signed objects (§3, §4) -------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Install,
    Observe,
    Attestation,
}

impl Kind {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "install" => Some(Self::Install),
            "observe" => Some(Self::Observe),
            "attestation" => Some(Self::Attestation),
            _ => None,
        }
    }

    /// The closed field set of each kind, sorted by bytes.
    fn fields(self) -> &'static [&'static str] {
        match self {
            Self::Install => &[
                "candidateId",
                "expiresAt",
                "format",
                "installationId",
                "issuedAt",
                "kid",
                "kind",
                "policy",
                "protocol",
                "receiptSha256",
                "requestSha256",
                "signature",
                "target",
            ],
            Self::Observe => &[
                "expiresAt",
                "format",
                "issuedAt",
                "kid",
                "kind",
                "nonce",
                "protocol",
                "signature",
                "target",
            ],
            Self::Attestation => &[
                "candidateId",
                "checkedAt",
                "format",
                "installationId",
                "kid",
                "kind",
                "nonce",
                "phase",
                "policy",
                "protocol",
                "reason",
                "receiptSha256",
                "requestSha256",
                "signature",
                "status",
                "target",
            ],
        }
    }
}

/// Strict inverse of the supervisor's `encode_token`: exact prefix, base64url
/// alphabet only, ≤ 8192 bytes, decoded bytes equal to
/// `canonical(parse(bytes))`.
fn decode_token(token: &str) -> Result<Value, String> {
    let body = match token.strip_prefix(TOKEN_PREFIX) {
        Some(body) if token.len() <= MAX_TOKEN_BYTES && token.is_ascii() => body,
        _ => return Err("receipt token is malformed: prefix or size".into()),
    };
    if body.is_empty()
        || !body
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err("receipt token is malformed: not base64url".into());
    }
    let raw = base64::decode_config(body, base64::URL_SAFE_NO_PAD)
        .map_err(|error| format!("receipt token is malformed: {error}"))?;
    let object: Value = serde_json::from_slice(&raw)
        .map_err(|error| format!("receipt token is malformed: {error}"))?;
    if !object.is_object() || canonical_json_bytes(&object) != raw {
        return Err("receipt token is malformed: not canonical".into());
    }
    Ok(object)
}

fn plain(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// §4, in the reference implementation's order: shape, protocol, format,
/// kind, field set, kid (and retirement), target, window / freshness,
/// signature. Returns the object's kind.
fn verify_object(
    trust: &Trust,
    object: &Value,
    allowed: &[Kind],
    instance_name: &str,
    now: SystemTime,
) -> Result<Kind, String> {
    let (Some(fields), Some(kind_name)) = (
        object.as_object(),
        object.get("kind").and_then(Value::as_str),
    ) else {
        return Err("receipt token is malformed: not a signed object".into());
    };
    let protocol = fields.get("protocol").unwrap_or(&Value::Null);
    if protocol.as_str() != Some(PROTOCOL) {
        return Err(format!("receipt protocol {protocol} is not {PROTOCOL}"));
    }
    let format = fields.get("format").unwrap_or(&Value::Null);
    if format.as_u64() != Some(FORMAT) {
        return Err(format!(
            "receipt format {format} is not supported (expected {FORMAT})"
        ));
    }
    let kind = match Kind::parse(kind_name) {
        Some(kind) if allowed.contains(&kind) => kind,
        _ => return Err(format!("receipt kind {kind_name:?} cannot be used here")),
    };
    let mut names: Vec<&str> = fields.keys().map(String::as_str).collect();
    names.sort_unstable();
    if names != kind.fields() {
        return Err("receipt fields differ from the protocol".into());
    }
    let kid = plain(&fields["kid"]);
    let key = trust
        .key(&kid)
        .ok_or_else(|| format!("unknown verifier key {kid}"))?;
    if let Some((not_after, at)) = &key.not_after
        && now >= *at
    {
        return Err(format!("verifier key {kid} retired at {not_after}"));
    }
    let target = fields["target"]
        .get("instanceName")
        .and_then(Value::as_str)
        .ok_or_else(|| "receipt target lacks instanceName".to_string())?;
    if target != instance_name {
        return Err(format!(
            "receipt targets instance {target}, this is {instance_name}"
        ));
    }
    if kind == Kind::Attestation {
        let checked = parse_timestamp("checkedAt", &fields["checkedAt"])?;
        let skew = checked
            .duration_since(now)
            .or_else(|_| now.duration_since(checked))
            .unwrap_or(Duration::ZERO);
        if skew > MAX_CLOCK_SKEW {
            return Err(format!(
                "attestation checkedAt {} is more than 5 minutes from now",
                plain(&fields["checkedAt"])
            ));
        }
    } else {
        check_window(&fields["issuedAt"], &fields["expiresAt"], now)?;
    }
    let signature = fields["signature"]
        .as_str()
        .and_then(|text| base64::decode(text).ok())
        .ok_or_else(|| "receipt signature is not valid base64".to_string())?;
    let mut unsigned = fields.clone();
    unsigned.remove("signature");
    if !verify_ed25519(
        &key.public_key,
        &canonical_json_bytes(&Value::Object(unsigned)),
        &signature,
    ) {
        return Err("receipt signature invalid".into());
    }
    Ok(kind)
}

fn parse_timestamp(field: &str, value: &Value) -> Result<SystemTime, String> {
    value
        .as_str()
        .and_then(parse_rfc3339)
        .ok_or_else(|| format!("receipt {field} {value} is not an RFC 3339 timestamp"))
}

fn check_window(issued_at: &Value, expires_at: &Value, now: SystemTime) -> Result<(), String> {
    let issued = parse_timestamp("issuedAt", issued_at)?;
    let expires = parse_timestamp("expiresAt", expires_at)?;
    let (issued_at, expires_at) = (plain(issued_at), plain(expires_at));
    if issued > now + MAX_CLOCK_SKEW {
        return Err(format!(
            "receipt issued at {issued_at} is more than 5 minutes in the future"
        ));
    }
    if expires < now {
        return Err(format!("receipt expired at {expires_at}"));
    }
    let lifetime = expires.duration_since(issued).map_err(|_| {
        format!("receipt expires at {expires_at}, before it was issued at {issued_at}")
    })?;
    if lifetime > MAX_LIFETIME {
        return Err(format!(
            "receipt validity window from {issued_at} to {expires_at} exceeds 24 hours"
        ));
    }
    Ok(())
}

fn verify_token(
    trust: &Trust,
    token: &str,
    allowed: &[Kind],
    instance_name: &str,
    now: SystemTime,
) -> Result<(Kind, Value), String> {
    let object = decode_token(token)?;
    let kind = verify_object(trust, &object, allowed, instance_name, now)?;
    Ok((kind, object))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Policy {
    id: String,
    version: u64,
    sha256: String,
}

/// What an install receipt binds: one installation of one artifact under one
/// policy, and the key that signed it. Also the content of a provenance line.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Binding {
    installation_id: String,
    candidate_id: String,
    request_sha256: String,
    receipt_sha256: String,
    policy: Policy,
    kid: String,
}

impl Binding {
    /// Typed view of a verified `install` receipt or attestation; the values
    /// must have the protocol's shapes (hex ids, `policy` = {id, version,
    /// sha256}) so the provenance line stays parseable.
    fn parse(object: &Value) -> Result<Self, String> {
        let hex = |field: &str, len: usize| {
            object[field]
                .as_str()
                .filter(|text| is_hex(text, len))
                .map(str::to_string)
        };
        let policy = object["policy"]
            .as_object()
            .filter(|policy| policy.len() == 3)
            .and_then(|policy| serde_json::from_value::<Policy>(Value::Object(policy.clone())).ok())
            .filter(|policy| {
                !policy.id.is_empty()
                    && policy
                        .id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
                    && is_hex(&policy.sha256, 64)
            });
        match (
            hex("installationId", 32),
            hex("candidateId", 32),
            hex("requestSha256", 64),
            hex("receiptSha256", 64),
            policy,
            hex("kid", 64),
        ) {
            (
                Some(installation_id),
                Some(candidate_id),
                Some(request_sha256),
                Some(receipt_sha256),
                Some(policy),
                Some(kid),
            ) => Ok(Self {
                installation_id,
                candidate_id,
                request_sha256,
                receipt_sha256,
                policy,
                kid,
            }),
            _ => Err("receipt fields differ from the protocol".into()),
        }
    }

    /// §7: the durable, atomic record of an installation in the audit log.
    fn provenance(&self) -> String {
        format!(
            "{PROTOCOL} installation={} candidate={} policy={}@{}:{} request={} receipt={} kid={}",
            self.installation_id,
            self.candidate_id,
            self.policy.id,
            self.policy.version,
            self.policy.sha256,
            self.request_sha256,
            self.receipt_sha256,
            self.kid,
        )
    }

    fn parse_provenance(line: &str) -> Option<Self> {
        let mut words = line.split(' ');
        if words.next() != Some(PROTOCOL) {
            return None;
        }
        let mut field = |name: &str| words.next()?.strip_prefix(name).map(str::to_string);
        let installation_id = field("installation=")?;
        let candidate_id = field("candidate=")?;
        let policy = field("policy=")?;
        let request_sha256 = field("request=")?;
        let receipt_sha256 = field("receipt=")?;
        let kid = field("kid=")?;
        if words.next().is_some() {
            return None;
        }
        let (id, rest) = policy.split_once('@')?;
        let (version, sha256) = rest.split_once(':')?;
        Self::parse(&json!({
            "installationId": installation_id,
            "candidateId": candidate_id,
            "requestSha256": request_sha256,
            "receiptSha256": receipt_sha256,
            "policy": {"id": id, "version": version.parse::<u64>().ok()?, "sha256": sha256},
            "kid": kid,
        }))
        .ok()
    }

    /// §8: the question the backend asks the supervisor.
    fn question(&self, instance_name: &str, phase: &str, nonce: &str) -> Value {
        json!({
            "protocol": PROTOCOL,
            "target": {"instanceName": instance_name},
            "installationId": self.installation_id,
            "candidateId": self.candidate_id,
            "requestSha256": self.request_sha256,
            "receiptSha256": self.receipt_sha256,
            "policy": self.policy,
            "kid": self.kid,
            "phase": phase,
            "nonce": nonce,
        })
    }
}

fn install_receipt(
    trust: &Trust,
    token: &str,
    instance_name: &str,
    now: SystemTime,
) -> Result<Binding, String> {
    install_receipt_issued(trust, token, instance_name, now).map(|(binding, _)| binding)
}

/// A verified `install` receipt and its `issuedAt` (already inside the window
/// `check_window` accepted).
fn install_receipt_issued(
    trust: &Trust,
    token: &str,
    instance_name: &str,
    now: SystemTime,
) -> Result<(Binding, SystemTime), String> {
    let (_, object) = verify_token(trust, token, &[Kind::Install], instance_name, now)?;
    let issued = parse_timestamp("issuedAt", &object["issuedAt"])?;
    Ok((Binding::parse(&object)?, issued))
}

// --- identity (§5) ----------------------------------------------------------

/// `AccessTokenAuth` of a protected deployment: a valid `install` token is an
/// admin identity allowed exactly `DeploymentOp::Deploy`; anything else is the
/// `BadAdminKey` that `NullAccessTokenAuth` answers.
pub struct AmageReceiptAuth {
    instance_name: String,
}

fn receipt_identity(
    trust: &Trust,
    instance_name: &str,
    token: &str,
    now: SystemTime,
) -> anyhow::Result<Identity> {
    if token.starts_with(TOKEN_FAMILY) {
        match install_receipt(trust, token, instance_name, now) {
            Ok(binding) => {
                tracing::debug!(
                    "install receipt {} accepted as the deploy identity",
                    binding.installation_id
                );
                return Ok(Identity::DeploymentAdmin(
                    AdminIdentity::new_for_access_token(
                        instance_name.to_string(),
                        AdminIdentityPrincipal::Member(MemberId(0)),
                        token.to_string(),
                        false,
                        vec![DeploymentOp::Deploy],
                        now,
                        None,
                        None,
                    ),
                ));
            },
            Err(reason) => tracing::debug!("receipt credential refused: {reason}"),
        }
    }
    Err(bad_admin_key())
}

impl AccessTokenAuth for AmageReceiptAuth {
    fn is_authorized<'a, 'b, 'c>(
        &'a self,
        access_token: &'b str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Identity>> + Send + 'c>>
    where
        'a: 'c,
        'b: 'c,
        Self: 'c,
    {
        let identity = match protection() {
            Ok(Some(protection)) => receipt_identity(
                &protection.config.trust,
                &self.instance_name,
                access_token,
                SystemTime::now(),
            ),
            Ok(None) => Err(bad_admin_key()),
            Err(error) => Err(error),
        };
        Box::pin(async move { identity })
    }
}

/// The access-token authority `make_app` wires into `ApplicationAuth`.
pub fn access_token_auth(instance_name: String) -> anyhow::Result<Arc<dyn AccessTokenAuth>> {
    Ok(match protection()? {
        Some(_) => Arc::new(AmageReceiptAuth { instance_name }),
        None => Arc::new(NullAccessTokenAuth),
    })
}

// --- installation table (§6) ------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    /// `start_push` in flight (first attempt or idempotent retry).
    Starting,
    Started,
    /// `finish_push` in flight: finish happens at most once per process.
    Finishing,
    Finished,
}

impl State {
    fn label(self) -> &'static str {
        match self {
            Self::Starting | Self::Started | Self::Finishing => "started",
            Self::Finished => "finished",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
    binding: Binding,
    state: State,
    response_digest: Option<Sha256Digest>,
    started_at: Option<SystemTime>,
    finished_at: Option<SystemTime>,
    starts: u32,
    updated_at: SystemTime,
}

/// In-memory installation table keyed by `installationId`. Durable truth is
/// the audit log (§7); this table gives `start` idempotency and single finish.
#[derive(Default)]
struct Installations(BTreeMap<String, Entry>);

impl Installations {
    fn state(&self, id: &str) -> Option<State> {
        self.0.get(id).map(|entry| entry.state)
    }

    /// Claims `start_push` for `binding`: a fresh entry becomes `Starting`; a
    /// `Started` entry with the same artifact is retried (its previous state
    /// is returned so a failure can restore it).
    fn claim_start(&mut self, binding: &Binding, now: SystemTime) -> Result<Option<Entry>, String> {
        self.prune(now);
        let id = &binding.installation_id;
        let previous = match self.0.get(id) {
            None => None,
            Some(entry) => match entry.state {
                State::Starting => {
                    return Err(format!("installation {id} start already in progress"));
                },
                State::Started if entry.binding.request_sha256 == binding.request_sha256 => {
                    Some(entry.clone())
                },
                State::Started => {
                    return Err(format!(
                        "installation {id} is started for a different request"
                    ));
                },
                State::Finishing => {
                    return Err(format!("installation {id} finish already in progress"));
                },
                State::Finished => return Err(format!("installation {id} already finished")),
            },
        };
        let entry = self.0.entry(id.clone()).or_insert_with(|| Entry {
            binding: binding.clone(),
            state: State::Starting,
            response_digest: None,
            started_at: None,
            finished_at: None,
            starts: 0,
            updated_at: now,
        });
        entry.state = State::Starting;
        entry.updated_at = now;
        Ok(previous)
    }

    fn start_failed(&mut self, id: &str, previous: Option<Entry>) {
        match previous {
            Some(entry) => {
                self.0.insert(id.to_string(), entry);
            },
            None => {
                self.0.remove(id);
            },
        }
    }

    fn started(&mut self, id: &str, response_digest: Sha256Digest, now: SystemTime) -> u32 {
        let Some(entry) = self.0.get_mut(id) else {
            return 0;
        };
        entry.state = State::Started;
        entry.response_digest = Some(response_digest);
        entry.started_at.get_or_insert(now);
        entry.starts += 1;
        entry.updated_at = now;
        entry.starts
    }

    fn require_started(&self, id: &str) -> Result<(), String> {
        match self.state(id) {
            Some(State::Started) => Ok(()),
            _ => Err(format!("no started installation {id} on this backend")),
        }
    }

    /// Claims `finish_push`: the installation must be `Started` and the
    /// payload must be the remembered `start_push` response.
    fn claim_finish(
        &mut self,
        id: &str,
        response_digest: &Sha256Digest,
        now: SystemTime,
    ) -> Result<Binding, String> {
        let entry = match self.0.get_mut(id) {
            Some(entry) if entry.state == State::Started => entry,
            _ => {
                return Err(format!(
                    "no started installation {id} on this backend; run start_push again"
                ));
            },
        };
        if entry.response_digest.as_ref() != Some(response_digest) {
            return Err(
                "finish_push payload does not match the verified start_push response".into(),
            );
        }
        entry.state = State::Finishing;
        entry.updated_at = now;
        Ok(entry.binding.clone())
    }

    fn finish_failed(&mut self, id: &str) {
        if let Some(entry) = self.0.get_mut(id)
            && entry.state == State::Finishing
        {
            entry.state = State::Started;
        }
    }

    fn finished(&mut self, id: &str, now: SystemTime) {
        if let Some(entry) = self.0.get_mut(id) {
            entry.state = State::Finished;
            entry.finished_at = Some(now);
            entry.updated_at = now;
        }
    }

    /// Drops entries older than 24 h, then the oldest settled ones beyond 1000.
    fn prune(&mut self, now: SystemTime) {
        self.0
            .retain(|_, entry| entry.updated_at + INSTALLATION_TTL > now);
        if self.0.len() > MAX_INSTALLATIONS {
            let mut settled: Vec<(SystemTime, String)> = self
                .0
                .iter()
                .filter(|(_, entry)| matches!(entry.state, State::Started | State::Finished))
                .map(|(id, entry)| (entry.updated_at, id.clone()))
                .collect();
            settled.sort();
            for (_, id) in settled.into_iter().take(self.0.len() - MAX_INSTALLATIONS) {
                self.0.remove(&id);
            }
        }
    }
}

static INSTALLATIONS: Mutex<Installations> = Mutex::new(Installations(BTreeMap::new()));

/// The table lock is never held across an `.await`.
fn installations() -> MutexGuard<'static, Installations> {
    INSTALLATIONS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A claimed `start_push`: dropping it before `remember` restores the table.
pub struct StartGuard {
    id: String,
    previous: Option<Entry>,
    done: bool,
}

impl Drop for StartGuard {
    fn drop(&mut self) {
        if !self.done {
            installations().start_failed(&self.id, self.previous.take());
        }
    }
}

/// A claimed `finish_push`: dropping it before `finished` reopens the
/// installation for a retry.
pub struct FinishGuard {
    id: String,
    provenance: String,
    done: bool,
}

impl FinishGuard {
    /// The audit-log message the backend writes instead of the client's (§7).
    pub fn provenance(&self) -> &str {
        &self.provenance
    }

    pub fn finished(mut self) {
        installations().finished(&self.id, SystemTime::now());
        self.done = true;
        tracing::info!("installation {} finished", self.id);
    }
}

impl Drop for FinishGuard {
    fn drop(&mut self) {
        if !self.done {
            installations().finish_failed(&self.id);
        }
    }
}

// --- deploy routes (§6) -----------------------------------------------------

/// The deploy credential of a protected route: a receipt token judged by §4;
/// the admin key (or nothing) is `VerifiedDeployRequired`.
fn install_credential(
    protection: &Protection,
    credential: &str,
    instance_name: &str,
    now: SystemTime,
) -> anyhow::Result<Binding> {
    if !credential.starts_with(TOKEN_FAMILY) {
        return Err(required());
    }
    install_receipt(&protection.config.trust, credential, instance_name, now).map_err(rejected)
}

fn install_credential_issued(
    protection: &Protection,
    credential: &str,
    instance_name: &str,
    now: SystemTime,
) -> anyhow::Result<(Binding, SystemTime)> {
    if !credential.starts_with(TOKEN_FAMILY) {
        return Err(required());
    }
    install_receipt_issued(&protection.config.trust, credential, instance_name, now)
        .map_err(rejected)
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

/// `start_push` gate: token, artifact digest, shape, installation table and
/// the online `start` check. `None` when the deployment is not protected.
pub async fn start(
    st: &LocalAppState,
    raw: &Value,
    req: &StartPushRequest,
) -> anyhow::Result<Option<StartGuard>> {
    let Some(protection) = protection()? else {
        return Ok(None);
    };
    let now = SystemTime::now();
    let (binding, issued_at) =
        install_credential_issued(protection, &req.admin_key, &st.instance_name, now)?;
    anyhow::ensure!(
        binding.request_sha256 == request_digest(raw).as_hex(),
        rejected("request digest does not match receipt")
    );
    check_shape(req)?;
    let id = binding.installation_id.clone();
    // Unknown here (after a restart or a prune): the durable log decides
    // whether this installation already happened. Its finish cannot predate
    // the receipt, so every push since `issuedAt` (less the tolerated skew) is
    // read; a fixed count of recent pushes let a replay through once enough
    // other installations had followed it. The table lock is released before
    // the read transaction.
    let unknown_here = installations().state(&id).is_none();
    if unknown_here
        && finished_since(
            st,
            &id,
            issued_at.checked_sub(MAX_CLOCK_SKEW).unwrap_or(UNIX_EPOCH),
        )
        .await?
    {
        return Err(rejected(format!("installation {id} already finished")));
    }
    let previous = installations()
        .claim_start(&binding, now)
        .map_err(rejected)?;
    let guard = StartGuard {
        id,
        previous,
        done: false,
    };
    online_check(protection, &st.instance_name, &binding, "start").await?;
    Ok(Some(guard))
}

/// Records the upstream `start_push` response the matching `finish_push` must
/// carry; the installation is now `started`.
pub fn remember(gate: &mut Option<StartGuard>, response: &impl Serialize) -> anyhow::Result<()> {
    if let Some(guard) = gate {
        let starts =
            installations().started(&guard.id, response_digest(response)?, SystemTime::now());
        guard.done = true;
        tracing::info!("installation {} started (attempt {starts})", guard.id);
    }
    Ok(())
}

/// `wait_for_schema` gate: a valid token whose installation is `started` here.
pub fn wait_for_schema(st: &LocalAppState, credential: &str) -> anyhow::Result<()> {
    let Some(protection) = protection()? else {
        return Ok(());
    };
    let binding = install_credential(protection, credential, &st.instance_name, SystemTime::now())?;
    installations()
        .require_started(&binding.installation_id)
        .map_err(rejected)
}

/// `finish_push` gate: token, `started` installation, exact `start_push`
/// response, no dry run, online `finish` check. The guard carries the
/// provenance line to write as the push message.
pub async fn finish(
    st: &LocalAppState,
    credential: &str,
    start_push: &impl Serialize,
    dry_run: bool,
) -> anyhow::Result<Option<FinishGuard>> {
    let Some(protection) = protection()? else {
        return Ok(None);
    };
    let now = SystemTime::now();
    let binding = install_credential(protection, credential, &st.instance_name, now)?;
    let id = binding.installation_id.clone();
    installations()
        .claim_finish(&id, &response_digest(start_push)?, now)
        .map_err(rejected)?;
    let guard = FinishGuard {
        id,
        provenance: binding.provenance(),
        done: false,
    };
    anyhow::ensure!(
        !dry_run,
        rejected("dry runs are not allowed on protected deployments")
    );
    online_check(protection, &st.instance_name, &binding, "finish").await?;
    Ok(Some(guard))
}

// --- online check (§8) ------------------------------------------------------

fn random_nonce() -> String {
    // `Secret::random()` is 32 CSPRNG bytes; its Display is their hex.
    keybroker::Secret::random().to_string()[..32].to_string()
}

async fn online_check(
    protection: &Protection,
    instance_name: &str,
    binding: &Binding,
    phase: &str,
) -> anyhow::Result<()> {
    let supervisor = &protection.config.supervisor_url;
    let unreachable = |detail: String| {
        rejected(format!(
            "verifier authority at {supervisor} is unreachable ({detail}); nothing was deployed"
        ))
    };
    let question = binding.question(instance_name, phase, &random_nonce());
    let response = protection
        .client
        .post(format!("{supervisor}/v1/installation_check"))
        .json(&question)
        .send()
        .await
        .map_err(|error| unreachable(error.without_url().to_string()))?;
    let status = response.status();
    if status != reqwest::StatusCode::OK {
        return Err(unreachable(format!("HTTP {status}")));
    }
    let body: Value = response
        .json()
        .await
        .map_err(|error| unreachable(format!("invalid JSON: {}", error.without_url())))?;
    check_attestation(
        &protection.config.trust,
        &question,
        &body,
        instance_name,
        SystemTime::now(),
    )
    .map_err(rejected)
}

/// §8 acceptance of an `installation_check` response for `question`.
fn check_attestation(
    trust: &Trust,
    question: &Value,
    body: &Value,
    instance_name: &str,
    now: SystemTime,
) -> Result<(), String> {
    const STATUSES: [&str; 3] = ["current", "revoked", "unknown"];
    let status = body["status"].as_str().filter(|s| STATUSES.contains(s));
    let (Some(status), Some(attestation)) = (status, body["attestation"].as_object()) else {
        return Err("verifier attestation invalid: response shape".into());
    };
    if body["format"].as_u64() != Some(FORMAT) {
        return Err("verifier attestation invalid: response shape".into());
    }
    let attestation = Value::Object(attestation.clone());
    verify_object(
        trust,
        &attestation,
        &[Kind::Attestation],
        instance_name,
        now,
    )
    .map_err(|reason| format!("verifier attestation invalid: {reason}"))?;
    const ECHOED: [&str; 8] = [
        "target",
        "installationId",
        "candidateId",
        "requestSha256",
        "receiptSha256",
        "policy",
        "phase",
        "nonce",
    ];
    if ECHOED.iter().any(|field| {
        canonical_json_bytes(&attestation[field]) != canonical_json_bytes(&question[field])
    }) || attestation["status"].as_str() != Some(status)
    {
        return Err("verifier attestation invalid: fields differ from the question".into());
    }
    match status {
        "current" => Ok(()),
        "revoked" => Err(format!(
            "installation authorization revoked: {}",
            attestation["reason"].as_str().unwrap_or("unspecified")
        )),
        _ => Err("installation unknown to the verifier authority".into()),
    }
}

// --- durable state and status routes (§7, §9) -------------------------------

struct DurablePush {
    finished_at: SystemTime,
    /// `None` when the push message is not a provenance line (unprotected
    /// push).
    binding: Option<Binding>,
}

/// The last `push_config_with_components` events, newest first.
async fn durable_pushes(st: &LocalAppState) -> anyhow::Result<Vec<DurablePush>> {
    let mut tx = st.application.begin(Identity::system()).await?;
    let query = Query::index_range(IndexRange {
        index_name: AUDIT_LOG_INDEX_BY_ACTION.name(),
        range: vec![IndexRangeExpression::Eq(
            ACTION_FIELD.clone(),
            ConvexValue::try_from(PUSH_ACTION)?.into(),
        )],
        order: Order::Desc,
    });
    let mut stream = ResolvedQuery::new_bounded(
        &mut tx,
        TableNamespace::Global,
        query,
        PaginationOptions::ManualPagination {
            start_cursor: None,
            maximum_rows_read: Some(AUDIT_LOG_LIMIT),
            maximum_bytes_read: None,
        },
        None,
        TableFilter::IncludePrivateSystemTables,
    )?;
    let mut pushes = Vec::new();
    while pushes.len() < AUDIT_LOG_LIMIT
        && let Some(document) = stream.next(&mut tx, None).await?
    {
        let created_ms = f64::from(document.creation_time()).max(0.0) as u64;
        let event: Value = document.into_value().into_value().into();
        pushes.push(DurablePush {
            finished_at: UNIX_EPOCH + Duration::from_millis(created_ms),
            binding: event
                .pointer("/metadata/message")
                .and_then(Value::as_str)
                .and_then(Binding::parse_provenance),
        });
    }
    Ok(pushes)
}

/// Whether `installation_id` finished at or after `since`. Fails closed when
/// the window holds more than `FINISH_SCAN_LIMIT` pushes: nothing is deployed.
async fn finished_since(
    st: &LocalAppState,
    installation_id: &str,
    since: SystemTime,
) -> anyhow::Result<bool> {
    Ok(push_since(st, installation_id, since).await?.is_some())
}

/// The durable push that finished `installation_id` at or after `since`,
/// reading the audit log's pushes newest first until one predates `since`; an
/// error (never a guess) past `FINISH_SCAN_LIMIT` pushes.
async fn push_since(
    st: &LocalAppState,
    installation_id: &str,
    since: SystemTime,
) -> anyhow::Result<Option<DurablePush>> {
    let mut tx = st.application.begin(Identity::system()).await?;
    let query = Query::index_range(IndexRange {
        index_name: AUDIT_LOG_INDEX_BY_ACTION.name(),
        range: vec![IndexRangeExpression::Eq(
            ACTION_FIELD.clone(),
            ConvexValue::try_from(PUSH_ACTION)?.into(),
        )],
        order: Order::Desc,
    });
    let mut stream = ResolvedQuery::new_bounded(
        &mut tx,
        TableNamespace::Global,
        query,
        PaginationOptions::ManualPagination {
            start_cursor: None,
            maximum_rows_read: Some(FINISH_SCAN_LIMIT + 1),
            maximum_bytes_read: None,
        },
        None,
        TableFilter::IncludePrivateSystemTables,
    )?;
    let mut read = 0;
    while let Some(document) = stream.next(&mut tx, None).await? {
        let created_ms = f64::from(document.creation_time()).max(0.0) as u64;
        let finished_at = UNIX_EPOCH + Duration::from_millis(created_ms);
        if finished_at < since {
            return Ok(None);
        }
        read += 1;
        if read > FINISH_SCAN_LIMIT {
            return Err(rejected(format!(
                "more than {FINISH_SCAN_LIMIT} pushes in the window; cannot establish whether \
                 installation {installation_id} already finished"
            )));
        }
        let event: Value = document.into_value().into_value().into();
        let binding = event
            .pointer("/metadata/message")
            .and_then(Value::as_str)
            .and_then(Binding::parse_provenance);
        if binding
            .as_ref()
            .is_some_and(|binding| binding.installation_id == installation_id)
        {
            return Ok(Some(DurablePush {
                finished_at,
                binding,
            }));
        }
    }
    Ok(None)
}

fn format_timestamp(time: SystemTime) -> String {
    let secs = time
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn installation_json(
    id: &str,
    binding: Option<&Binding>,
    state: &str,
    started_at: Option<SystemTime>,
    finished_at: Option<SystemTime>,
    source: Option<&str>,
    active: bool,
) -> Value {
    json!({
        "installationId": id,
        "state": state,
        "candidateId": binding.map(|b| &b.candidate_id),
        "requestSha256": binding.map(|b| &b.request_sha256),
        "receiptSha256": binding.map(|b| &b.receipt_sha256),
        "policy": binding.map(|b| &b.policy),
        "kid": binding.map(|b| &b.kid),
        "startedAt": started_at.map(format_timestamp),
        "finishedAt": finished_at.map(format_timestamp),
        "source": source,
        "active": active,
    })
}

fn entry_json(id: &str, entry: &Entry, active_id: Option<&str>) -> Value {
    installation_json(
        id,
        Some(&entry.binding),
        entry.state.label(),
        entry.started_at,
        entry.finished_at,
        Some("memory"),
        active_id == Some(id),
    )
}

fn durable_json(push: &DurablePush, active: bool) -> Value {
    match &push.binding {
        Some(binding) => installation_json(
            &binding.installation_id,
            Some(binding),
            "finished",
            None,
            Some(push.finished_at),
            Some("audit_log"),
            active,
        ),
        None => json!({
            "installationId": null,
            "verified": false,
            "finishedAt": format_timestamp(push.finished_at),
        }),
    }
}

fn require_protected() -> anyhow::Result<&'static Protection> {
    protection()?.ok_or_else(|| {
        ErrorMetadata::not_found(
            "VerifiedDeployNotEnabled",
            format!("this deployment is not protected by {PROTOCOL}"),
        )
        .into()
    })
}

/// `Authorization: Convex <install token | observe token | admin key>`.
async fn authorize_status(
    st: &LocalAppState,
    protection: &Protection,
    headers: &HeaderMap,
) -> anyhow::Result<()> {
    let credential = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            value
                .get(..7)
                .filter(|scheme| scheme.eq_ignore_ascii_case("convex "))
                .map(|_| value[7..].trim())
        })
        .ok_or_else(|| unauthorized("send `Authorization: Convex <receipt token | admin key>`"))?;
    if credential.starts_with(TOKEN_FAMILY) {
        verify_token(
            &protection.config.trust,
            credential,
            &[Kind::Install, Kind::Observe],
            &st.instance_name,
            SystemTime::now(),
        )
        .map_err(unauthorized)?;
        return Ok(());
    }
    let identity = st
        .application
        .app_auth()
        .check_key(credential.to_string())
        .await
        .map_err(|_| unauthorized("the credential is neither a receipt token nor the admin key"))?;
    must_be_admin(&identity)
        .map_err(|_| unauthorized("the credential is neither a receipt token nor the admin key"))?;
    Ok(())
}

/// `GET /api/amage/installations`: pending and finished installations of this
/// process, plus the installation of the latest durable push.
pub async fn installations_handler(
    MtState(st): MtState<LocalAppState>,
    headers: HeaderMap,
) -> Result<axum::Json<Value>, HttpResponseError> {
    let protection = require_protected()?;
    authorize_status(&st, protection, &headers).await?;
    let pushes = durable_pushes(&st).await?;
    let latest = pushes.first();
    let active_id = latest
        .and_then(|push| push.binding.as_ref())
        .map(|binding| binding.installation_id.as_str());
    let (pending, finished): (Vec<Value>, Vec<Value>) = {
        let table = installations();
        let (pending, finished): (Vec<_>, Vec<_>) = table
            .0
            .iter()
            .partition(|(_, entry)| entry.state != State::Finished);
        let render = |entries: Vec<(&String, &Entry)>| {
            entries
                .into_iter()
                .map(|(id, entry)| entry_json(id, entry, active_id))
                .collect()
        };
        (render(pending), render(finished))
    };
    Ok(axum::Json(json!({
        "protocol": PROTOCOL,
        "target": {"instanceName": st.instance_name},
        "pending": pending,
        "finished": finished,
        "active": latest.map(|push| durable_json(push, true)),
    })))
}

/// `GET /api/amage/installations/{installation_id}`: memory first, then every
/// durable push a still-meaningful receipt could have made (the longest
/// accepted receipt lifetime plus clock skew), else `unknown`.
pub async fn installation_handler(
    MtState(st): MtState<LocalAppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<axum::Json<Value>, HttpResponseError> {
    let protection = require_protected()?;
    authorize_status(&st, protection, &headers).await?;
    let pushes = durable_pushes(&st).await?;
    let active_id = pushes
        .first()
        .and_then(|push| push.binding.as_ref())
        .map(|binding| binding.installation_id.as_str());
    let entry = installations().0.get(&id).cloned();
    let installation = match entry {
        Some(entry) => entry_json(&id, &entry, active_id),
        None => {
            let since = SystemTime::now()
                .checked_sub(MAX_LIFETIME + MAX_CLOCK_SKEW)
                .unwrap_or(UNIX_EPOCH);
            match push_since(&st, &id, since).await? {
                Some(push) => durable_json(&push, active_id == Some(id.as_str())),
                None => installation_json(&id, None, "unknown", None, None, None, false),
            }
        },
    };
    Ok(axum::Json(json!({
        "protocol": PROTOCOL,
        "target": {"instanceName": st.instance_name},
        "installation": installation,
    })))
}

#[cfg(test)]
mod tests {
    use std::{
        io::{
            Read,
            Write,
        },
        net::TcpListener,
    };

    use errors::ErrorMetadataAnyhowExt;
    use keybroker::verified_deploy::Ed25519Signer;
    use roles::RequireDeploymentOp;

    use super::*;

    fn canonical(value: &Value) -> String {
        String::from_utf8(canonical_json_bytes(value)).unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    // --- canonical JSON (unchanged from v1) -----------------------------------

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
    fn request_digest_excludes_only_admin_key() {
        let a = request_digest(&json!({"adminKey": "secret", "functions": "convex/"}));
        let b = request_digest(&json!({"functions": "convex/", "adminKey": "other"}));
        let c = request_digest(&json!({"functions": "convex/"}));
        assert_eq!(a, b);
        assert_eq!(a, c);
        // v1's envelope is an ordinary field now: it changes the digest.
        assert_ne!(
            a,
            request_digest(&json!({"functions": "convex/", "amageVerified": {}}))
        );
        assert_ne!(a, request_digest(&json!({"functions": "other/"})));
        // Nested `adminKey` keys are part of the artifact.
        assert_ne!(
            request_digest(&json!({"x": {"adminKey": 1}})),
            request_digest(&json!({"x": {}}))
        );
    }

    // --- trust set ------------------------------------------------------------

    const PUBLIC: &str = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
    const KID: &str = "21fe31dfa154a261626bf854046fd2271b7bed4b6abe45aa58877ef47f9721b9";

    #[test]
    fn trust_set_parses_v1_single_key() {
        let trust = Trust::parse(PUBLIC).unwrap();
        assert_eq!(trust.0.len(), 1);
        assert_eq!(trust.0[0].kid, KID);
        assert_eq!(trust.0[0].public_key[..2], [0xd7, 0x5a]);
        assert_eq!(trust.0[0].not_after, None);
        assert_eq!(trust.capability_keys(), json!([{"kid": KID}]));
    }

    #[test]
    fn trust_set_parses_retiring_keys_in_order() {
        let other = hex(&Ed25519Signer::generate().public_key());
        let trust = Trust::parse(&format!("{other}@2026-09-21T00:00:00Z,{PUBLIC}")).unwrap();
        assert_eq!(trust.0.len(), 2);
        assert_eq!(
            trust.0[0].not_after,
            Some(("2026-09-21T00:00:00Z".to_string(), at(1_789_948_800)))
        );
        assert_eq!(trust.0[1].kid, KID);
        let keys = trust.capability_keys();
        assert_eq!(keys[0]["notAfter"], json!("2026-09-21T00:00:00Z"));
        assert_eq!(keys[1], json!({"kid": KID}));
        assert!(trust.key(KID).is_some());
        assert!(trust.key("00").is_none());
    }

    #[test]
    fn trust_set_rejects_bad_entries() {
        let error = |text: &str| Trust::parse(text).unwrap_err().to_string();
        assert!(error("").contains("is empty"));
        assert!(error(&PUBLIC.to_uppercase()).contains("64 lowercase hex"));
        assert!(error(&PUBLIC[..62]).contains("64 lowercase hex"));
        assert!(error(&format!("{PUBLIC}00")).contains("64 lowercase hex"));
        assert!(error(&format!("{PUBLIC},{PUBLIC}")).contains("same key twice"));
        assert!(error(&format!("{PUBLIC}@yesterday")).contains("not an RFC 3339 timestamp"));
        assert!(error(&format!("{PUBLIC}@")).contains("not an RFC 3339 timestamp"));
        let nine: Vec<String> = (0..9)
            .map(|_| hex(&Ed25519Signer::generate().public_key()))
            .collect();
        assert!(error(&nine.join(",")).contains("more than 8 keys"));
        assert!(Trust::parse(&nine[..8].join(",")).is_ok());
    }

    // --- golden vector (tests/receipts.py) ------------------------------------

    const NOW: u64 = 1_789_927_200; // 2026-09-20T18:00:00Z
    const VECTOR_TOKEN: &str = "amage-receipt/2.eyJjYW5kaWRhdGVJZCI6IjExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExIiwiZXhwaXJlc0F0IjoiMjAyNi0wOS0yMFQxODoxNTowMFoiLCJmb3JtYXQiOjIsImluc3RhbGxhdGlvbklkIjoiMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAiLCJpc3N1ZWRBdCI6IjIwMjYtMDktMjBUMTg6MDA6MDBaIiwia2lkIjoiMjFmZTMxZGZhMTU0YTI2MTYyNmJmODU0MDQ2ZmQyMjcxYjdiZWQ0YjZhYmU0NWFhNTg4NzdlZjQ3Zjk3MjFiOSIsImtpbmQiOiJpbnN0YWxsIiwicG9saWN5Ijp7ImlkIjoiYzciLCJzaGEyNTYiOiJhYmFiYWJhYmFiYWJhYmFiYWJhYmFiYWJhYmFiYWJhYmFiYWJhYmFiYWJhYmFiYWJhYmFiYWJhYmFiYWJhYmFiIiwidmVyc2lvbiI6M30sInByb3RvY29sIjoiYW1hZ2UudmVyaWZpZWQtZGVwbG95LzIiLCJyZWNlaXB0U2hhMjU2IjoiY2RjZGNkY2RjZGNkY2RjZGNkY2RjZGNkY2RjZGNkY2RjZGNkY2RjZGNkY2RjZGNkY2RjZGNkY2RjZGNkY2RjZCIsInJlcXVlc3RTaGEyNTYiOiIzMTk2MDU4NWY2YjIzMzY5MjM5YTY1MzFjNWI1ZTI0ZGE1ZTU2ZWMxOGEzZjUwMjlhMDE0NjkwODVjYWRhNDkyIiwic2lnbmF0dXJlIjoiZmJ0WGVXT1A3aGJBc2xZYmd2UXZMSUN6UGtQNnlncDgrNWsxcnN1Q2VJNGVZMm1Mak9KQUlUcU9BSkJVV2ViM1NvT0pKQ0YxMk14bWQ5YmxOMHM5QkE9PSIsInRhcmdldCI6eyJkZXBsb3ltZW50IjoiZCIsImluc3RhbmNlTmFtZSI6ImM3c21va2UiLCJwcm9qZWN0IjoicCJ9fQ";
    const VECTOR_CANONICAL: &str = r#"{"candidateId":"11111111111111111111111111111111","expiresAt":"2026-09-20T18:15:00Z","format":2,"installationId":"00000000000000000000000000000000","issuedAt":"2026-09-20T18:00:00Z","kid":"21fe31dfa154a261626bf854046fd2271b7bed4b6abe45aa58877ef47f9721b9","kind":"install","policy":{"id":"c7","sha256":"abababababababababababababababababababababababababababababababab","version":3},"protocol":"amage.verified-deploy/2","receiptSha256":"cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd","requestSha256":"31960585f6b23369239a6531c5b5e24da5e56ec18a3f5029a01469085cada492","target":{"deployment":"d","instanceName":"c7smoke","project":"p"}}"#;
    const VECTOR_SIGNATURE: &str =
        "fbtXeWOP7hbAslYbgvQvLICzPkP6ygp8+5k1rsuCeI4eY2mLjOJAITqOAJBUWeb3SoOJJCF12Mxmd9blN0s9BA==";
    const VECTOR_REQUEST_SHA256: &str =
        "31960585f6b23369239a6531c5b5e24da5e56ec18a3f5029a01469085cada492";
    const VECTOR_PROVENANCE: &str =
        "amage.verified-deploy/2 installation=00000000000000000000000000000000 \
         candidate=11111111111111111111111111111111 \
         policy=c7@3:abababababababababababababababababababababababababababababababab \
         request=31960585f6b23369239a6531c5b5e24da5e56ec18a3f5029a01469085cada492 \
         receipt=cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd \
         kid=21fe31dfa154a261626bf854046fd2271b7bed4b6abe45aa58877ef47f9721b9";

    fn vector_request() -> Value {
        json!({
            "adminKey": "secret", "functions": "convex/", "dryRun": false, "forCodegen": false,
            "appDefinition": {
                "definition": null, "dependencies": [],
                "schema": {"path": "schema.js", "source": "export default 1;", "environment": "isolate"},
                "changedModules": [{"path": "records.js", "source": "export const x = 1;", "environment": "isolate"}],
                "unchangedModuleHashes": [], "udfServerVersion": "1.44.0",
            },
            "componentDefinitions": [], "nodeDependencies": [],
            "bendvex": {"target": {"project": "p", "deployment": "d"}, "version": 1.0},
        })
    }

    #[test]
    fn golden_vector_pins_both_implementations() {
        let trust = Trust::parse(PUBLIC).unwrap();
        assert_eq!(
            request_digest(&vector_request()).as_hex(),
            VECTOR_REQUEST_SHA256
        );
        let object = decode_token(VECTOR_TOKEN).unwrap();
        assert_eq!(object["signature"], json!(VECTOR_SIGNATURE));
        let mut unsigned = object.clone();
        unsigned.as_object_mut().unwrap().remove("signature");
        assert_eq!(canonical(&unsigned), VECTOR_CANONICAL);
        assert_eq!(encode_token(&object), VECTOR_TOKEN);
        assert_eq!(
            verify_object(&trust, &object, &[Kind::Install], "c7smoke", at(NOW)),
            Ok(Kind::Install)
        );
        let binding = Binding::parse(&object).unwrap();
        assert_eq!(binding.provenance(), VECTOR_PROVENANCE);
        assert_eq!(
            Binding::parse_provenance(VECTOR_PROVENANCE),
            Some(binding.clone())
        );
        assert_eq!(binding.kid, KID);
        assert_eq!(binding.request_sha256, VECTOR_REQUEST_SHA256);
        assert_eq!(
            install_receipt(&trust, VECTOR_TOKEN, "c7smoke", at(NOW)),
            Ok(binding)
        );
        assert_eq!(
            install_receipt(&trust, VECTOR_TOKEN, "c7smoke", at(NOW - 301)),
            Err(
                "receipt issued at 2026-09-20T18:00:00Z is more than 5 minutes in the future"
                    .into()
            )
        );
        assert_eq!(
            install_receipt(&trust, VECTOR_TOKEN, "c7smoke", at(NOW + 901)),
            Err("receipt expired at 2026-09-20T18:15:00Z".into())
        );
    }

    // --- signing helpers ------------------------------------------------------

    const INSTANCE: &str = "c7smoke";

    fn kid_of(signer: &Ed25519Signer) -> String {
        Sha256::hash(&signer.public_key()).as_hex()
    }

    fn trust_of(signers: &[&Ed25519Signer]) -> Trust {
        let text: Vec<String> = signers.iter().map(|s| hex(&s.public_key())).collect();
        Trust::parse(&text.join(",")).unwrap()
    }

    fn sign(signer: &Ed25519Signer, mut object: Value) -> Value {
        let signature = signer.sign(&canonical_json_bytes(&object));
        object["signature"] = json!(base64::encode(signature));
        object
    }

    fn encode_token(object: &Value) -> String {
        format!(
            "{TOKEN_PREFIX}{}",
            base64::encode_config(canonical_json_bytes(object), base64::URL_SAFE_NO_PAD)
        )
    }

    fn unsigned_install(signer: &Ed25519Signer, request: &Value, instance: &str) -> Value {
        json!({
            "format": 2, "protocol": PROTOCOL, "kind": "install", "kid": kid_of(signer),
            "target": {"instanceName": instance, "project": "p", "deployment": "d"},
            "installationId": "0".repeat(32), "candidateId": "1".repeat(32),
            "requestSha256": request_digest(request).as_hex(), "receiptSha256": "cd".repeat(32),
            "policy": {"id": "c7", "version": 3, "sha256": "ab".repeat(32)},
            "issuedAt": "2026-09-20T18:00:00Z", "expiresAt": "2026-09-20T18:15:00Z",
        })
    }

    fn install_token(signer: &Ed25519Signer, request: &Value) -> String {
        encode_token(&sign(signer, unsigned_install(signer, request, INSTANCE)))
    }

    fn observe(signer: &Ed25519Signer) -> Value {
        sign(
            signer,
            json!({
                "format": 2, "protocol": PROTOCOL, "kind": "observe", "kid": kid_of(signer),
                "target": {"instanceName": INSTANCE}, "nonce": "f".repeat(32),
                "issuedAt": "2026-09-20T18:00:00Z", "expiresAt": "2026-09-20T18:02:00Z",
            }),
        )
    }

    fn with(mut object: Value, field: &str, value: Value) -> Value {
        object[field] = value;
        object
    }

    fn without(mut object: Value, field: &str) -> Value {
        object.as_object_mut().unwrap().remove(field);
        object
    }

    // --- token decoding -------------------------------------------------------

    #[test]
    fn token_decoding_is_strict() {
        let signer = Ed25519Signer::generate();
        let object = sign(
            &signer,
            unsigned_install(&signer, &vector_request(), INSTANCE),
        );
        let token = encode_token(&object);
        assert_eq!(decode_token(&token), Ok(object.clone()));
        let body = token.strip_prefix(TOKEN_PREFIX).unwrap();
        assert_eq!(
            decode_token(&format!("amage-receipt/1.{body}")),
            Err("receipt token is malformed: prefix or size".into())
        );
        assert_eq!(
            decode_token(&format!("{TOKEN_PREFIX}{}", "A".repeat(9000))),
            Err("receipt token is malformed: prefix or size".into())
        );
        assert_eq!(
            decode_token(&format!("{TOKEN_PREFIX}{body}ç")),
            Err("receipt token is malformed: prefix or size".into())
        );
        assert_eq!(
            decode_token(&format!("{TOKEN_PREFIX}a+b")),
            Err("receipt token is malformed: not base64url".into())
        );
        assert_eq!(
            decode_token(&format!("{TOKEN_PREFIX}{body}=")),
            Err("receipt token is malformed: not base64url".into())
        );
        assert_eq!(
            decode_token(TOKEN_PREFIX),
            Err("receipt token is malformed: not base64url".into())
        );
        let mut padded = canonical_json_bytes(&object);
        padded.push(b' ');
        assert_eq!(
            decode_token(&format!(
                "{TOKEN_PREFIX}{}",
                base64::encode_config(padded, base64::URL_SAFE_NO_PAD)
            )),
            Err("receipt token is malformed: not canonical".into())
        );
        let unsorted = base64::encode_config(
            serde_json::to_vec(&object).unwrap(),
            base64::URL_SAFE_NO_PAD,
        );
        assert_eq!(
            decode_token(&format!("{TOKEN_PREFIX}{unsorted}")),
            Err("receipt token is malformed: not canonical".into())
        );
        let list = base64::encode_config(b"[1]", base64::URL_SAFE_NO_PAD);
        assert_eq!(
            decode_token(&format!("{TOKEN_PREFIX}{list}")),
            Err("receipt token is malformed: not canonical".into())
        );
        let garbage = base64::encode_config(b"{", base64::URL_SAFE_NO_PAD);
        assert!(decode_token(&format!("{TOKEN_PREFIX}{garbage}"))
            .unwrap_err()
            .starts_with("receipt token is malformed: "));
    }

    // --- §4 rejections --------------------------------------------------------

    #[test]
    fn every_signed_object_rejection_has_its_message() {
        let signer = Ed25519Signer::generate();
        let other = Ed25519Signer::generate();
        let trust = trust_of(&[&signer]);
        let request = vector_request();
        let unsigned = unsigned_install(&signer, &request, INSTANCE);
        let good = sign(&signer, unsigned.clone());
        let now = at(NOW);
        let verify =
            |object: &Value| verify_object(&trust, object, &[Kind::Install], INSTANCE, now);
        assert_eq!(verify(&good), Ok(Kind::Install));

        assert_eq!(
            verify(&json!("receipt")),
            Err("receipt token is malformed: not a signed object".into())
        );
        assert_eq!(
            verify(&with(good.clone(), "kind", json!(1))),
            Err("receipt token is malformed: not a signed object".into())
        );
        assert_eq!(
            verify(&with(
                good.clone(),
                "protocol",
                json!("amage.verified-deploy/1")
            )),
            Err(
                "receipt protocol \"amage.verified-deploy/1\" is not amage.verified-deploy/2"
                    .into()
            )
        );
        assert_eq!(
            verify(&without(good.clone(), "protocol")),
            Err("receipt protocol null is not amage.verified-deploy/2".into())
        );
        assert_eq!(
            verify(&with(good.clone(), "format", json!(1))),
            Err("receipt format 1 is not supported (expected 2)".into())
        );
        assert_eq!(
            verify(&with(good.clone(), "format", json!("2"))),
            Err("receipt format \"2\" is not supported (expected 2)".into())
        );
        assert_eq!(
            verify(&with(good.clone(), "kind", json!("observe"))),
            Err("receipt kind \"observe\" cannot be used here".into())
        );
        assert_eq!(
            verify(&with(good.clone(), "kind", json!("grant"))),
            Err("receipt kind \"grant\" cannot be used here".into())
        );
        assert_eq!(
            verify_object(
                &trust,
                &good,
                &[Kind::Observe, Kind::Attestation],
                INSTANCE,
                now
            ),
            Err("receipt kind \"install\" cannot be used here".into())
        );
        assert_eq!(
            verify(&with(good.clone(), "extra", json!(1))),
            Err("receipt fields differ from the protocol".into())
        );
        assert_eq!(
            verify(&without(good.clone(), "policy")),
            Err("receipt fields differ from the protocol".into())
        );
        assert_eq!(
            verify(&with(good.clone(), "nonce", json!("00"))),
            Err("receipt fields differ from the protocol".into())
        );
        let foreign = sign(&other, unsigned_install(&other, &request, INSTANCE));
        assert_eq!(
            verify(&foreign),
            Err(format!("unknown verifier key {}", kid_of(&other)))
        );
        let retiring = Trust::parse(&format!(
            "{}@2026-09-20T18:01:00Z",
            hex(&signer.public_key())
        ))
        .unwrap();
        assert_eq!(
            verify_object(&retiring, &good, &[Kind::Install], INSTANCE, at(NOW + 59)),
            Ok(Kind::Install)
        );
        assert_eq!(
            verify_object(&retiring, &good, &[Kind::Install], INSTANCE, at(NOW + 60)),
            Err(format!(
                "verifier key {} retired at 2026-09-20T18:01:00Z",
                kid_of(&signer)
            ))
        );
        // Both keys trusted: the kid names the wrong key for the signature.
        let both = trust_of(&[&signer, &other]);
        assert_eq!(
            verify_object(
                &both,
                &with(good.clone(), "kid", json!(kid_of(&other))),
                &[Kind::Install],
                INSTANCE,
                now
            ),
            Err("receipt signature invalid".into())
        );
        assert_eq!(
            verify(&with(
                good.clone(),
                "signature",
                foreign["signature"].clone()
            )),
            Err("receipt signature invalid".into())
        );
        let elsewhere = sign(&signer, unsigned_install(&signer, &request, "elsewhere"));
        assert_eq!(
            verify(&elsewhere),
            Err("receipt targets instance elsewhere, this is c7smoke".into())
        );
        assert_eq!(
            verify(&with(good.clone(), "target", json!({"project": "p"}))),
            Err("receipt target lacks instanceName".into())
        );
        assert_eq!(
            verify_object(&trust, &good, &[Kind::Install], INSTANCE, at(NOW - 301)),
            Err(
                "receipt issued at 2026-09-20T18:00:00Z is more than 5 minutes in the future"
                    .into()
            )
        );
        assert_eq!(
            verify_object(&trust, &good, &[Kind::Install], INSTANCE, at(NOW - 300)),
            Ok(Kind::Install)
        );
        assert_eq!(
            verify_object(&trust, &good, &[Kind::Install], INSTANCE, at(NOW + 901)),
            Err("receipt expired at 2026-09-20T18:15:00Z".into())
        );
        assert_eq!(
            verify_object(&trust, &good, &[Kind::Install], INSTANCE, at(NOW + 900)),
            Ok(Kind::Install)
        );
        let inverted = sign(
            &signer,
            with(unsigned.clone(), "expiresAt", json!("2026-09-20T17:59:59Z")),
        );
        assert_eq!(
            verify_object(&trust, &inverted, &[Kind::Install], INSTANCE, at(NOW - 2)),
            Err(
                "receipt expires at 2026-09-20T17:59:59Z, before it was issued at \
                 2026-09-20T18:00:00Z"
                    .into()
            )
        );
        let long = sign(
            &signer,
            with(unsigned.clone(), "expiresAt", json!("2026-09-21T18:00:01Z")),
        );
        assert_eq!(
            verify(&long),
            Err(
                "receipt validity window from 2026-09-20T18:00:00Z to 2026-09-21T18:00:01Z \
                 exceeds 24 hours"
                    .into()
            )
        );
        let day = sign(
            &signer,
            with(unsigned.clone(), "expiresAt", json!("2026-09-21T18:00:00Z")),
        );
        assert_eq!(verify(&day), Ok(Kind::Install));
        let unparsable = sign(
            &signer,
            with(unsigned.clone(), "issuedAt", json!("2026-09-20 18:00:00")),
        );
        assert_eq!(
            verify(&unparsable),
            Err("receipt issuedAt \"2026-09-20 18:00:00\" is not an RFC 3339 timestamp".into())
        );
        assert_eq!(
            verify(&with(good.clone(), "signature", json!("***"))),
            Err("receipt signature is not valid base64".into())
        );
        assert_eq!(
            verify(&with(good.clone(), "signature", json!(7))),
            Err("receipt signature is not valid base64".into())
        );
        assert_eq!(
            verify(&with(good.clone(), "candidateId", json!("2".repeat(32)))),
            Err("receipt signature invalid".into())
        );
        // Binding shapes: a signed object with a non-hex id is not a receipt.
        let odd = sign(
            &signer,
            with(unsigned.clone(), "installationId", json!("cand-1")),
        );
        assert_eq!(verify(&odd), Ok(Kind::Install));
        assert_eq!(
            Binding::parse(&odd),
            Err("receipt fields differ from the protocol".into())
        );
        let odd_policy = sign(
            &signer,
            with(unsigned, "policy", json!({"id": "c7", "version": 1})),
        );
        assert_eq!(
            Binding::parse(&odd_policy),
            Err("receipt fields differ from the protocol".into())
        );
        // A request that changed after sealing.
        let token = install_token(&signer, &request);
        let binding = install_receipt(&trust, &token, INSTANCE, now).unwrap();
        assert_ne!(
            binding.request_sha256,
            request_digest(&with(request.clone(), "functions", json!("x/"))).as_hex()
        );
        assert_eq!(
            binding.request_sha256,
            request_digest(&with(request, "adminKey", json!("other"))).as_hex()
        );
    }

    #[test]
    fn observe_tokens_only_observe() {
        let signer = Ed25519Signer::generate();
        let trust = trust_of(&[&signer]);
        let token = encode_token(&observe(&signer));
        let now = at(NOW);
        assert_eq!(
            verify_token(&trust, &token, &[Kind::Observe], INSTANCE, now).map(|(kind, _)| kind),
            Ok(Kind::Observe)
        );
        assert_eq!(
            verify_token(
                &trust,
                &token,
                &[Kind::Install, Kind::Observe],
                INSTANCE,
                now
            )
            .map(|(kind, _)| kind),
            Ok(Kind::Observe)
        );
        assert_eq!(
            verify_token(&trust, &token, &[Kind::Install], INSTANCE, now).map(|(kind, _)| kind),
            Err("receipt kind \"observe\" cannot be used here".into())
        );
        assert_eq!(
            verify_token(&trust, &token, &[Kind::Observe], INSTANCE, at(NOW + 121))
                .map(|(kind, _)| kind),
            Err("receipt expired at 2026-09-20T18:02:00Z".into())
        );
    }

    // --- capability -----------------------------------------------------------

    #[test]
    fn capability_shapes() {
        assert_eq!(
            capability_for("inst", None),
            json!({
                "protocol": "amage.verified-deploy/2",
                "supported": true,
                "required": false,
                "target": {"instanceName": "inst"},
            })
        );
        let other = hex(&Ed25519Signer::generate().public_key());
        let config = Config {
            trust: Trust::parse(&format!("{PUBLIC},{other}@2026-09-21T00:00:00Z")).unwrap(),
            supervisor_url: "http://127.0.0.1:47000".into(),
        };
        let capability = capability_for("inst", Some(&config));
        assert_eq!(
            capability,
            json!({
                "protocol": "amage.verified-deploy/2",
                "supported": true,
                "required": true,
                "target": {"instanceName": "inst"},
                "verifierKeys": [
                    {"kid": KID},
                    {"kid": Sha256::hash(&decode_public_key(&other).unwrap()).as_hex(), "notAfter": "2026-09-21T00:00:00Z"},
                ],
                "supervisorUrl": "http://127.0.0.1:47000",
            })
        );
        assert!(capability.get("verifierKeyId").is_none());
    }

    // --- identity (§5) --------------------------------------------------------

    #[test]
    fn install_token_is_a_deploy_only_identity() {
        let signer = Ed25519Signer::generate();
        let trust = trust_of(&[&signer]);
        // `require_operation` checks the identity's age against the wall
        // clock, so this receipt is issued now.
        let now = SystemTime::now();
        let unsigned = with(
            with(
                unsigned_install(&signer, &vector_request(), INSTANCE),
                "issuedAt",
                json!(format_timestamp(now)),
            ),
            "expiresAt",
            json!(format_timestamp(now + Duration::from_secs(900))),
        );
        let token = encode_token(&sign(&signer, unsigned));
        let identity = receipt_identity(&trust, INSTANCE, &token, now).unwrap();
        let Identity::DeploymentAdmin(admin) = &identity else {
            panic!("expected a deployment admin, got {identity:?}");
        };
        assert_eq!(
            admin.principal(),
            &AdminIdentityPrincipal::Member(MemberId(0))
        );
        assert!(!admin.is_read_only());
        assert_eq!(identity.instance_name(), Some(INSTANCE.to_string()));
        identity.require_operation(DeploymentOp::Deploy).unwrap();
        for op in [
            DeploymentOp::ViewData,
            DeploymentOp::WriteData,
            DeploymentOp::RunInternalMutations,
            DeploymentOp::ViewLogs,
            DeploymentOp::ViewEnvironmentVariables,
            DeploymentOp::PauseDeployment,
        ] {
            let error = identity.require_operation(op).unwrap_err();
            assert_eq!(
                error.short_msg(),
                "OperationNotPermitted",
                "{op:?}: {error:#}"
            );
            assert!(error.is_forbidden());
        }
        let bad = |token: &str| {
            let error = receipt_identity(&trust, INSTANCE, token, now).unwrap_err();
            assert_eq!(error.short_msg(), "BadAdminKey", "{error:#}");
            assert!(error.is_unauthenticated());
        };
        bad(&encode_token(&observe(&signer)));
        bad("c7smoke|0123456789abcdef");
        bad("");
        bad(&format!("{TOKEN_PREFIX}garbage"));
        // Expired install tokens are refused too.
        let error =
            receipt_identity(&trust, INSTANCE, &token, now + Duration::from_secs(901)).unwrap_err();
        assert_eq!(error.short_msg(), "BadAdminKey");
    }

    // --- installation table (§6) ----------------------------------------------

    fn binding(id: char, request: &str) -> Binding {
        Binding {
            installation_id: id.to_string().repeat(32),
            candidate_id: "1".repeat(32),
            request_sha256: request.repeat(32),
            receipt_sha256: "cd".repeat(32),
            policy: Policy {
                id: "c7".into(),
                version: 3,
                sha256: "ab".repeat(32),
            },
            kid: "ef".repeat(32),
        }
    }

    #[test]
    fn installation_state_machine() {
        let mut table = Installations::default();
        let (a, b) = (binding('a', "aa"), binding('b', "bb"));
        let (id_a, id_b) = (a.installation_id.clone(), b.installation_id.clone());
        let now = at(NOW);
        let (digest_a, digest_b) = (Sha256::hash(b"a"), Sha256::hash(b"b"));

        // Unknown id at finish / wait.
        assert_eq!(
            table.claim_finish(&id_a, &digest_a, now),
            Err(format!(
                "no started installation {id_a} on this backend; run start_push again"
            ))
        );
        assert_eq!(
            table.require_started(&id_a),
            Err(format!("no started installation {id_a} on this backend"))
        );

        // Two installations start and coexist.
        assert_eq!(table.claim_start(&a, now), Ok(None));
        assert_eq!(table.state(&id_a), Some(State::Starting));
        assert_eq!(
            table.claim_start(&a, now),
            Err(format!("installation {id_a} start already in progress"))
        );
        assert_eq!(
            table.require_started(&id_a),
            Err(format!("no started installation {id_a} on this backend"))
        );
        assert_eq!(table.started(&id_a, digest_a.clone(), now), 1);
        assert_eq!(table.claim_start(&b, now), Ok(None));
        assert_eq!(table.started(&id_b, digest_b.clone(), now), 1);
        assert_eq!(table.require_started(&id_a), Ok(()));
        assert_eq!(table.require_started(&id_b), Ok(()));

        // Idempotent start while started, same artifact; a failed retry restores.
        let previous = table.claim_start(&a, now).unwrap();
        assert_eq!(previous.as_ref().map(|e| e.state), Some(State::Started));
        assert_eq!(table.state(&id_a), Some(State::Starting));
        table.start_failed(&id_a, previous);
        assert_eq!(table.state(&id_a), Some(State::Started));
        assert_eq!(
            table
                .claim_finish(&id_a, &digest_a, now)
                .map(|b| b.installation_id),
            Ok(id_a.clone())
        );
        table.finish_failed(&id_a);
        let previous = table.claim_start(&a, now).unwrap();
        assert!(previous.is_some());
        assert_eq!(table.started(&id_a, Sha256::hash(b"a2"), now), 2);
        assert_eq!(table.0[&id_a].started_at, Some(now));
        // The remembered response is replaced.
        assert_eq!(
            table.claim_finish(&id_a, &digest_a, now),
            Err("finish_push payload does not match the verified start_push response".into())
        );
        // A different artifact under the same installation id is refused.
        assert_eq!(
            table.claim_start(&binding('a', "ff"), now),
            Err(format!(
                "installation {id_a} is started for a different request"
            ))
        );
        // A fresh claim that fails is removed.
        let c = binding('c', "cc");
        assert_eq!(table.claim_start(&c, now), Ok(None));
        table.start_failed(&c.installation_id, None);
        assert_eq!(table.state(&c.installation_id), None);

        // Finish out of order: b first, then a; each exactly once.
        assert_eq!(
            table
                .claim_finish(&id_b, &digest_b, now)
                .map(|b| b.installation_id),
            Ok(id_b.clone())
        );
        assert_eq!(
            table.claim_finish(&id_b, &digest_b, now),
            Err(format!(
                "no started installation {id_b} on this backend; run start_push again"
            ))
        );
        assert_eq!(
            table.claim_start(&b, now),
            Err(format!("installation {id_b} finish already in progress"))
        );
        table.finished(&id_b, at(NOW + 5));
        assert_eq!(table.state(&id_b), Some(State::Finished));
        assert_eq!(table.0[&id_b].finished_at, Some(at(NOW + 5)));
        assert_eq!(
            table.claim_finish(&id_b, &digest_b, now),
            Err(format!(
                "no started installation {id_b} on this backend; run start_push again"
            ))
        );
        assert_eq!(
            table.claim_start(&b, now),
            Err(format!("installation {id_b} already finished"))
        );
        assert_eq!(table.state(&id_a), Some(State::Started));
        assert_eq!(
            table
                .claim_finish(&id_a, &Sha256::hash(b"a2"), now)
                .map(|b| b.installation_id),
            Ok(id_a.clone())
        );
        table.finished(&id_a, now);
        assert_eq!(table.state(&id_a), Some(State::Finished));

        // Entries expire after 24 h; the table is capped at 1000 settled entries.
        table.prune(at(NOW + 24 * 3600 + 6));
        assert!(table.0.is_empty());
        for i in 0..1005u32 {
            let mut entry = binding('d', "dd");
            entry.installation_id = format!("{i:032x}");
            table.claim_start(&entry, at(NOW + u64::from(i))).unwrap();
            table.started(
                &entry.installation_id,
                digest_a.clone(),
                at(NOW + u64::from(i)),
            );
        }
        table.prune(at(NOW + 2000));
        assert_eq!(table.0.len(), MAX_INSTALLATIONS);
        assert!(!table.0.contains_key(&format!("{:032x}", 0)));
        assert!(table.0.contains_key(&format!("{:032x}", 1004)));
    }

    #[test]
    fn guards_restore_the_table_on_failure() {
        let a = binding('e', "ee");
        let id = a.installation_id.clone();
        let now = at(NOW);
        {
            let previous = installations().claim_start(&a, now).unwrap();
            let _guard = StartGuard {
                id: id.clone(),
                previous,
                done: false,
            };
        }
        assert_eq!(installations().state(&id), None);
        let previous = installations().claim_start(&a, now).unwrap();
        let mut gate = Some(StartGuard {
            id: id.clone(),
            previous,
            done: false,
        });
        remember(&mut gate, &json!({"app": 1})).unwrap();
        drop(gate);
        assert_eq!(installations().state(&id), Some(State::Started));
        let digest = response_digest(&json!({"app": 1})).unwrap();
        installations().claim_finish(&id, &digest, now).unwrap();
        {
            let _guard = FinishGuard {
                id: id.clone(),
                provenance: a.provenance(),
                done: false,
            };
        }
        assert_eq!(installations().state(&id), Some(State::Started));
        installations().claim_finish(&id, &digest, now).unwrap();
        FinishGuard {
            id: id.clone(),
            provenance: a.provenance(),
            done: false,
        }
        .finished();
        assert_eq!(installations().state(&id), Some(State::Finished));
        installations().0.remove(&id);
    }

    // --- provenance (§7) and timestamps ---------------------------------------

    #[test]
    fn provenance_line_round_trips() {
        let b = binding('a', "aa");
        let line = b.provenance();
        assert!(line.len() <= 1024);
        assert_eq!(line.split(' ').count(), 7);
        assert_eq!(Binding::parse_provenance(&line), Some(b.clone()));
        assert_eq!(Binding::parse_provenance("deploy from laptop"), None);
        assert_eq!(Binding::parse_provenance(&format!("{line} extra=1")), None);
        assert_eq!(
            Binding::parse_provenance(&line.replace("policy=c7@3:", "policy=c7@x:")),
            None
        );
        assert_eq!(
            Binding::parse_provenance(
                &line.replace("amage.verified-deploy/2", "amage.verified-deploy/1")
            ),
            None
        );
        assert_eq!(Binding::parse_provenance(&line[..line.len() - 1]), None);
        let mut b2 = b;
        b2.policy.id = "policy_1.0-beta".into();
        assert_eq!(Binding::parse_provenance(&b2.provenance()), Some(b2));
    }

    #[test]
    fn timestamps_format_as_rfc3339_utc() {
        for (secs, text) in [
            (0, "1970-01-01T00:00:00Z"),
            (951_782_400, "2000-02-29T00:00:00Z"),
            (1_789_927_200, "2026-09-20T18:00:00Z"),
            (1_789_948_800, "2026-09-21T00:00:00Z"),
            (4_102_444_799, "2099-12-31T23:59:59Z"),
        ] {
            assert_eq!(format_timestamp(at(secs)), text);
            assert_eq!(parse_rfc3339(text), Some(at(secs)));
        }
        assert_eq!(
            format_timestamp(at(1_789_927_200) + Duration::from_millis(999)),
            "2026-09-20T18:00:00Z"
        );
    }

    // --- attestations (§8) ----------------------------------------------------

    fn attestation(
        signer: &Ed25519Signer,
        question: &Value,
        status: &str,
        reason: Option<&str>,
        now: u64,
    ) -> Value {
        let mut object = json!({
            "format": 2, "protocol": PROTOCOL, "kind": "attestation", "kid": kid_of(signer),
            "status": status, "reason": reason, "checkedAt": format_timestamp(at(now)),
        });
        for field in [
            "target",
            "installationId",
            "candidateId",
            "requestSha256",
            "receiptSha256",
            "policy",
            "phase",
            "nonce",
        ] {
            object[field] = question[field].clone();
        }
        sign(signer, object)
    }

    #[test]
    fn attestation_acceptance() {
        let signer = Ed25519Signer::generate();
        let other = Ed25519Signer::generate();
        let trust = trust_of(&[&signer]);
        let b = binding('a', "aa");
        let question = b.question(INSTANCE, "finish", &"f".repeat(32));
        assert_eq!(
            question
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            [
                "protocol",
                "target",
                "installationId",
                "candidateId",
                "requestSha256",
                "receiptSha256",
                "policy",
                "kid",
                "phase",
                "nonce"
            ]
        );
        let now = at(NOW);
        let check = |body: &Value, now: SystemTime| {
            check_attestation(&trust, &question, body, INSTANCE, now)
        };
        let current = attestation(&signer, &question, "current", None, NOW);
        let body =
            |status: &str, att: &Value| json!({"format": 2, "status": status, "attestation": att});
        assert_eq!(check(&body("current", &current), now), Ok(()));
        assert_eq!(check(&body("current", &current), at(NOW + 300)), Ok(()));
        assert_eq!(
            check(&body("current", &current), at(NOW + 301)),
            Err(
                "verifier attestation invalid: attestation checkedAt 2026-09-20T18:00:00Z is more \
                 than 5 minutes from now"
                    .into()
            )
        );
        assert_eq!(
            check(&body("current", &current), at(NOW - 301)),
            Err(
                "verifier attestation invalid: attestation checkedAt 2026-09-20T18:00:00Z is more \
                 than 5 minutes from now"
                    .into()
            )
        );
        assert_eq!(
            check(&json!({"format": 2, "status": "current"}), now),
            Err("verifier attestation invalid: response shape".into())
        );
        assert_eq!(
            check(
                &json!({"format": 1, "status": "current", "attestation": current}),
                now
            ),
            Err("verifier attestation invalid: response shape".into())
        );
        assert_eq!(
            check(
                &json!({"format": 2, "status": "fine", "attestation": current}),
                now
            ),
            Err("verifier attestation invalid: response shape".into())
        );
        assert_eq!(
            check(&json!("current"), now),
            Err("verifier attestation invalid: response shape".into())
        );
        // Echo mismatches: nonce, phase, binding, target.
        let other_nonce = attestation(
            &signer,
            &b.question(INSTANCE, "finish", &"e".repeat(32)),
            "current",
            None,
            NOW,
        );
        assert_eq!(
            check(&body("current", &other_nonce), now),
            Err("verifier attestation invalid: fields differ from the question".into())
        );
        let other_phase = attestation(
            &signer,
            &b.question(INSTANCE, "start", &"f".repeat(32)),
            "current",
            None,
            NOW,
        );
        assert_eq!(
            check(&body("current", &other_phase), now),
            Err("verifier attestation invalid: fields differ from the question".into())
        );
        let other_binding = attestation(
            &signer,
            &binding('a', "bb").question(INSTANCE, "finish", &"f".repeat(32)),
            "current",
            None,
            NOW,
        );
        assert_eq!(
            check(&body("current", &other_binding), now),
            Err("verifier attestation invalid: fields differ from the question".into())
        );
        let other_target = attestation(
            &signer,
            &b.question("elsewhere", "finish", &"f".repeat(32)),
            "current",
            None,
            NOW,
        );
        assert_eq!(
            check(&body("current", &other_target), now),
            Err(
                "verifier attestation invalid: receipt targets instance elsewhere, this is c7smoke"
                    .into()
            )
        );
        // Statuses.
        let revoked = attestation(
            &signer,
            &question,
            "revoked",
            Some("policy_superseded"),
            NOW,
        );
        assert_eq!(
            check(&body("revoked", &revoked), now),
            Err("installation authorization revoked: policy_superseded".into())
        );
        let silent = attestation(&signer, &question, "revoked", None, NOW);
        assert_eq!(
            check(&body("revoked", &silent), now),
            Err("installation authorization revoked: unspecified".into())
        );
        assert_eq!(
            check(&body("current", &revoked), now),
            Err("verifier attestation invalid: fields differ from the question".into())
        );
        let unknown = attestation(&signer, &question, "unknown", None, NOW);
        assert_eq!(
            check(&body("unknown", &unknown), now),
            Err("installation unknown to the verifier authority".into())
        );
        // Keys: foreign refused, another trusted key accepted, tampering refused.
        let foreign = attestation(&other, &question, "current", None, NOW);
        assert_eq!(
            check(&body("current", &foreign), now),
            Err(format!(
                "verifier attestation invalid: unknown verifier key {}",
                kid_of(&other)
            ))
        );
        let rotated = trust_of(&[&signer, &other]);
        assert_eq!(
            check_attestation(
                &rotated,
                &question,
                &body("current", &foreign),
                INSTANCE,
                now
            ),
            Ok(())
        );
        let tampered = with(current.clone(), "status", json!("current"));
        assert_eq!(check(&body("current", &tampered), now), Ok(()));
        let tampered = with(revoked.clone(), "status", json!("current"));
        assert_eq!(
            check(&body("current", &tampered), now),
            Err("verifier attestation invalid: receipt signature invalid".into())
        );
        let wrong_kind = attestation(&signer, &question, "current", None, NOW);
        let wrong_kind = sign(
            &signer,
            with(
                without(with(wrong_kind, "kind", json!("install")), "signature"),
                "x",
                json!(1),
            ),
        );
        assert_eq!(
            check(&body("current", &wrong_kind), now),
            Err(
                "verifier attestation invalid: receipt kind \"install\" cannot be used here".into()
            )
        );
        // Numbers echoed as doubles still match (canonical comparison).
        let mut float = current.clone();
        float["policy"]["version"] = json!(3.0);
        assert_eq!(check(&body("current", &float), now), Ok(()));
    }

    // --- online check over real HTTP ------------------------------------------

    /// One round against a loopback HTTP/1.1 responder thread that answers
    /// the next `installation_check` with an attestation of `status` signed
    /// by a fresh key the returned `Protection` trusts (or with `raw`).
    fn responder(
        status: &'static str,
        raw: Option<&'static str>,
    ) -> (Protection, std::thread::JoinHandle<Value>) {
        let signer = Ed25519Signer::generate();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let protection = protection_for(&signer, port);
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = Vec::new();
            let mut chunk = [0u8; 4096];
            let headers_end = loop {
                let n = stream.read(&mut chunk).unwrap();
                assert!(n > 0, "client closed before sending the request");
                buffer.extend_from_slice(&chunk[..n]);
                if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let head = String::from_utf8_lossy(&buffer[..headers_end]).to_ascii_lowercase();
            assert!(
                head.starts_with("post /v1/installation_check http/1.1"),
                "{head}"
            );
            assert!(head.contains("content-type: application/json"), "{head}");
            assert!(!head.contains("authorization:"), "{head}");
            let length: usize = head
                .lines()
                .find_map(|line| {
                    line.strip_prefix("content-length:")
                        .map(|v| v.trim().parse().unwrap())
                })
                .unwrap();
            while buffer.len() < headers_end + length {
                let n = stream.read(&mut chunk).unwrap();
                assert!(n > 0);
                buffer.extend_from_slice(&chunk[..n]);
            }
            let question: Value =
                serde_json::from_slice(&buffer[headers_end..headers_end + length]).unwrap();
            // Freshness is judged against the wall clock, so sign with it.
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let body = match raw {
                Some(raw) => raw.to_string(),
                None => json!({
                    "format": 2,
                    "status": status,
                    "attestation": attestation(
                        &signer,
                        &question,
                        status,
                        (status == "revoked").then_some("policy_superseded"),
                        now,
                    ),
                })
                .to_string(),
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \
                 {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
            stream.flush().unwrap();
            question
        });
        (protection, handle)
    }

    fn protection_for(signer: &Ed25519Signer, port: u16) -> Protection {
        Protection::new(Config {
            trust: trust_of(&[signer]),
            supervisor_url: format!("http://127.0.0.1:{port}"),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn online_check_over_loopback_http() {
        let b = binding('a', "aa");

        // current ⇒ accepted, and the question has the §8 shape.
        let (protection, handle) = responder("current", None);
        online_check(&protection, INSTANCE, &b, "start")
            .await
            .unwrap();
        let question = handle.join().unwrap();
        assert_eq!(question["protocol"], json!(PROTOCOL));
        assert_eq!(question["target"], json!({"instanceName": INSTANCE}));
        assert_eq!(question["phase"], json!("start"));
        assert_eq!(question["kid"], json!(b.kid));
        assert_eq!(question["installationId"], json!(b.installation_id));
        assert_eq!(
            question["policy"],
            json!({"id": "c7", "version": 3, "sha256": "ab".repeat(32)})
        );
        assert!(is_hex(question["nonce"].as_str().unwrap(), 32));
        assert_eq!(question.as_object().unwrap().len(), 10);

        // revoked ⇒ rejected with the reason.
        let (protection, handle) = responder("revoked", None);
        let error = online_check(&protection, INSTANCE, &b, "finish")
            .await
            .unwrap_err();
        handle.join().unwrap();
        assert_eq!(error.short_msg(), "VerifiedDeployRejected");
        assert_eq!(
            error.msg(),
            "installation authorization revoked: policy_superseded"
        );

        // A 200 that is not an attestation ⇒ invalid.
        let (protection, handle) =
            responder("current", Some("{\"format\":2,\"status\":\"current\"}"));
        let error = online_check(&protection, INSTANCE, &b, "finish")
            .await
            .unwrap_err();
        handle.join().unwrap();
        assert_eq!(error.msg(), "verifier attestation invalid: response shape");

        // Closed port ⇒ unreachable, fail closed.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let protection = protection_for(&Ed25519Signer::generate(), port);
        let error = online_check(&protection, INSTANCE, &b, "start")
            .await
            .unwrap_err();
        assert_eq!(error.short_msg(), "VerifiedDeployRejected");
        assert!(error.is_bad_request());
        assert!(
            error.msg().starts_with(&format!(
                "verifier authority at http://127.0.0.1:{port} is unreachable ("
            )) && error.msg().ends_with("); nothing was deployed"),
            "{}",
            error.msg()
        );
    }

    // --- structural refusals (unchanged from v1) ------------------------------

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

    #[track_caller]
    fn assert_rejected(result: anyhow::Result<()>, code: &str, needle: &str) {
        let error = result.expect_err("gate must refuse");
        assert_eq!(error.short_msg(), code, "{error:#}");
        assert!(error.msg().contains(needle), "{error:#}");
        assert!(error.is_bad_request(), "{error:#}");
    }

    fn shape(raw: Value) -> anyhow::Result<()> {
        let req: StartPushRequest = serde_json::from_value(raw)?;
        check_shape(&req)
    }

    #[test]
    fn structural_refusals_still_apply() {
        shape(request("convex/", "isolate")).unwrap();
        assert_rejected(
            shape(request("convex/", "node")),
            "VerifiedDeployRejected",
            "isolate environment",
        );
        let mut dry_run = request("convex/", "isolate");
        dry_run["dryRun"] = json!(true);
        assert_rejected(
            shape(dry_run),
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
            shape(components),
            "VerifiedDeployRejected",
            "components are not allowed",
        );
        let mut node = request("convex/", "isolate");
        node["nodeVersion"] = json!("22");
        assert_rejected(
            shape(node),
            "VerifiedDeployRejected",
            "Node.js actions are not allowed",
        );
        let mut incremental = request("convex/", "isolate");
        incremental["appDefinition"]["unchangedModuleHashes"] =
            json!([{"path": "b.js", "environment": "isolate", "sha256": "00"}]);
        assert_rejected(
            shape(incremental),
            "VerifiedDeployRejected",
            "incremental pushes are not allowed",
        );
    }

    #[test]
    fn deploy_credentials_are_classified() {
        let signer = Ed25519Signer::generate();
        let protection = protection_for(&signer, 1);
        let now = at(NOW);
        let classify = |credential: &str| {
            install_credential(&protection, credential, INSTANCE, now).map(|_| ())
        };
        assert_rejected(
            classify(""),
            "VerifiedDeployRequired",
            "only a supervisor receipt does",
        );
        assert_rejected(
            classify("c7smoke|0123456789abcdef"),
            "VerifiedDeployRequired",
            "AMAGE (C7) Convex CLI",
        );
        assert_rejected(
            classify("amage-receipt/1.abc"),
            "VerifiedDeployRejected",
            "receipt token is malformed: prefix or size",
        );
        assert_rejected(
            classify(&encode_token(&observe(&signer))),
            "VerifiedDeployRejected",
            "receipt kind \"observe\" cannot be used here",
        );
        classify(&install_token(&signer, &vector_request())).unwrap();
        let foreign = Ed25519Signer::generate();
        assert_rejected(
            classify(&install_token(&foreign, &vector_request())),
            "VerifiedDeployRejected",
            "unknown verifier key",
        );
    }
}

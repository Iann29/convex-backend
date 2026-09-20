//! C7's optional, startup-pinned deploy boundary. The external verifier owns
//! the pin.
use std::sync::OnceLock;

use anyhow::Context;
use application::deploy_config::StartPushRequest;
use common::sha256::{
    Sha256,
    Sha256Digest,
};
use errors::ErrorMetadata;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{
    Mutex,
    MutexGuard,
};

static PIN: OnceLock<Option<String>> = OnceLock::new();
// ponytail: one pending deploy per process; use durable attestations for
// multiple writers.
static PENDING: Mutex<Option<Sha256Digest>> = Mutex::const_new(None);

pub fn initialize() -> anyhow::Result<()> {
    let pin = match std::env::var("BENDVEX_DEPLOY_SHA256") {
        Ok(pin) => {
            anyhow::ensure!(
                pin.len() == 64
                    && pin
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                "BENDVEX_DEPLOY_SHA256 must be 64 lowercase hex digits"
            );
            Some(pin)
        },
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => return Err(error.into()),
    };
    let existing = PIN.get_or_init(|| pin.clone());
    anyhow::ensure!(
        existing == &pin,
        "Bendvex startup pin cannot change in-process"
    );
    Ok(())
}

fn pin() -> anyhow::Result<Option<&'static str>> {
    Ok(PIN
        .get()
        .context("Bendvex startup state not initialized")?
        .as_deref())
}

fn denied() -> anyhow::Error {
    ErrorMetadata::bad_request(
        "BendvexDeployRejected",
        "Deploy is not authorized by the startup proof pin",
    )
    .into()
}

pub fn reject_legacy() -> anyhow::Result<()> {
    if pin()?.is_some() {
        return Err(denied());
    }
    Ok(())
}

pub fn digest(mut value: Value, request: bool) -> anyhow::Result<Sha256Digest> {
    if request {
        value
            .as_object_mut()
            .context("Deploy request must be an object")?
            .remove("adminKey");
    }
    value.sort_all_objects();
    let mut hash = Sha256::new();
    serde_json::to_writer(&mut hash, &value)?;
    Ok(hash.finalize())
}

pub async fn start(
    raw: Value,
    req: &StartPushRequest,
) -> anyhow::Result<Option<MutexGuard<'static, Option<Sha256Digest>>>> {
    let Some(pin) = pin()? else {
        return Ok(None);
    };
    let mut pending = PENDING.lock().await;
    // A new attempt invalidates any previous response, including on rejection.
    *pending = None;
    if digest(raw, true)?.as_hex() != pin
        || !req.component_definitions.is_empty()
        || !req.node_dependencies.is_empty()
        || req.node_version.is_some()
        || req.dry_run
        || req.for_codegen
        || req.app_definition.definition.is_some()
        || !req.app_definition.dependencies.is_empty()
        || !req.app_definition.unchanged_module_hashes.is_empty()
        || req
            .app_definition
            .changed_modules
            .iter()
            .any(|m| m.environment.as_deref() != Some("isolate"))
    {
        return Err(denied());
    }
    Ok(Some(pending))
}

pub fn remember(
    guard: &mut Option<MutexGuard<'static, Option<Sha256Digest>>>,
    response: &impl Serialize,
) -> anyhow::Result<()> {
    if let Some(pending) = guard {
        **pending = Some(digest(serde_json::to_value(response)?, false)?);
    }
    Ok(())
}

pub async fn finish(
    response: &impl Serialize,
) -> anyhow::Result<Option<MutexGuard<'static, Option<Sha256Digest>>>> {
    if pin()?.is_none() {
        return Ok(None);
    }
    let mut pending = PENDING.lock().await;
    let Some(expected) = pending.take() else {
        return Err(denied());
    };
    if expected != digest(serde_json::to_value(response)?, false)? {
        return Err(denied());
    }
    Ok(Some(pending))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_request_binds_all_fields_except_root_admin_key() -> anyhow::Result<()> {
        let a = serde_json::json!({"adminKey":"secret", "z":{"b":2,"a":1},"x":[1,2]});
        let b = serde_json::json!({"x":[1,2], "z":{"a":1,"b":2},"adminKey":"other"});
        assert_eq!(digest(a.clone(), true)?, digest(b.clone(), true)?);
        assert_ne!(digest(a, false)?, digest(b, false)?);
        assert_ne!(
            digest(serde_json::json!({"x":[1,2]}), true)?,
            digest(serde_json::json!({"x":[2,1]}), true)?
        );
        // Independent Python json.dumps(sort_keys=True, ensure_ascii=False,
        // separators=(',', ':')) + hashlib.sha256 vector, including Unicode.
        let unicode = serde_json::json!({"adminKey":"segredo", "ç":"ação 🧪", "nested":{"é":1,"A":"\n\t\""}, "list":["β","東京"]});
        assert_eq!(
            digest(unicode, true)?.as_hex(),
            "c34a11a3ba5d73726218dd550c211ef9fafc750f21a60333f493f34b45d445c0"
        );
        Ok(())
    }
}

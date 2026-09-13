//! Deployment-scoped, authenticated, in-memory explain captures.
use std::time::Duration;

use axum::{
    extract::Path,
    response::IntoResponse,
};
use common::{
    explain::{
        CaptureSnapshot,
        StartCapture,
    },
    http::{
        extract::{
            Json,
            MtState,
            Query,
        },
        HttpResponseError,
    },
    knobs::{
        APPLICATION_MAX_CONCURRENT_MUTATIONS,
        APPLICATION_MAX_CONCURRENT_NODE_ACTIONS,
        APPLICATION_MAX_CONCURRENT_QUERIES,
        APPLICATION_MAX_CONCURRENT_V8_ACTIONS,
    },
    version::SERVER_VERSION_STR,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    authentication::ExtractIdentity,
    LocalAppState,
};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CaptureResponse {
    #[serde(flatten)]
    capture: CaptureSnapshot,
    identity: CaptureIdentity,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CaptureIdentity {
    deployment_name: String,
    backend_version: String,
    database_engine: &'static str,
    persistence_driver_tag: &'static str,
    image_digest: Option<String>,
    code_version: Option<String>,
    available_parallelism: Option<usize>,
    max_concurrent_queries: usize,
    max_concurrent_mutations: usize,
    max_concurrent_v8_actions: usize,
    max_concurrent_node_actions: usize,
}

impl CaptureResponse {
    fn new(capture: CaptureSnapshot, st: &LocalAppState) -> Self {
        Self {
            capture,
            identity: CaptureIdentity {
                deployment_name: st.instance_name.clone(),
                backend_version: SERVER_VERSION_STR.to_string(),
                database_engine: st.explain_engine,
                persistence_driver_tag: st.explain_persistence_driver_tag,
                image_digest: None,
                code_version: None,
                available_parallelism: std::thread::available_parallelism().ok().map(|n| n.get()),
                max_concurrent_queries: *APPLICATION_MAX_CONCURRENT_QUERIES,
                max_concurrent_mutations: *APPLICATION_MAX_CONCURRENT_MUTATIONS,
                max_concurrent_v8_actions: *APPLICATION_MAX_CONCURRENT_V8_ACTIONS,
                max_concurrent_node_actions: *APPLICATION_MAX_CONCURRENT_NODE_ACTIONS,
            },
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PollCapture {
    #[serde(default)]
    pub cursor: u64,
    pub process_id: Option<String>,
}

pub async fn start(
    MtState(st): MtState<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
    Json(request): Json<StartCapture>,
) -> Result<impl IntoResponse, HttpResponseError> {
    // ViewLogs is checked against this deployment before touching sessions.
    let log = st.application.function_log(&identity)?;
    let recorder = log.explain_recorder().clone();
    let result = recorder.start(request)?;
    let duration = result.limits.duration_seconds;
    // At most four retained sessions per application, so these cleanup tasks
    // are bounded. Capture expiry and memory reclamation do not need traffic.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(duration)).await;
        recorder.cleanup();
        tokio::time::sleep(Duration::from_secs(301)).await;
        recorder.cleanup();
    });
    Ok(Json(CaptureResponse::new(result, &st)))
}

pub async fn poll(
    MtState(st): MtState<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
    Path(session_id): Path<String>,
    Query(query): Query<PollCapture>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let log = st.application.function_log(&identity)?;
    let capture = log.explain_recorder().poll(
        &session_id,
        query.cursor,
        query.process_id.as_deref(),
        false,
    )?;
    Ok(Json(CaptureResponse::new(capture, &st)))
}

pub async fn stop(
    MtState(st): MtState<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
    Path(session_id): Path<String>,
    Query(query): Query<PollCapture>,
) -> Result<impl IntoResponse, HttpResponseError> {
    let log = st.application.function_log(&identity)?;
    let capture = log.explain_recorder().poll(
        &session_id,
        query.cursor,
        query.process_id.as_deref(),
        true,
    )?;
    Ok(Json(CaptureResponse::new(capture, &st)))
}

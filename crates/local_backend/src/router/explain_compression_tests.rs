//! Exercises the production MethodRouter layer, real recorder and real token
//! extractor. The synthetic key check below is not Application/ViewLogs auth;
//! that boundary requires the separate running-backend gate.
use std::io::Read;

use axum::{
    body::{
        to_bytes,
        Body,
    },
    extract::{
        Path,
        State,
    },
    response::{
        IntoResponse,
        Response,
    },
    routing::{
        get,
        post,
    },
    Json,
    Router,
};
use common::{
    execution_context::{
        ExecutionContext,
        RequestContext,
    },
    explain::{
        CaptureState,
        ExplainRecorder,
        Outcome,
        StartCapture,
    },
    http::{
        cli_cors,
        extract::Query,
        HttpResponseError,
    },
    types::FunctionCaller,
    RequestId,
};
use flate2::read::GzDecoder;
use http::{
    header,
    HeaderMap,
    Method,
    Request,
    StatusCode,
};
use serde_json::{
    json,
    Value,
};
use sync_types::AuthenticationToken;
use tower::ServiceExt;

use super::compress_explain_snapshots;
use crate::{
    authentication::ExtractAuthenticationToken,
    explain::PollCapture,
};

const KEY: &str = "compression-fixture-key";
const ORIGIN: &str = "https://compression-test.invalid";
const BODY_LIMIT: usize = 64 * 1024;

async fn snapshot(
    State(recorder): State<ExplainRecorder>,
    Path(id): Path<String>,
    Query(query): Query<PollCapture>,
    ExtractAuthenticationToken(token): ExtractAuthenticationToken,
    method: Method,
) -> Result<Response, HttpResponseError> {
    if !matches!(token, AuthenticationToken::Admin(ref key, None) if key == KEY) {
        return Ok((
            StatusCode::UNAUTHORIZED,
            Json(json!({"code":"TestDenied", "message":"Synthetic test authorization denied"})),
        )
            .into_response());
    }
    Ok(Json(recorder.poll(
        &id,
        query.cursor,
        query.process_id.as_deref(),
        method == Method::DELETE,
    )?)
    .into_response())
}

fn fixture() -> (Router, ExplainRecorder, String) {
    let recorder = ExplainRecorder::default();
    let session = recorder
        .start(StartCapture {
            max_events: Some(2),
            max_bytes: Some(16 * 1024),
            ..Default::default()
        })
        .unwrap();
    // More events than retention allows: encoding must preserve both retained
    // cursors and the explicit incomplete-coverage counters.
    for _ in 0..3 {
        let context = ExecutionContext::new(
            RequestContext::new_for_system_request(RequestId::new()),
            &FunctionCaller::HttpEndpoint,
        );
        recorder
            .begin(&context, "fixture:read", "", "query", None)
            .unwrap()
            .finish(Outcome::Success);
    }
    let app = Router::new()
        .route("/explain/sessions/{session_id}", compress_explain_snapshots(get(snapshot).delete(snapshot)))
        .route("/explain/sessions", post(|| async { Json(json!({"neighbor":"POST remains outside the response compression layer"})) }))
        .route("/neighbor", get(|| async { Json(json!({"neighbor":"ordinary routes remain outside the response compression layer"})) }))
        .layer(cli_cors())
        .with_state(recorder.clone());
    (app, recorder, session.session_id)
}

async fn request(
    app: &Router,
    method: Method,
    path: &str,
    encoding: Option<&str>,
    auth: Option<&str>,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header(header::ORIGIN, ORIGIN);
    if let Some(encoding) = encoding {
        request = request.header(header::ACCEPT_ENCODING, encoding);
    }
    if let Some(auth) = auth {
        request = request.header(header::AUTHORIZATION, auth);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    let bytes = to_bytes(body, BODY_LIMIT).await.unwrap();
    let decoded = if parts
        .headers
        .get(header::CONTENT_ENCODING)
        .is_some_and(|v| v == "gzip")
    {
        let mut decoded = Vec::new();
        GzDecoder::new(bytes.as_ref())
            .take(BODY_LIMIT as u64 + 1)
            .read_to_end(&mut decoded)
            .unwrap();
        assert!(
            decoded.len() <= BODY_LIMIT,
            "decoded response exceeded test bound"
        );
        assert!(
            !parts.headers.contains_key(header::CONTENT_LENGTH),
            "compressed response retained a stale length"
        );
        decoded
    } else {
        bytes.to_vec()
    };
    (parts.status, parts.headers, decoded)
}

fn assert_vary(headers: &HeaderMap) {
    let tokens: Vec<_> = headers
        .get_all(header::VARY)
        .iter()
        .flat_map(|value| value.to_str().unwrap().split(','))
        .map(|token| token.trim().to_ascii_lowercase())
        .collect();
    for required in [
        "accept-encoding",
        "origin",
        "access-control-request-method",
        "access-control-request-headers",
    ] {
        assert!(
            tokens.iter().any(|token| token == required),
            "missing Vary token {required}: {tokens:?}"
        );
    }
    assert_eq!(
        headers.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
        ORIGIN
    );
}

#[tokio::test]
async fn explain_compression_negotiates_without_changing_snapshot_or_limits() {
    let (app, recorder, id) = fixture();
    let expected = recorder.poll(&id, 0, None, false).unwrap();
    assert!(expected.coverage.dropped_events > 0);
    assert_eq!(expected.events.len(), 2);
    assert!(expected.coverage.retained_bytes <= expected.limits.max_bytes);
    let expected_bytes = serde_json::to_vec(&expected).unwrap();
    let auth = format!("Convex {KEY}");
    for (encoding, gzip) in [
        (None, false),
        (Some(""), false),
        (Some("gzip"), true),
        (Some("gzip;q=0"), false),
        (Some("gzip;q=0.000, br;q=1"), false),
        (Some("gzip;q=0.5, identity;q=1"), false),
        (Some("gzip;q=1, identity;q=0"), true),
        (Some("gzip;q=invalid"), false),
        (Some("gzip;q=0, *;q=1"), false),
    ] {
        let (status, headers, bytes) = request(
            &app,
            Method::GET,
            &format!("/explain/sessions/{id}"),
            encoding,
            Some(&auth),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{encoding:?}");
        assert_eq!(
            headers
                .get(header::CONTENT_ENCODING)
                .is_some_and(|v| v == "gzip"),
            gzip,
            "{encoding:?}"
        );
        assert_vary(&headers);
        assert_eq!(bytes, expected_bytes, "{encoding:?}");
    }
}

#[tokio::test]
async fn explain_compression_delete_preserves_cursor_and_final_drain() {
    let (app, recorder, id) = fixture();
    let before = recorder.poll(&id, 0, None, false).unwrap();
    let cursor = before.events[0].cursor;
    let path = format!(
        "/explain/sessions/{id}?cursor={cursor}&processId={}",
        before.process_id
    );
    let auth = format!("Convex {KEY}");
    let (status, headers, stopped) =
        request(&app, Method::DELETE, &path, Some("gzip"), Some(&auth)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get(header::CONTENT_ENCODING).unwrap(), "gzip");
    assert_vary(&headers);
    let snapshot: Value = serde_json::from_slice(&stopped).unwrap();
    assert_eq!(snapshot["state"], "stopped");
    assert_eq!(snapshot["nextCursor"], before.next_cursor);
    assert_eq!(snapshot["events"].as_array().unwrap().len(), 1);
    for method in [Method::GET, Method::DELETE] {
        let (status, _, identity) =
            request(&app, method, &path, Some("gzip;q=0"), Some(&auth)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(identity, stopped);
    }
    assert_eq!(
        recorder.poll(&id, cursor, None, false).unwrap().state,
        CaptureState::Stopped
    );
}

#[tokio::test]
async fn explain_compression_preserves_token_rejection_and_neighbor_routes() {
    let (app, recorder, id) = fixture();
    let path = format!("/explain/sessions/{id}");
    let before = serde_json::to_vec(&recorder.poll(&id, 0, None, false).unwrap()).unwrap();
    for method in [Method::GET, Method::DELETE] {
        for auth in [None, Some("Convex wrong-fixture-key"), Some("malformed")] {
            let identity = request(&app, method.clone(), &path, Some("gzip;q=0"), auth).await;
            let gzip = request(&app, method.clone(), &path, Some("gzip"), auth).await;
            assert!(identity.0.is_client_error());
            assert_eq!(identity.0, gzip.0);
            assert_eq!(identity.2, gzip.2);
            assert!(!String::from_utf8(gzip.2).unwrap().contains(&id));
        }
    }
    let auth = format!("Convex {KEY}");
    for invalid_path in [
        format!("{path}?cursor=-1"),
        format!("{path}?processId=another-process"),
    ] {
        let identity = request(
            &app,
            Method::DELETE,
            &invalid_path,
            Some("gzip;q=0"),
            Some(&auth),
        )
        .await;
        let gzip = request(
            &app,
            Method::DELETE,
            &invalid_path,
            Some("gzip"),
            Some(&auth),
        )
        .await;
        assert!(identity.0.is_client_error());
        assert_eq!(identity.0, gzip.0);
        assert_eq!(identity.2, gzip.2);
    }
    assert_eq!(
        before,
        serde_json::to_vec(&recorder.poll(&id, 0, None, false).unwrap()).unwrap(),
        "rejected DELETE touched the capture"
    );
    for (method, path, expected) in [
        (Method::POST, "/explain/sessions", StatusCode::OK),
        (Method::GET, "/neighbor", StatusCode::OK),
        (Method::PUT, path.as_str(), StatusCode::METHOD_NOT_ALLOWED),
    ] {
        let (status, headers, _) = request(&app, method, path, Some("gzip"), None).await;
        assert_eq!(status, expected);
        assert!(!headers.contains_key(header::CONTENT_ENCODING));
    }
}

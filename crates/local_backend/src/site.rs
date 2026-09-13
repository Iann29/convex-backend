use std::net::SocketAddr;

use axum::Router;
use common::{
    http::{
        ConvexHttpService,
        NoopRouteMapper,
    },
    knobs::HTTP_SERVER_TIMEOUT_DURATION,
};

/// Serve the application's HTTP routes on the site port without an internal
/// HTTP hop.
pub async fn serve_http_actions(
    site_bind_addr: Option<([u8; 4], u16)>,
    router: Router,
    mut shutdown_rx: async_broadcast::Receiver<()>,
) -> anyhow::Result<()> {
    let Some(addr) = site_bind_addr else {
        return Ok(());
    };
    let max_concurrent_requests =
        cmd_util::env::env_config::<usize>("SITE_MAX_CONCURRENT_REQUESTS", 16);
    anyhow::ensure!(
        max_concurrent_requests > 0,
        "SITE_MAX_CONCURRENT_REQUESTS must be positive"
    );
    // Keep the existing service/metric identity and meta-route behavior.
    let service = ConvexHttpService::new(
        Router::new().fallback_service(router),
        "backend_http_proxy",
        "unknown".to_string(),
        max_concurrent_requests,
        *HTTP_SERVER_TIMEOUT_DURATION,
        NoopRouteMapper,
    );
    service
        .serve(SocketAddr::from(addr), async move {
            let _ = shutdown_rx.recv().await;
            tracing::info!("Shut down site HTTP service");
        })
        .await
}

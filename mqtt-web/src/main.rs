//! AtomMQTT Broker - Main entry point.
//!
//! Starts both the MQTT broker server and the web management interface.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, error};
use tracing_subscriber::EnvFilter;

use mqtt_broker::persistence::Persistence;

mod api;
mod models;

use actix_web::HttpResponse;
use include_dir::{include_dir, Dir};

/// Embedded static files directory (compiled into binary).
static STATIC_DIR: Dir<'_> = include_dir!("mqtt-web/static");

/// Serve a file from the embedded static directory.
/// All paths under `/` are resolved against the `static/` directory.
async fn serve_embedded_file(req: actix_web::HttpRequest) -> HttpResponse {
    let path = req.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match STATIC_DIR.get_file(path) {
        Some(file) => {
            let body = file.contents();
            let mime = mime_guess::from_path(path)
                .first_or_octet_stream()
                .to_string();
            HttpResponse::Ok().content_type(mime).body(body)
        }
        None => HttpResponse::NotFound().body("404 Not Found"),
    }
}

/// Main entry point.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info,mqtt_broker=debug,mqtt_web=debug")))
        .init();

    info!("AtomMQTT Broker starting...");

    // Load configuration (from config.toml or defaults)
    let config = mqtt_broker::config::load_config();

    // Create persistence for state restoration
    let persistence = Persistence::open(&config).expect("Failed to open persistence DB");
    let persistence_arc = Arc::new(persistence);

    // Create shared broker state
    let state = Arc::new(mqtt_broker::BrokerState::new(config.clone(), persistence_arc.clone()));

    // Restore state from DB
    for session in persistence_arc.load_sessions() {
        state.sessions.insert(session.client_id.clone(), session);
    }
    for (cid, filter, qos) in persistence_arc.load_subscriptions() {
        state.subscriptions.lock().unwrap().subscribe(&cid, &filter, qos);
    }
    for msg in persistence_arc.load_retained() {
        state.retained.insert(msg.topic.clone(), msg);
    }
    for will in persistence_arc.load_wills() {
        state.wills.insert(will.client_id.clone(), will);
    }

    // Graceful shutdown handler
    {
        let p = persistence_arc.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down...");
            p.shutdown().await;
            std::process::exit(0);
        });
    }

    // Start uptime counter
    let uptime_state = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            uptime_state.metrics.lock().unwrap().uptime_seconds += 1;
        }
    });

    // Start MQTT broker
    let broker_handle = mqtt_broker::server::start_broker(state.clone()).await?;

    // Store broker handle in state for API to send messages
    *state.broker_handle.lock().unwrap() = Some(broker_handle);

    // Start Web management interface
    let web_state = state.clone();
    let web_fut = start_web_server(web_state);

    // Run both servers
    tokio::select! {
        result = web_fut => {
            if let Err(e) = result {
                error!("Web server error: {}", e);
            }
        }
    }

    Ok(())
}

/// Start the web management server.
async fn start_web_server(state: Arc<mqtt_broker::BrokerState>) -> anyhow::Result<()> {
    use actix_web::{web, App, HttpServer, middleware};

    let addr = format!("{}:{}", state.config.web_host, state.config.web_port);
    let state_data = web::Data::new(state);

    info!("Web management interface starting on http://{}", addr);

    let server = HttpServer::new(move || {
        App::new()
            .app_data(state_data.clone())
            .wrap(middleware::Logger::default())
            // API routes
            .service(api::get_metrics)
            .service(api::get_clients)
            .service(api::get_client_detail)
            .service(api::get_subscriptions)
            .service(api::get_retained_messages)
            .service(api::get_broker_info)
            .service(api::publish_message)
            .service(api::disconnect_client)
            // WebSocket endpoints
            .route("/ws/subscribe", actix_web::web::get().to(api::ws_subscribe))
            .route("/mqtt", actix_web::web::get().to(api::ws_mqtt))
            // Embedded static files (compiled into binary at build time)
            .route("/{path:.*}", actix_web::web::get().to(serve_embedded_file))
    })
    .bind(&addr)?
    .run();

    server.await?;
    Ok(())
}

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        RwLock,
    },
    time::Duration,
};

use axum::{
    extract::{Json, State},
    http::StatusCode,
    routing::{get, post},
    Router,
};
use hbb_common::{
    anyhow::{anyhow, Result},
    config::{self, keys::*, Config},
    log,
    tokio::{
        net::TcpListener,
        select,
        sync::mpsc::{self, Receiver, Sender},
        time,
    },
};
use serde_json::{json, Value};

// --service ------------------------------------------------------------------
// Owns Axum, registration, reporting, the plaintext password and its rotation.
const LISTEN_PORT: u16 = 3000;
const ALIVE_CONN_POLL_INTERVAL_SECS: u64 = 5;
const ZERO_CONNECTION_DISABLE_SECS: u64 = 3 * 60;
const REGISTER_INTERVAL_SECS: u64 = 30;
const PASSWORD_ROTATE_SECS: u64 = 5 * 60;
const PASSWORD_LENGTH: usize = 10;
const BUSINESS: &str = "rustdesk";
const REGISTER_ENDPOINT: &str = "http://localhost:35000/register";
const ABILITY_ACK_ENDPOINT: &str = "http://localhost:35000/ability_ack";

static ABILITY_STARTED: AtomicBool = AtomicBool::new(false);

lazy_static::lazy_static! {
    static ref CURRENT_PASSWORD: RwLock<String> = RwLock::new(String::new());
    static ref ABILITY_ACK_CLIENT: Option<reqwest::Client> = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .ok();
}

enum AbilityAckEvent {
    Start,
    Stop,
}

pub fn start() {
    hbb_common::tokio::spawn(async {
        run().await;
    });
}

async fn run() {
    let addr = SocketAddr::from(([127, 0, 0, 1], LISTEN_PORT));
    let (ability_ack_tx, ability_ack_rx) = mpsc::channel(16);
    let app = Router::new()
        .route("/ability", post(ability))
        .route("/account", get(account))
        .with_state(ability_ack_tx);

    match TcpListener::bind(addr).await {
        Ok(listener) => {
            log::info!("Internal ability API listening on http://{addr}");
            start_registration();
            hbb_common::tokio::spawn(ability_ack_worker(ability_ack_rx));
            if let Err(err) = axum::serve(listener, app).await {
                log::error!("Internal ability API stopped: {err}");
            }
        }
        Err(err) => {
            log::error!("Failed to bind internal ability API on {addr}: {err}");
        }
    }
}

fn start_registration() {
    hbb_common::tokio::spawn(async {
        let Some(client) = ABILITY_ACK_CLIENT.as_ref() else {
            return;
        };
        let payload = json!({
            "business": BUSINESS,
            "port": LISTEN_PORT,
        });
        let mut ticker = time::interval(Duration::from_secs(REGISTER_INTERVAL_SECS));

        loop {
            ticker.tick().await;
            if let Err(err) = client
                .post(REGISTER_ENDPOINT)
                .json(&payload)
                .send()
                .await
                .and_then(|resp| resp.error_for_status())
            {
                log::warn!("Failed to register rustdesk service to iothub-client: {err}");
            }
        }
    });
}

async fn ability(
    State(ability_ack_tx): State<Sender<AbilityAckEvent>>,
    Json(body): Json<Value>,
) -> StatusCode {
    if body.pointer("/data/type").and_then(Value::as_str) != Some(BUSINESS) {
        return StatusCode::OK;
    }
    match body.pointer("/data/action").and_then(Value::as_str) {
        Some("start") => queue_ability_ack(&ability_ack_tx, AbilityAckEvent::Start).await,
        Some("stop") => queue_ability_ack(&ability_ack_tx, AbilityAckEvent::Stop).await,
        _ => {}
    }

    StatusCode::OK
}

async fn account() -> Json<Value> {
    Json(account_data("running").await.unwrap_or_else(|err| {
        json!({
            "ok": false,
            "error": err.to_string(),
        })
    }))
}

async fn queue_ability_ack(tx: &Sender<AbilityAckEvent>, event: AbilityAckEvent) {
    if tx.send(event).await.is_err() {
        log::warn!("Failed to queue ability ack because the worker stopped");
    }
}

async fn ability_ack_worker(mut rx: Receiver<AbilityAckEvent>) {
    let now = time::Instant::now();
    let alive_connection_interval = Duration::from_secs(ALIVE_CONN_POLL_INTERVAL_SECS);
    let password_interval = Duration::from_secs(PASSWORD_ROTATE_SECS);
    let mut alive_connection_ticker =
        time::interval_at(now + alive_connection_interval, alive_connection_interval);
    let mut password_ticker = time::interval_at(now + password_interval, password_interval);
    let mut last_alive_connection_count = None;
    let mut zero_connection_since = None;

    loop {
        select! {
            event = rx.recv() => {
                let Some(event) = event else {
                    break;
                };
                match event {
                    AbilityAckEvent::Start => {
                        zero_connection_since = None;
                        ABILITY_STARTED.store(true, Ordering::Relaxed);
                        send_ability_ack("start", "running").await;
                    }
                    AbilityAckEvent::Stop => {
                        zero_connection_since = None;
                        ABILITY_STARTED.store(false, Ordering::Relaxed);
                        send_ability_ack("stop", "stopped").await;
                    }
                }
            }
            _ = alive_connection_ticker.tick() => {
                let password_initialized = current_password().is_empty();
                if password_initialized {
                    if let Err(err) = rotate_password().await {
                        log::warn!("Failed to initialize internal API password: {err}");
                        continue;
                    }
                }
                match account_data("running").await {
                    Ok(mut result) => {
                        let current = result["userNum"].as_i64().unwrap_or_default();
                        let changed = last_alive_connection_count
                            .map_or(true, |last| last != current);
                        last_alive_connection_count = Some(current);
                        if !ABILITY_STARTED.load(Ordering::Relaxed) {
                            zero_connection_since = None;
                        } else if current == 0 {
                            if password_initialized {
                                send_ability_ack_with_result("start", result).await;
                                zero_connection_since = Some(time::Instant::now());
                            } else {
                                let zero_since =
                                    zero_connection_since.get_or_insert_with(time::Instant::now);
                                if zero_since.elapsed()
                                    >= Duration::from_secs(ZERO_CONNECTION_DISABLE_SECS)
                                {
                                    result["rdStatus"] = json!("stopped");
                                    send_ability_ack_with_result("stop", result).await;
                                    ABILITY_STARTED.store(false, Ordering::Relaxed);
                                    zero_connection_since = None;
                                }
                            }
                        } else {
                            zero_connection_since = None;
                            if password_initialized || changed {
                                send_ability_ack_with_result("start", result).await;
                            }
                        }
                    }
                    Err(err) => log::warn!("Failed to query current server account data: {err}"),
                }
            }
            _ = password_ticker.tick() => {
                match rotate_password().await {
                    Ok(()) if ABILITY_STARTED.load(Ordering::Relaxed) => {
                        send_ability_ack("start", "running").await;
                    }
                    Ok(()) => {}
                    Err(err) => log::warn!("Failed to rotate internal API password: {err}"),
                }
            }
        }
    }
}

async fn send_ability_ack(action: &str, status: &str) {
    match account_data(status).await {
        Ok(result) => send_ability_ack_with_result(action, result).await,
        Err(err) => log::warn!("Failed to query current server for ability {action} ack: {err}"),
    }
}

async fn send_ability_ack_with_result(action: &str, result: Value) {
    let Some(client) = ABILITY_ACK_CLIENT.as_ref() else {
        return;
    };
    let body = json!({
        "bid": uuid::Uuid::new_v4().to_string(),
        "tid": uuid::Uuid::new_v4().to_string(),
        "ts": chrono::Utc::now().timestamp_millis(),
        "data": {
            "type": BUSINESS,
            "action": action,
            "result": result,
        },
    });
    if let Err(err) = client
        .post(ABILITY_ACK_ENDPOINT)
        .json(&body)
        .send()
        .await
        .and_then(|response| response.error_for_status())
    {
        log::warn!("Failed to report ability {action} ack: {err}");
    }
}

async fn rotate_password() -> Result<()> {
    let password = Config::get_auto_password(PASSWORD_LENGTH);
    crate::ipc::set_internal_api_password(password.clone()).await?;
    *CURRENT_PASSWORD
        .write()
        .map_err(|err| anyhow!("Failed to cache internal API password: {err}"))? = password;
    log::info!("Internal API password rotated");
    Ok(())
}

fn current_password() -> String {
    CURRENT_PASSWORD
        .read()
        .map(|password| password.clone())
        .unwrap_or_default()
}

async fn account_data(status: &str) -> Result<Value> {
    let (rd_id, user_num) = crate::ipc::get_internal_api_account().await?;
    let rd_pwd = current_password();
    if rd_pwd.is_empty() {
        return Err(anyhow!("Internal API password is not initialized"));
    }

    Ok(json!({
        "rdID": rd_id,
        "rdPwd": rd_pwd,
        "snMac": "",
        "rdStatus": status,
        "ts": chrono::Utc::now().timestamp_millis(),
        "userNum": user_num,
    }))
}

// --server -------------------------------------------------------------------
// Owns RustDesk configuration, identity, password storage and live connections.
const ID_SERVER: &str = env!("RUSTDESK_ID_SERVER");
const RELAY_SERVER: &str = env!("RUSTDESK_RELAY_SERVER");
const SERVER_KEY: &str = env!("RUSTDESK_SERVER_KEY");

static SERVER_READY: AtomicBool = AtomicBool::new(false);

pub(crate) fn initialize_server() {
    apply_startup_config();
    set_hostname_id();
    Config::set_option(
        OPTION_VERIFICATION_METHOD.to_owned(),
        "use-permanent-password".to_owned(),
    );
    SERVER_READY.store(true, Ordering::Release);
}

pub(crate) fn server_account() -> Option<(String, usize)> {
    SERVER_READY
        .load(Ordering::Acquire)
        .then(|| (Config::get_id(), crate::server::alive_connection_count()))
}

fn apply_startup_config() {
    let before = (
        Config::get_option(OPTION_CUSTOM_RENDEZVOUS_SERVER),
        Config::get_option(OPTION_API_SERVER),
        Config::get_option(OPTION_RELAY_SERVER),
        Config::get_option(OPTION_KEY),
        Config::get_option(OPTION_DIRECT_SERVER),
        Config::get_option(OPTION_ALLOW_REMOTE_CONFIG_MODIFICATION),
        Config::get_option(OPTION_ALLOW_LINUX_HEADLESS),
    );

    {
        let mut defaults = config::DEFAULT_SETTINGS.write().unwrap();
        for key in [
            OPTION_DIRECT_SERVER,
            OPTION_ALLOW_REMOTE_CONFIG_MODIFICATION,
            OPTION_ALLOW_LINUX_HEADLESS,
        ] {
            defaults
                .entry(key.to_owned())
                .or_insert_with(|| "Y".to_owned());
        }
    }
    config::BUILTIN_SETTINGS
        .write()
        .unwrap()
        .insert(OPTION_REGISTER_DEVICE.to_owned(), "N".to_owned());

    for (key, value) in [
        (OPTION_CUSTOM_RENDEZVOUS_SERVER, ID_SERVER),
        (OPTION_API_SERVER, ""),
        (OPTION_RELAY_SERVER, RELAY_SERVER),
        (OPTION_KEY, SERVER_KEY),
        (OPTION_DIRECT_SERVER, "Y"),
        (OPTION_ALLOW_REMOTE_CONFIG_MODIFICATION, "Y"),
        (OPTION_ALLOW_LINUX_HEADLESS, "Y"),
    ] {
        Config::set_option(key.to_owned(), value.to_owned());
    }

    let after = (
        Config::get_option(OPTION_CUSTOM_RENDEZVOUS_SERVER),
        Config::get_option(OPTION_API_SERVER),
        Config::get_option(OPTION_RELAY_SERVER),
        Config::get_option(OPTION_KEY),
        Config::get_option(OPTION_DIRECT_SERVER),
        Config::get_option(OPTION_ALLOW_REMOTE_CONFIG_MODIFICATION),
        Config::get_option(OPTION_ALLOW_LINUX_HEADLESS),
    );
    if before != after {
        crate::RendezvousMediator::restart();
    }
}

fn set_hostname_id() {
    Config::set_key_confirmed(false);
    if let Some(id) = sanitized_hostname() {
        Config::set_id(&id);
        log::info!("RustDesk ID set to hostname: {id}");
    } else {
        Config::update_id();
        log::info!("Hostname is empty; RustDesk ID set to a random ID");
    }
}

fn sanitized_hostname() -> Option<String> {
    let id = crate::common::hostname()
        .trim()
        .replace(' ', "-")
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        .collect::<String>();
    (!id.is_empty()).then_some(id)
}

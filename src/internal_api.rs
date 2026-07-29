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
    anyhow::Result,
    config::{self, keys::*, Config},
    log,
    sha2::{Digest, Sha256},
    tokio::{
        net::TcpListener,
        select,
        sync::mpsc::{self, Receiver, Sender},
        time,
    },
};
use serde_json::{json, Value};

// --service ------------------------------------------------------------------
// Owns Axum, registration and reporting.
const LISTEN_PORT: u16 = 3000;
const ALIVE_CONN_POLL_INTERVAL_SECS: u64 = 10;
const ZERO_CONNECTION_DISABLE_SECS: u64 = 60;
const REGISTER_INTERVAL_SECS: u64 = 30;
const BUSINESS: &str = "rustdesk";
const REGISTER_ENDPOINT: &str = "http://localhost:35000/register";
const ABILITY_ACK_ENDPOINT: &str = "http://localhost:35000/ability_ack";

static ABILITY_STARTED: AtomicBool = AtomicBool::new(false);

lazy_static::lazy_static! {
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
    let mut alive_connection_ticker =
        time::interval(Duration::from_secs(ALIVE_CONN_POLL_INTERVAL_SECS));
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
                if !ABILITY_STARTED.load(Ordering::Relaxed) {
                    zero_connection_since = None;
                    continue;
                }
                match account_data("running").await {
                    Ok(mut result) => {
                        let current = result["userNum"].as_i64().unwrap_or_default();
                        if current == 0 {
                            let zero_since =
                                zero_connection_since.get_or_insert_with(time::Instant::now);
                            if zero_since.elapsed()
                                >= Duration::from_secs(ZERO_CONNECTION_DISABLE_SECS)
                            {
                                result["rdStatus"] = json!("stopped");
                                send_ability_ack_with_result("stop", result).await;
                                ABILITY_STARTED.store(false, Ordering::Relaxed);
                                zero_connection_since = None;
                            } else {
                                send_ability_ack_with_result("start", result).await;
                            }
                        } else {
                            zero_connection_since = None;
                            send_ability_ack_with_result("start", result).await;
                        }
                    }
                    Err(err) => log::warn!("Failed to query current server account data: {err}"),
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

async fn account_data(status: &str) -> Result<Value> {
    let (rd_id, rd_pwd, user_num) = crate::ipc::get_internal_api_account().await?;

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
const PASSWORD_DATE_CHECK_SECS: u64 = 30;
const PASSWORD_LENGTH: usize = 10;
const ID_SERVER: &str = env!("RUSTDESK_ID_SERVER");
const RELAY_SERVER: &str = env!("RUSTDESK_RELAY_SERVER");
const SERVER_KEY: &str = env!("RUSTDESK_SERVER_KEY");

static SERVER_READY: AtomicBool = AtomicBool::new(false);
lazy_static::lazy_static! {
    static ref SERVER_PASSWORD: RwLock<String> = RwLock::new(String::new());
}

pub(crate) fn initialize_server() {
    apply_startup_config();
    set_hostname_id();
    Config::set_option(
        OPTION_VERIFICATION_METHOD.to_owned(),
        "use-permanent-password".to_owned(),
    );
    let mut password_date = set_daily_server_password();
    SERVER_READY.store(true, Ordering::Release);
    hbb_common::tokio::spawn(async move {
        let interval = Duration::from_secs(PASSWORD_DATE_CHECK_SECS);
        let mut ticker = time::interval_at(time::Instant::now() + interval, interval);
        loop {
            ticker.tick().await;
            if password_date.as_deref() != Some(&chrono::Local::now().date_naive().to_string()) {
                password_date = set_daily_server_password();
            }
        }
    });
}

pub(crate) fn server_account() -> Option<(String, String, usize)> {
    if !SERVER_READY.load(Ordering::Acquire) {
        return None;
    }
    let password = match SERVER_PASSWORD.read() {
        Ok(password) => password.clone(),
        Err(err) => {
            log::error!("Failed to read server password: {err}");
            return None;
        }
    };
    if password.is_empty() {
        return None;
    }
    Some((
        Config::get_id(),
        password,
        crate::server::alive_connection_count(),
    ))
}

fn set_daily_server_password() -> Option<String> {
    let date = chrono::Local::now().date_naive().to_string();
    let source = format!("{date}:{}", Config::get_id());
    let password = Sha256::digest(source.as_bytes())
        .iter()
        .take(PASSWORD_LENGTH / 2)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    if !Config::set_permanent_password(&password) {
        log::warn!("Failed to set daily server password");
        return None;
    }
    match SERVER_PASSWORD.write() {
        Ok(mut current) => {
            *current = password;
            log::info!("Daily server password set for {date}");
            Some(date)
        }
        Err(err) => {
            log::error!("Failed to cache server password: {err}");
            None
        }
    }
}

pub(crate) fn reapply_server_password() {
    if !SERVER_READY.load(Ordering::Acquire) {
        return;
    }
    let password = match SERVER_PASSWORD.read() {
        Ok(password) => password.clone(),
        Err(err) => {
            log::error!("Failed to read server password after config sync: {err}");
            return;
        }
    };
    if !password.is_empty() && !Config::set_permanent_password(&password) {
        log::warn!("Failed to reapply server password after config sync");
    }
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

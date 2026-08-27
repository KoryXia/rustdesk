use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex, OnceLock, RwLock,
    },
    time::Duration,
};

use axum::{routing::get, Json, Router};
use hbb_common::{
    anyhow::{Result, anyhow, bail},
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
use serde::Deserialize;
use serde_json::{json, Value};
use zenoh::handlers::FifoChannelHandler;
use zenoh::pubsub::Subscriber;
use zenoh::sample::Sample;

// --zenoh --------------------------------------------------------------------
// Interacts with iothub-client over the zenoh nc/v1 keyspace.
const ABILITY_PREFIX: &str = "nc/v1/events/iothub_client/ability/rustdesk/";
const ABILITY_ACK_PREFIX: &str = "nc/v1/events/iothub_client/ability_ack/rustdesk/";
const DEFAULT_ZENOH_CONNECT: &str = "tcp/192.168.217.100:37447";

static ZENOH_SESSION: OnceLock<zenoh::Session> = OnceLock::new();

fn build_zenoh_config() -> zenoh::Config {
    let mut config = zenoh::Config::default();
    let _ = config.insert_json5("mode", "\"client\"");
    let _ = config.insert_json5("scouting/multicast/enabled", "false");
    let connect = std::env::var("ZENOH_CONNECT")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_ZENOH_CONNECT.to_string());
    let endpoints: Vec<&str> = connect
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    if let Ok(json) = serde_json::to_string(&endpoints) {
        if config.insert_json5("connect/endpoints", &json).is_err() {
            log::warn!("Failed to set zenoh connect/endpoints");
        }
    }
    config
}

async fn init_zenoh() -> Result<()> {
    if ZENOH_SESSION.get().is_some() {
        return Ok(());
    }
    let session = time::timeout(Duration::from_secs(5), zenoh::open(build_zenoh_config()))
        .await
        .map_err(|_| anyhow!("open zenoh session timeout (5s)"))?
        .map_err(|e| anyhow!("open zenoh session: {e}"))?;
    ZENOH_SESSION
        .set(session)
        .map_err(|_| anyhow!("zenoh session already initialized"))
}

fn zenoh_session() -> Result<&'static zenoh::Session> {
    ZENOH_SESSION
        .get()
        .ok_or_else(|| anyhow!("zenoh session not initialized"))
}

async fn publish(key: &str, payload: impl Into<String>) -> Result<()> {
    zenoh_session()?
        .put(key, payload.into())
        .await
        .map_err(|e| anyhow!("publish {key}: {e}"))
}

#[derive(Deserialize)]
struct AbilityEnvelope {
    ts: i64,
    data: AbilityEnvelopeData,
}

#[derive(Deserialize)]
struct AbilityEnvelopeData {
    bid: Option<String>,
    tid: Option<String>,
    params: Option<Value>,
}

fn ability_action_from_key(key: &str) -> Option<String> {
    let rest = key.strip_prefix(ABILITY_PREFIX)?;
    let mut parts = rest.split('/');
    let _type = parts.next()?;
    parts.next().map(str::to_string).filter(|a| !a.is_empty())
}

async fn init_ability_subscriber() -> Result<()> {
    let session = zenoh_session()?;
    let subscriber: Subscriber<FifoChannelHandler<Sample>> = session
        .declare_subscriber(format!("{ABILITY_PREFIX}**"))
        .await
        .map_err(|e| anyhow!("declare ability subscriber: {e}"))?;
    log::info!("RustDesk subscribed to zenoh ability channel");
    hbb_common::tokio::spawn(async move {
        loop {
            match subscriber.recv_async().await {
                Ok(sample) => {
                    let key = sample.key_expr().to_string();
                    let payload = match sample.payload().try_to_string() {
                        Ok(p) => p.to_string(),
                        Err(e) => {
                            log::warn!("Ability payload not UTF-8: key={key} err={e}");
                            continue;
                        }
                    };
                    if let Err(e) = handle_ability(&key, &payload).await {
                        log::warn!("Failed to handle zenoh ability message: key={key} err={e}");
                    }
                }
                Err(e) => {
                    log::warn!("Zenoh ability subscription closed: {e}");
                    return;
                }
            }
        }
    });
    Ok(())
}

async fn handle_ability(key: &str, payload: &str) -> Result<()> {
    let Some(action) = ability_action_from_key(key) else {
        bail!("ability key missing action: {key}");
    };
    let envelope: AbilityEnvelope = serde_json::from_str(payload)
        .map_err(|e| anyhow!("parse ability payload: {e}"))?;
    set_ability_context(envelope.data.bid, envelope.data.tid);
    match action.as_str() {
        "start" => queue_ability_ack(AbilityAckEvent::Start).await,
        "stop" => queue_ability_ack(AbilityAckEvent::Stop).await,
        _ => bail!("ability unsupported action: {action}"),
    }
    Ok(())
}

// --service ------------------------------------------------------------------
// Owns the internal HTTP API (account query) and ability reporting.
const LISTEN_PORT: u16 = 3000;
const ALIVE_CONN_POLL_INTERVAL_SECS: u64 = 10;
const ZERO_CONNECTION_DISABLE_SECS: u64 = 60;

static ABILITY_STARTED: AtomicBool = AtomicBool::new(false);
static ABILITY_CONTEXT: Mutex<Option<(String, String)>> = Mutex::new(None);
static ABILITY_ACK_TX: OnceLock<Sender<AbilityAckEvent>> = OnceLock::new();

fn set_ability_context(bid: Option<String>, tid: Option<String>) {
    if let (Some(bid), Some(tid), Ok(mut context)) = (bid, tid, ABILITY_CONTEXT.lock()) {
        *context = Some((bid, tid));
    }
}

fn ability_context() -> (Option<String>, Option<String>) {
    match ABILITY_CONTEXT.lock().ok().and_then(|c| c.clone()) {
        Some((bid, tid)) => (Some(bid), Some(tid)),
        None => (None, None),
    }
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
    if let Err(e) = init_zenoh().await {
        log::warn!("Zenoh init failed: {e}");
        return;
    }
    if let Err(e) = init_ability_subscriber().await {
        log::warn!("Ability subscriber init failed: {e}");
        return;
    }
    let addr = SocketAddr::from(([127, 0, 0, 1], LISTEN_PORT));
    let (ability_ack_tx, ability_ack_rx) = mpsc::channel(16);
    if ABILITY_ACK_TX.set(ability_ack_tx).is_err() {
        log::warn!("Ability ack channel already initialized");
    }
    let app = Router::new().route("/account", get(account));

    match TcpListener::bind(addr).await {
        Ok(listener) => {
            log::info!("Internal ability API listening on http://{addr}");
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

async fn account() -> Json<Value> {
    Json(account_data("running").await.unwrap_or_else(|err| {
        json!({
            "ok": false,
            "error": err.to_string(),
        })
    }))
}

async fn queue_ability_ack(event: AbilityAckEvent) {
    let Some(tx) = ABILITY_ACK_TX.get() else {
        log::warn!("Ability ack channel not initialized");
        return;
    };
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
    let (bid, tid) = ability_context();
    let key = format!("{ABILITY_ACK_PREFIX}{action}");
    let body = json!({
        "ts": chrono::Utc::now().timestamp_millis(),
        "data": {
            "bid": bid,
            "tid": tid,
            "result": result,
        },
    });
    if let Err(e) = publish(&key, body.to_string()).await {
        log::warn!("Failed to report ability {action} ack: {e}");
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

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Once, RwLock},
    time::Duration,
};

use axum::{extract::Json, routing::get, Router};
use hbb_common::{
    config::{self, keys::*, Config},
    log,
    tokio::{net::TcpListener, time},
};
use serde::Serialize;

const LISTEN_PORT: u16 = 3000;
const PASSWORD_ROTATE_SECS: u64 = 10 * 60;
const ID_SERVER: &str = env!("RUSTDESK_ID_SERVER");
const RELAY_SERVER: &str = env!("RUSTDESK_RELAY_SERVER");
const SERVER_KEY: &str = env!("RUSTDESK_SERVER_KEY");

static START: Once = Once::new();

lazy_static::lazy_static! {
    static ref CURRENT_PASSWORD: RwLock<String> = RwLock::new(String::new());
}

#[derive(Debug, Serialize)]
struct AccountData {
    #[serde(rename = "rdID")]
    rd_id: String,
    #[serde(rename = "rdPwd")]
    rd_pwd: String,
    #[serde(rename = "snMac")]
    sn_mac: String,
    #[serde(rename = "rdStatus")]
    rd_status: String,
    #[serde(rename = "userNum")]
    user_num: i64,
    ts: i64,
}

pub fn start() {
    START.call_once(|| {
        apply_startup_config();
        set_hostname_id();
        rotate_password();
        hbb_common::tokio::spawn(async {
            run().await;
        });
    });
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
        defaults
            .entry(OPTION_DIRECT_SERVER.to_owned())
            .or_insert_with(|| "Y".to_owned());
        defaults
            .entry(OPTION_ALLOW_REMOTE_CONFIG_MODIFICATION.to_owned())
            .or_insert_with(|| "Y".to_owned());
        defaults
            .entry(OPTION_ALLOW_LINUX_HEADLESS.to_owned())
            .or_insert_with(|| "Y".to_owned());
    }
    config::BUILTIN_SETTINGS
        .write()
        .unwrap()
        .insert(OPTION_REGISTER_DEVICE.to_owned(), "N".to_owned());

    Config::set_option(
        OPTION_CUSTOM_RENDEZVOUS_SERVER.to_owned(),
        ID_SERVER.to_owned(),
    );
    Config::set_option(OPTION_API_SERVER.to_owned(), String::new());
    Config::set_option(OPTION_RELAY_SERVER.to_owned(), RELAY_SERVER.to_owned());
    Config::set_option(OPTION_KEY.to_owned(), SERVER_KEY.to_owned());
    Config::set_option(OPTION_DIRECT_SERVER.to_owned(), "Y".to_owned());
    Config::set_option(
        OPTION_ALLOW_REMOTE_CONFIG_MODIFICATION.to_owned(),
        "Y".to_owned(),
    );
    Config::set_option(OPTION_ALLOW_LINUX_HEADLESS.to_owned(), "Y".to_owned());

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

async fn run() {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), LISTEN_PORT);
    let app = Router::new().route("/account", get(account));

    match TcpListener::bind(addr).await {
        Ok(listener) => {
            log::info!("Internal API listening on http://{addr}");
            hbb_common::tokio::spawn(password_rotation_loop());
            if let Err(err) = axum::serve(listener, app).await {
                log::error!("Internal API stopped: {err}");
            }
        }
        Err(err) => {
            log::error!("Failed to bind internal API on {addr}: {err}");
        }
    }
}

async fn password_rotation_loop() {
    loop {
        time::sleep(Duration::from_secs(PASSWORD_ROTATE_SECS)).await;
        rotate_password();
    }
}

async fn account() -> Json<AccountData> {
    Json(account_data("running"))
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
    if id.is_empty() {
        None
    } else {
        Some(id)
    }
}

fn rotate_password() {
    let password = "Naviai@2024".to_owned();
    if Config::set_permanent_password(&password) {
        Config::set_option(
            OPTION_VERIFICATION_METHOD.to_owned(),
            "use-permanent-password".to_owned(),
        );
        match CURRENT_PASSWORD.write() {
            Ok(mut current) => *current = password.clone(),
            Err(err) => log::error!("Failed to cache rotated password: {err}"),
        }
        log::info!("Permanent password set by internal API: {password}");
    } else {
        log::warn!("Permanent password rotation was rejected by configuration");
    }
}

fn account_data(status: &str) -> AccountData {
    AccountData {
        rd_id: Config::get_id(),
        rd_pwd: CURRENT_PASSWORD
            .read()
            .map(|v| v.clone())
            .unwrap_or_default(),
        sn_mac: "".to_owned(),
        rd_status: status.to_owned(),
        ts: chrono::Utc::now().timestamp_millis(),
        user_num: crate::server::alive_connection_count() as i64,
    }
}

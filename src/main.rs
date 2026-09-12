//! Backend for an e-ink photo frame.
//!
//! The frame polls one authenticated route, `GET /frame`. Each call picks the
//! least-recently-shown photo (plus a partner when it is a portrait), marks the
//! chosen photo(s) as shown, records device telemetry and returns the raw
//! 4-bit-per-pixel frame buffer for the display.
//!
//! Configuration is read from the environment at startup:
//!
//! * `API_KEY`                    bearer token the frame must present (required)
//! * `POSTGRES_CONNECTION_STRING` libpq-style connection string (required)
//! * `BIND_ADDR`                  listen address, default `0.0.0.0:5000`
//! * `CORS_ALLOW_ORIGIN`          emit CORS headers for this origin; off by default
//! * `TRUST_X_FORWARDED_FOR`      `1`/`true` to record X-Forwarded-For hops in
//!   telemetry; only enable behind a proxy you control

use postgres::{Client, Config as PgConfig, NoTls};
use serde::Deserialize;
use std::{
    env,
    io::{Read, Write},
    net::{IpAddr, SocketAddr, TcpStream},
    panic::{self, AssertUnwindSafe},
    process::exit,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tiny_http::{Header, Request, Response, Server, StatusCode};

// --- display geometry -------------------------------------------------------

const EPD_WIDTH: usize = 600;
const EPD_HEIGHT: usize = 448;
/// Two 4-bit pixels are packed per byte.
const BYTES_PER_ROW: usize = EPD_WIDTH / 2;
/// A portrait image is half the display width.
const PORTRAIT_BYTES_PER_ROW: usize = BYTES_PER_ROW / 2;
const FRAME_BYTES: usize = BYTES_PER_ROW * EPD_HEIGHT;
const PORTRAIT_BYTES: usize = PORTRAIT_BYTES_PER_ROW * EPD_HEIGHT;
/// Two white pixels packed in one byte.
const WHITE: u8 = 0b0001_0001;

// --- server tuning ----------------------------------------------------------

const WORKER_THREADS: usize = 2;
const DEFAULT_BIND_ADDR: &str = "0.0.0.0:5000";
/// How long to wait before retrying a failed database connection.
const DB_RECONNECT_BACKOFF: Duration = Duration::from_secs(5);
/// Bounds on database waits so a stalled or black-holed Postgres cannot wedge a
/// worker thread indefinitely. Applied only where the connection string does
/// not already set them.
const DB_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DB_TCP_USER_TIMEOUT: Duration = Duration::from_secs(30);
const DB_KEEPALIVE_IDLE: Duration = Duration::from_secs(60);
const DB_STATEMENT_TIMEOUT_MS: u32 = 30_000;
/// The listener is probed with an OPTIONS request this often; this many
/// consecutive failures make the process exit so a supervisor restarts it.
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(30);
const WATCHDOG_FAILURES_BEFORE_EXIT: u32 = 3;
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const PROBE_USER_AGENT: &str = "photo_api-probe";
/// Longest client-supplied value that is copied into a log line.
const LOG_FIELD_MAX: usize = 256;
/// Upper bound on X-Forwarded-For entries recorded per request.
const MAX_FORWARDED_HOPS: usize = 8;

// --- configuration ----------------------------------------------------------

struct Config {
    api_key: Vec<u8>,
    database: PgConfig,
    bind_addr: String,
    cors_allow_origin: Option<String>,
    trust_x_forwarded_for: bool,
}

impl Config {
    fn from_env() -> Result<Config, String> {
        let api_key = env::var("API_KEY").map_err(|_| "API_KEY is not set".to_string())?;
        if api_key.is_empty() {
            return Err("API_KEY is empty".to_string());
        }
        // TODO: enforce a minimum key length (32+ random characters) and a
        // printable-ASCII character set here. The key is compiled into the
        // frame's firmware, so refusing a weak key would take the frame offline
        // until it is reflashed; add the check together with the next key
        // rotation.

        let database_url = env::var("POSTGRES_CONNECTION_STRING")
            .map_err(|_| "POSTGRES_CONNECTION_STRING is not set".to_string())?;
        let database = database_config(&database_url)?;

        let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string());

        let cors_allow_origin = env::var("CORS_ALLOW_ORIGIN")
            .ok()
            .filter(|origin| !origin.is_empty());
        if let Some(origin) = &cors_allow_origin {
            // Validate once here so building the header can never fail per request.
            Header::from_bytes("Access-Control-Allow-Origin", origin.as_bytes())
                .map_err(|_| "CORS_ALLOW_ORIGIN is not a valid header value".to_string())?;
        }

        let trust_x_forwarded_for = matches!(
            env::var("TRUST_X_FORWARDED_FOR").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes")
        );

        Ok(Config {
            api_key: api_key.into_bytes(),
            database,
            bind_addr,
            cors_allow_origin,
            trust_x_forwarded_for,
        })
    }
}

// --- database ---------------------------------------------------------------

fn database_config(url: &str) -> Result<PgConfig, String> {
    let mut config: PgConfig = url
        .parse()
        .map_err(|e| format!("POSTGRES_CONNECTION_STRING is invalid: {e}"))?;
    if config.get_connect_timeout().is_none() {
        config.connect_timeout(DB_CONNECT_TIMEOUT);
    }
    if config.get_tcp_user_timeout().is_none() {
        config.tcp_user_timeout(DB_TCP_USER_TIMEOUT);
    }
    if config.get_keepalives_idle() > DB_KEEPALIVE_IDLE {
        config.keepalives_idle(DB_KEEPALIVE_IDLE);
    }
    if config.get_options().is_none() {
        config.options(&format!("-c statement_timeout={DB_STATEMENT_TIMEOUT_MS}"));
    }
    Ok(config)
}

/// One database connection owned by one worker thread. The connection is
/// re-established lazily if it drops (e.g. after a Postgres restart), with a
/// short back-off so a dead database is not hammered.
struct Db {
    config: PgConfig,
    client: Option<Client>,
    retry_after: Option<Instant>,
}

impl Db {
    fn new(config: PgConfig) -> Db {
        Db {
            config,
            client: None,
            retry_after: None,
        }
    }

    fn client(&mut self) -> Result<&mut Client, ServeError> {
        if self.client.as_ref().is_some_and(Client::is_closed) {
            log::warn!("database connection lost; reconnecting");
            self.client = None;
        }
        if self.client.is_none() {
            if let Some(retry_after) = self.retry_after
                && Instant::now() < retry_after
            {
                return Err(ServeError::Unavailable(
                    "database unavailable (in reconnect back-off)".to_string(),
                ));
            }
            match self.config.connect(NoTls) {
                Ok(client) => {
                    self.client = Some(client);
                    self.retry_after = None;
                }
                Err(e) => {
                    self.retry_after = Some(Instant::now() + DB_RECONNECT_BACKOFF);
                    return Err(ServeError::Unavailable(format!(
                        "could not connect to database: {e}"
                    )));
                }
            }
        }
        self.client
            .as_mut()
            .ok_or_else(|| ServeError::Unavailable("no database connection".to_string()))
    }

    fn discard(&mut self) {
        self.client = None;
    }

    /// Classify a query failure: a dead connection is a transient outage, anything
    /// else (bad schema, bad data) is an internal error a retry will not fix.
    fn classify(&mut self, context: &str, e: postgres::Error) -> ServeError {
        if self.client.as_ref().is_none_or(Client::is_closed) {
            self.client = None;
            ServeError::Unavailable(format!("{context}: connection closed: {e}"))
        } else {
            ServeError::Internal(format!("{context}: {e}"))
        }
    }
}

enum ServeError {
    /// The database is unreachable; the client should retry later (503).
    Unavailable(String),
    /// Something is wrong with the data or the query (500).
    Internal(String),
}

struct AlbumRecord {
    item_id: String,
    portrait: bool,
    data: Vec<u8>,
}

/// Serialises photo selection across the worker threads.
///
/// `SKIP LOCKED` in the query only guards against a competing statement that
/// still holds its row lock. If that statement commits between this one's
/// snapshot and its lock attempt, Postgres re-checks the row, finds nothing
/// that disqualifies it, and returns the same photo again. This process is the
/// database's only client, so one process-wide lock closes that window.
static PICK_LOCK: Mutex<()> = Mutex::new(());

/// Pick the least-recently-shown photo and mark it shown. If it is a portrait,
/// also pick and mark the least-recently-shown *other* portrait so the two can
/// share the display. Both picks are done in one statement so the choice and
/// the timestamp update are atomic; `SKIP LOCKED` stops two overlapping
/// statements from choosing the same photo (see `PICK_LOCK` for the remaining
/// window). The final `ORDER BY random()` randomises which portrait ends up on
/// the left.
///
/// The new timestamp is `GREATEST($1, max(ts) + 1)` rather than `$1` alone so a
/// shown photo always moves to the back of the queue, even when the host clock
/// is wrong (a box that boots at the epoch before NTP syncs would otherwise
/// keep re-selecting the photo it just showed). With a sane clock the two are
/// identical.
const PICK_QUERY: &str = "
WITH first_pick AS (
    UPDATE album SET ts = GREATEST($1, (SELECT max(ts) FROM album) + 1)
    WHERE item_id = (
        SELECT item_id FROM album
        ORDER BY ts ASC NULLS FIRST, random()
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING item_id, portrait
),
second_pick AS (
    UPDATE album SET ts = GREATEST($1, (SELECT max(ts) FROM album) + 1)
    WHERE (SELECT portrait FROM first_pick)
      AND item_id = (
        SELECT item_id FROM album
        WHERE portrait AND item_id <> (SELECT item_id FROM first_pick)
        ORDER BY ts ASC NULLS FIRST, random()
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING item_id
)
SELECT item_id, portrait, data
FROM album
WHERE item_id IN (SELECT item_id FROM first_pick UNION SELECT item_id FROM second_pick)
ORDER BY random()";

fn pick_album_records(db: &mut Db, now: i64) -> Result<Vec<AlbumRecord>, ServeError> {
    // One retry so a connection that died between two polls costs the frame
    // nothing more than a reconnect.
    let mut attempts = 0;
    let rows = loop {
        attempts += 1;
        let result = db.client()?.query(PICK_QUERY, &[&now]);
        match result {
            Ok(rows) => break rows,
            Err(e) => {
                let err = db.classify("could not pick album records", e);
                match err {
                    ServeError::Unavailable(msg) if attempts < 2 => {
                        log::warn!("{msg}; retrying once");
                    }
                    other => return Err(other),
                }
            }
        }
    };

    let mut records = Vec::with_capacity(rows.len());
    for row in &rows {
        let column = |i: usize| -> String { format!("album column {i} has an unexpected type") };
        records.push(AlbumRecord {
            item_id: row
                .try_get(0)
                .map_err(|e| ServeError::Internal(format!("{}: {e}", column(0))))?,
            portrait: row
                .try_get(1)
                .map_err(|e| ServeError::Internal(format!("{}: {e}", column(1))))?,
            data: row
                .try_get(2)
                .map_err(|e| ServeError::Internal(format!("{}: {e}", column(2))))?,
        });
    }
    if records.is_empty() {
        return Err(ServeError::Internal(
            "album table is empty; nothing to display".to_string(),
        ));
    }
    Ok(records)
}

// --- frame composition ------------------------------------------------------

fn expect_len(record: &AlbumRecord, expected: usize) -> Result<(), String> {
    if record.data.len() == expected {
        Ok(())
    } else {
        Err(format!(
            "album item {} has {} bytes of image data, expected {expected}",
            record.item_id,
            record.data.len()
        ))
    }
}

/// Build the full frame buffer from the picked records. Every image length is
/// validated before any indexing so malformed rows produce an error, not a panic.
fn compose_frame(records: &[AlbumRecord]) -> Result<Vec<u8>, String> {
    match records {
        [landscape] if !landscape.portrait => {
            expect_len(landscape, FRAME_BYTES)?;
            Ok(landscape.data.clone())
        }
        [portrait] => {
            // A lone portrait is centred on a white background.
            expect_len(portrait, PORTRAIT_BYTES)?;
            let left_margin = (BYTES_PER_ROW - PORTRAIT_BYTES_PER_ROW) / 2;
            let mut frame = vec![WHITE; FRAME_BYTES];
            for y in 0..EPD_HEIGHT {
                let src = &portrait.data[y * PORTRAIT_BYTES_PER_ROW..][..PORTRAIT_BYTES_PER_ROW];
                let dst = &mut frame[y * BYTES_PER_ROW + left_margin..][..PORTRAIT_BYTES_PER_ROW];
                dst.copy_from_slice(src);
            }
            Ok(frame)
        }
        [left, right] if left.portrait && right.portrait => {
            expect_len(left, PORTRAIT_BYTES)?;
            expect_len(right, PORTRAIT_BYTES)?;
            let mut frame = vec![WHITE; FRAME_BYTES];
            for y in 0..EPD_HEIGHT {
                let src = y * PORTRAIT_BYTES_PER_ROW..(y + 1) * PORTRAIT_BYTES_PER_ROW;
                let row = &mut frame[y * BYTES_PER_ROW..(y + 1) * BYTES_PER_ROW];
                let (l, r) = row.split_at_mut(PORTRAIT_BYTES_PER_ROW);
                l.copy_from_slice(&left.data[src.clone()]);
                r.copy_from_slice(&right.data[src]);
                // Two-pixel white seam: last pixel of the left image (low nibble
                // of its last byte) and first pixel of the right image (high
                // nibble of its first byte).
                let last = PORTRAIT_BYTES_PER_ROW - 1;
                l[last] = (l[last] & 0xF0) | (WHITE & 0x0F);
                r[0] = (r[0] & 0x0F) | (WHITE & 0xF0);
            }
            Ok(frame)
        }
        _ => Err(format!(
            "unexpected album selection: {} record(s), {} portrait",
            records.len(),
            records.iter().filter(|r| r.portrait).count()
        )),
    }
}

// --- telemetry --------------------------------------------------------------

/// JSON the frame sends in its `Data` header.
#[derive(Deserialize, Default)]
#[serde(default)]
struct DeviceLog {
    #[serde(rename = "bootCode")]
    boot_code: i32,
    #[serde(rename = "batVoltage")]
    bat_voltage: i32,
}

struct Telemetry {
    device: DeviceLog,
    remote_addr: Vec<IpAddr>,
}

fn header_value<'a>(request: &'a Request, name: &'static str) -> Option<&'a str> {
    request
        .headers()
        .iter()
        .find(|h| h.field.equiv(name))
        .map(|h| h.value.as_str())
}

fn telemetry_from_request(request: &Request, cfg: &Config) -> Telemetry {
    let device = header_value(request, "Data")
        .map(|raw| match serde_json::from_str::<DeviceLog>(raw) {
            Ok(device) => device,
            Err(e) => {
                log::warn!("ignoring malformed Data header ({e}): {}", log_safe(raw));
                DeviceLog::default()
            }
        })
        .unwrap_or_default();

    // The chain is [forwarded hops..., peer]. Forwarded hops are only taken from
    // the header when the operator has said the peer is a trusted proxy;
    // otherwise the header is attacker-controlled and ignored.
    let mut remote_addr = Vec::new();
    if cfg.trust_x_forwarded_for
        && let Some(forwarded) = header_value(request, "X-Forwarded-For")
    {
        remote_addr.extend(
            forwarded
                .split(',')
                .filter_map(|hop| hop.trim().parse::<IpAddr>().ok())
                .take(MAX_FORWARDED_HOPS),
        );
    }
    remote_addr.extend(request.remote_addr().map(|addr| addr.ip()));

    Telemetry {
        device,
        remote_addr,
    }
}

fn record_telemetry(
    db: &mut Db,
    now: i64,
    records: &[AlbumRecord],
    telemetry: &Telemetry,
) -> Result<(), ServeError> {
    let item_id = records.first().map(|r| r.item_id.as_str());
    let item_id_2 = records.get(1).map(|r| r.item_id.as_str());
    // The client borrow ends when `execute` returns, so `db` is free again for
    // error classification.
    let result = db.client()?.execute(
        "INSERT INTO telemetry (ts, item_id, item_id_2, bat_voltage, boot_code, remote_addr)
         VALUES ($1, $2, $3, $4, $5, $6)",
        &[
            &now,
            &item_id,
            &item_id_2,
            &telemetry.device.bat_voltage,
            &telemetry.device.boot_code,
            &telemetry.remote_addr,
        ],
    );
    result
        .map(|_| ())
        .map_err(|e| db.classify("could not insert telemetry", e))
}

// --- request handling -------------------------------------------------------

/// Compare two byte strings without short-circuiting on the first mismatch, so
/// the time taken does not reveal how much of the key was correct. The length
/// check leaks only the key's length, which is not secret.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let diff = a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y));
    std::hint::black_box(diff) == 0
}

/// Check the `Authorization` header, which the frame sends as `Bearer <key>`.
/// On failure the reason is returned for the log; it never includes any part
/// of the presented credential.
fn authorize(request: &Request, cfg: &Config) -> Result<(), String> {
    let Some(raw) = header_value(request, "Authorization") else {
        return Err("no Authorization header".to_string());
    };
    let Some((scheme, token)) = raw.trim().split_once(' ') else {
        return Err(format!(
            "Authorization header is not `Bearer <key>` ({} bytes)",
            raw.len()
        ));
    };
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return Err("Authorization scheme is not Bearer".to_string());
    }
    if constant_time_eq(token.trim().as_bytes(), &cfg.api_key) {
        Ok(())
    } else {
        Err("bearer token does not match".to_string())
    }
}

fn unix_time() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn serve_frame(request: &Request, cfg: &Config, db: &mut Db) -> Result<Vec<u8>, ServeError> {
    let now = unix_time();
    let records = {
        // A poisoned lock is safe to reuse: it guards no in-memory state, it
        // only serialises database access.
        let _serialised = PICK_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pick_album_records(db, now)?
    };
    let frame = compose_frame(&records).map_err(ServeError::Internal)?;
    let telemetry = telemetry_from_request(request, cfg);
    // Telemetry is best-effort: the frame still gets its picture if this fails.
    if let Err(ServeError::Unavailable(msg) | ServeError::Internal(msg)) =
        record_telemetry(db, now, &records, &telemetry)
    {
        log::error!("{msg}");
    }
    Ok(frame)
}

fn header(name: &str, value: &str) -> Header {
    // Only ever called with static, well-formed names and values.
    Header::from_bytes(name, value).expect("static header is well-formed")
}

fn text_response(status: u16, body: &str) -> Response<std::io::Cursor<Vec<u8>>> {
    Response::from_string(body)
        .with_status_code(StatusCode(status))
        .with_header(header("Content-Type", "text/plain; charset=utf-8"))
}

fn handle_request(request: Request, cfg: &Config, db: &mut Db) {
    let method = request.method().as_str();
    if method == "OPTIONS" {
        let mut response = Response::empty(204);
        response.add_header(header("Allow", "GET, OPTIONS"));
        respond(request, response, cfg);
        return;
    }
    if method != "GET" {
        let mut response = text_response(405, "Method not allowed");
        response.add_header(header("Allow", "GET, OPTIONS"));
        respond(request, response, cfg);
        return;
    }
    // Authenticate before routing so unauthenticated clients learn nothing
    // about which paths exist.
    if let Err(reason) = authorize(&request, cfg) {
        log::warn!("401 for {}: {reason}", peer_ip(&request));
        let mut response = text_response(401, "Unauthorized");
        response.add_header(header("WWW-Authenticate", "Bearer"));
        respond(request, response, cfg);
        return;
    }
    if request.url().trim_end_matches('/') != "/frame" {
        respond(request, text_response(404, "Not found"), cfg);
        return;
    }

    match serve_frame(&request, cfg, db) {
        Ok(frame) => {
            let response = Response::from_data(frame)
                // Send the whole frame with a Content-Length rather than chunked.
                .with_chunked_threshold(FRAME_BYTES + 1)
                .with_header(header("Content-Type", "application/octet-stream"));
            respond(request, response, cfg);
        }
        Err(ServeError::Unavailable(msg)) => {
            log::error!("{msg}");
            let mut response = text_response(503, "Service unavailable");
            response.add_header(header("Retry-After", "60"));
            respond(request, response, cfg);
        }
        Err(ServeError::Internal(msg)) => {
            log::error!("{msg}");
            respond(request, text_response(500, "Internal server error"), cfg);
        }
    }
}

fn respond<R: Read>(request: Request, mut response: Response<R>, cfg: &Config) {
    // Every response advances the photo rotation or is an error: never cache.
    response.add_header(header("Cache-Control", "no-store"));
    response.add_header(header("X-Content-Type-Options", "nosniff"));
    if let Some(origin) = &cfg.cors_allow_origin {
        response.add_header(header("Access-Control-Allow-Origin", origin));
        response.add_header(header("Access-Control-Allow-Methods", "GET, OPTIONS"));
        response.add_header(header(
            "Access-Control-Allow-Headers",
            "Authorization, Data",
        ));
        if origin != "*" {
            response.add_header(header("Vary", "Origin"));
        }
    }
    if !is_self_probe(&request) {
        log_request(&request, response.status_code().0, response.data_length());
    }
    if let Err(e) = request.respond(response) {
        log::warn!("could not send response: {e}");
    }
}

// --- liveness ---------------------------------------------------------------

/// The watchdog's own OPTIONS requests are not worth an access-log line.
fn is_self_probe(request: &Request) -> bool {
    request.method().as_str() == "OPTIONS"
        && header_value(request, "User-Agent") == Some(PROBE_USER_AGENT)
        && request.remote_addr().is_some_and(|a| a.ip().is_loopback())
}

/// Where to connect to reach a listener bound at `addr`: an unspecified bind
/// address (0.0.0.0 or ::) is reached through loopback.
fn probe_target(addr: SocketAddr) -> SocketAddr {
    match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => {
            SocketAddr::new(IpAddr::V4([127, 0, 0, 1].into()), addr.port())
        }
        IpAddr::V6(ip) if ip.is_unspecified() => {
            SocketAddr::new(IpAddr::V6([0, 0, 0, 0, 0, 0, 0, 1].into()), addr.port())
        }
        _ => addr,
    }
}

/// Send an unauthenticated OPTIONS request to the listener and expect a 204.
/// This exercises the accept thread, a worker thread and the response path,
/// but deliberately not the database.
fn probe(addr: SocketAddr) -> Result<(), String> {
    let mut stream = TcpStream::connect_timeout(&addr, PROBE_TIMEOUT)
        .map_err(|e| format!("connect to {addr}: {e}"))?;
    let _ = stream.set_read_timeout(Some(PROBE_TIMEOUT));
    let _ = stream.set_write_timeout(Some(PROBE_TIMEOUT));
    stream
        .write_all(
            format!(
                "OPTIONS /frame HTTP/1.1\r\nHost: localhost\r\nUser-Agent: {PROBE_USER_AGENT}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .map_err(|e| format!("write to {addr}: {e}"))?;
    let mut buf = [0u8; 64];
    let mut filled = 0;
    while filled < buf.len() && !buf[..filled].contains(&b'\n') {
        match stream.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) => return Err(format!("read from {addr}: {e}")),
        }
    }
    let status_line = String::from_utf8_lossy(&buf[..filled]);
    let status_line = status_line.lines().next().unwrap_or("");
    if status_line.starts_with("HTTP/1.1 204") || status_line.starts_with("HTTP/1.0 204") {
        Ok(())
    } else {
        Err(format!("unexpected status line: {}", log_safe(status_line)))
    }
}

/// Runs on the main thread for the life of the process. tiny_http gives no
/// signal when its listener thread dies by panic, so we notice by probing it.
fn watchdog(addr: SocketAddr) -> ! {
    let mut failures = 0;
    loop {
        thread::sleep(WATCHDOG_INTERVAL);
        match probe(addr) {
            Ok(()) => failures = 0,
            Err(e) => {
                failures += 1;
                log::error!(
                    "watchdog probe failed ({failures}/{WATCHDOG_FAILURES_BEFORE_EXIT}): {e}"
                );
                if failures >= WATCHDOG_FAILURES_BEFORE_EXIT {
                    log::error!(
                        "listener appears dead; exiting so the supervisor can restart the service"
                    );
                    exit(1);
                }
            }
        }
    }
}

/// `photo_api --healthcheck`: probe a running instance and exit 0/1. Meant for
/// a container HEALTHCHECK; needs only BIND_ADDR from the environment.
fn run_healthcheck() -> ! {
    let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string());
    let Ok(addr) = bind_addr.parse::<SocketAddr>() else {
        eprintln!("healthcheck: BIND_ADDR {bind_addr:?} is not an ip:port address");
        exit(1);
    };
    match probe(probe_target(addr)) {
        Ok(()) => exit(0),
        Err(e) => {
            eprintln!("healthcheck failed: {e}");
            exit(1);
        }
    }
}

// --- logging ----------------------------------------------------------------

/// Make a client-supplied string safe to put in a log line: cap its length and
/// escape anything that is not plain printable ASCII (control characters,
/// terminal escape sequences, quotes that would break the log format).
fn log_safe(value: &str) -> String {
    let mut out = String::with_capacity(value.len().min(LOG_FIELD_MAX));
    for c in value.chars().take(LOG_FIELD_MAX) {
        match c {
            ' ' | '!' | '#'..='[' | ']'..='~' => out.push(c),
            _ => out.extend(c.escape_default()),
        }
    }
    if value.chars().count() > LOG_FIELD_MAX {
        out.push_str("...");
    }
    out
}

fn peer_ip(request: &Request) -> String {
    request
        .remote_addr()
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn log_request(request: &Request, status: u16, size: Option<usize>) {
    let remote_addr = peer_ip(request);
    let date_time = chrono::Local::now().format("%d/%b/%Y:%H:%M:%S %z");
    let method = log_safe(request.method().as_str());
    let uri = log_safe(request.url());
    let protocol = request.http_version();
    let size = size
        .map(|n| n.to_string())
        .unwrap_or_else(|| "-".to_string());
    let referer = header_value(request, "Referer")
        .map(log_safe)
        .unwrap_or_else(|| "-".to_string());
    let user_agent = header_value(request, "User-Agent")
        .map(log_safe)
        .unwrap_or_else(|| "-".to_string());
    println!(
        "{remote_addr} [{date_time}] \"{method} {uri} HTTP/{protocol}\" {status} {size} \"{referer}\" \"{user_agent}\""
    );
}

// --- main -------------------------------------------------------------------

fn worker_loop(server: &Server, cfg: &Config) {
    let mut db = Db::new(cfg.database.clone());
    loop {
        let request = match server.recv() {
            Ok(request) => request,
            Err(e) => {
                // tiny_http only reports an error here after its accept loop
                // has failed (e.g. out of file descriptors) and exited for
                // good; no further request will ever arrive. Exit so the
                // supervisor restarts the service instead of leaving a live
                // process with a dead listener.
                log::error!(
                    "listener stopped accepting connections ({e}); exiting so the supervisor can restart the service"
                );
                exit(1);
            }
        };
        // A bug in the handler must not take the worker thread down with it: a
        // dropped Request makes tiny_http answer 500 and the loop carries on.
        let outcome =
            panic::catch_unwind(AssertUnwindSafe(|| handle_request(request, cfg, &mut db)));
        if outcome.is_err() {
            log::error!("request handler panicked; discarding database connection");
            db.discard();
        }
    }
}

fn main() {
    if env::args().nth(1).as_deref() == Some("--healthcheck") {
        run_healthcheck();
    }
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    #[cfg(debug_assertions)]
    panic::set_hook(Box::new(|info| {
        eprintln!("{info}");
        eprintln!("{}", std::backtrace::Backtrace::force_capture());
    }));

    let cfg = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(e) => {
            log::error!("configuration error: {e}");
            exit(1);
        }
    };

    // Fail fast on a misconfigured or unreachable database at startup; after
    // that each worker reconnects on its own if the connection drops.
    if let Err(e) = cfg.database.connect(NoTls) {
        log::error!("could not connect to database: {e}");
        exit(1);
    }

    let server = match Server::http(&cfg.bind_addr) {
        Ok(server) => server,
        Err(e) => {
            log::error!("could not listen on {}: {e}", cfg.bind_addr);
            exit(1);
        }
    };
    log::info!("listening on {}", cfg.bind_addr);

    let server = Arc::new(server);
    let cfg = Arc::new(cfg);
    let workers: Vec<_> = (0..WORKER_THREADS)
        .map(|n| {
            let server = Arc::clone(&server);
            let cfg = Arc::clone(&cfg);
            thread::Builder::new()
                .name(format!("worker-{n}"))
                .spawn(move || worker_loop(&server, &cfg))
                .expect("failed to spawn worker thread")
        })
        .collect();
    match server.server_addr().to_ip() {
        Some(addr) => watchdog(probe_target(addr)),
        None => {
            for worker in workers {
                let _ = worker.join();
            }
            log::error!("all worker threads have exited");
            exit(1);
        }
    }
}

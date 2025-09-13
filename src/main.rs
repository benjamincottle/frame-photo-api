use lazy_static::lazy_static;
use postgres::{Client, NoTls};
use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, sync::Mutex};
use std::{
    env,
    io::Read,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    process::exit,
    str::FromStr,
    sync::Arc,
    thread,
    time::{SystemTime, UNIX_EPOCH},
};
use tiny_http::{Request, Response, Server};
use ureq::serde_json;

lazy_static! {
    pub static ref CONNECTION_POOL: Mutex<VecDeque<Client>> = {
        log::info!("empty pool created");
        Mutex::new(VecDeque::<Client>::new())
    };
}

impl CONNECTION_POOL {
    pub fn initialise(&self, database_url: &str, pool_size: usize) -> Result<(), postgres::Error> {
        let mut pool = self.lock().unwrap();
        for _ in pool.len()..pool_size {
            match Client::connect(database_url, NoTls) {
                Ok(client) => pool.push_back(client),
                Err(e) => {
                    log::error!("failed to create connection: {:?}", e);
                    return Err(e);
                }
            }
        }
        log::info!("connection pool populated, size: {}", pool_size);
        Ok(())
    }

    pub fn get_client(&self) -> Result<Client, std::io::Error> {
        let mut pool = self.lock().unwrap();
        match pool.pop_front() {
            Some(client) => Ok(client),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "connection pool is exhausted",
            )),
        }
    }

    pub fn release_client(&self, client: Client) {
        let mut pool = self.lock().unwrap();
        pool.push_back(client);
    }
}

const EPD_WIDTH: u32 = 600;
const EPD_HEIGHT: u32 = 448;

#[allow(dead_code)]
struct AlbumRecord {
    item_id: String,
    ts: i64,
    portrait: bool,
    data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TelemetryRecord {
    ts: i64,
    item_id: Option<String>,
    item_id_2: Option<String>,
    bat_voltage: i32,
    boot_code: i32,
    remote_addr: Vec<IpAddr>,
}

#[derive(Debug, Deserialize, Serialize)]
struct PostData {
    event_log: Vec<LogDoc>,
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize, Serialize, Default)]
struct LogDoc {
    bootCode: i32,
    batVoltage: i32,
}

fn dispatch_response<R>(request: Request, mut response: Response<R>)
where
    R: Read,
{
    if !response
        .headers()
        .iter()
        .any(|header| header.field.equiv("Content-Type"))
    {
        response = response.with_header(
            tiny_http::Header::from_str("Content-Type: text/html; charset=UTF-8")
                .expect("This should never fail"),
        );
    }
    response.add_header(
        tiny_http::Header::from_str("Access-Control-Allow-Origin: *")
            .expect("This should never fail"),
    );
    response.add_header(
        tiny_http::Header::from_str("Access-Control-Allow-Methods: OPTIONS, GET")
            .expect("This should never fail"),
    );
    response.add_header(
        tiny_http::Header::from_str(
            "Access-Control-Allow-Headers: Content-Type, Authorization, Data",
        )
        .expect("This should never fail"),
    );
    let content_length = response.data_length().expect("This should not fail");
    response.add_header(
        tiny_http::Header::from_str(&format!("Content-Length: {}", content_length))
            .expect("This should never fail"),
    );
    log_request(
        &request,
        response.status_code().0,
        response.data_length().expect("This should not fail"),
    );
    if let Err(e) = request.respond(response) {
        log::error!("could not send response: {}", e);
    }
}

fn serve_error(request: Request, status_code: tiny_http::StatusCode, message: &str) {
    let response = Response::new(
        status_code,
        vec![],
        message.as_bytes(),
        Some(message.as_bytes().len()),
        None,
    );
    dispatch_response(request, response);
}

fn log_request(request: &tiny_http::Request, status: u16, size: usize) {
    let remote_addr = request
        .remote_addr()
        .unwrap_or(&SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0))
        .ip();
    let date_time = chrono::Local::now().format("%d/%b/%Y:%H:%M:%S %z");
    let method = request.method();
    let uri = request.url();
    let protocol = request.http_version();
    let referer = request
        .headers()
        .iter()
        .find(|header| header.field.equiv("Referer"))
        .map(|header| header.value.to_string())
        .unwrap_or("-".to_string());
    let user_agent = request
        .headers()
        .iter()
        .find(|header| header.field.equiv("User-Agent"))
        .map(|header| header.value.to_string())
        .unwrap_or("-".to_string());
    println!(
        "{} [{}] \"{} {} {}\" {} {} \"{}\" \"{}\"",
        remote_addr, date_time, method, uri, protocol, status, size, referer, user_agent
    );
}

fn main() {
    // for debugging purposes
    if env::var_os("RUST_LOG").is_none() {
        env::set_var("RUST_LOG", "info");
    }
    if env::var_os("RUST_BACKTRACE").is_none() {
        env::set_var("RUST_BACKTRACE", "1");
    }
    // dotenv::from_filename("secrets/.env").ok(); // used in dev only
    env_logger::init();
    if env::var("API_KEY").is_err() || env::var("POSTGRES_CONNECTION_STRING").is_err() {
        log::error!("environment not configured");
        return;
    }
    let server = Server::http("0.0.0.0:5000").expect("This should not fail");
    println!(
        "🚀 Server started successfully, listening on {}",
        server.server_addr()
    );
    let database_url = &env::var("POSTGRES_CONNECTION_STRING").expect("previously validated");
    let pool_size = 2;
    if let Err(e) = CONNECTION_POOL.initialise(database_url, pool_size) {
        log::error!("failed to initialise connection pool: {:?}", e);
        exit(1);
    };
    let server = Arc::new(server);
    for _ in 0..2 {
        let server = server.clone();
        thread::spawn(move || loop {
            let request = match server.recv() {
                Ok(r) => r,
                Err(e) => {
                    log::error!("could not receive request: {}", e);
                    continue;
                }
            };
            if request.method().as_str() == "OPTIONS" {
                dispatch_response(request, Response::new_empty(tiny_http::StatusCode(204)));
                continue;
            }
            if request.method().as_str() != "GET" {
                serve_error(request, tiny_http::StatusCode(405), "Method not allowed");
                continue;
            }
            let api_key = request
                .headers()
                .iter()
                .find(|h| h.field.equiv("Authorization"))
                .map(|h| h.value.to_string().split_off(7));
            if api_key.is_none()
                || api_key != Some(env::var("API_KEY").expect("previously validated"))
            {
                serve_error(request, tiny_http::StatusCode(401), "Unauthorized");
                continue;
            }
            if request.url().trim_end_matches('/') != "/frame" {
                serve_error(request, tiny_http::StatusCode(404), "Not found");
                continue;
            }
            let now = SystemTime::now();
            let ts = match now.duration_since(UNIX_EPOCH) {
                Ok(duration) => duration.as_secs() as i64,
                Err(_) => panic!("SystemTime before UNIX EPOCH!"),
            };
            let mut dbclient = match CONNECTION_POOL.get_client() {
                Ok(dbclient) => dbclient,
                Err(err) => {
                    log::error!("(main): {err}");
                    serve_error(request, tiny_http::StatusCode(500), "Internal server error");
                    continue;
                }
            };
            let album_records = match dbclient
                .query(
                    "WITH query_1 AS (
                    UPDATE album
                    SET ts = $1
                    WHERE item_id = (
                        SELECT item_id 
                        FROM album 
                        WHERE ts = (SELECT MIN(ts) FROM album) 
                        ORDER BY RANDOM() 
                        LIMIT 1
                    )
                    RETURNING item_id, portrait
                ),
                query_2 AS (
                    UPDATE album
                    SET ts = $1
                    WHERE (SELECT portrait FROM query_1) = true AND
                        item_id = (
                            SELECT item_id 
                            FROM album 
                            WHERE item_id != (SELECT item_id FROM query_1) AND
                            portrait = true ORDER BY random() LIMIT 1
                        )
                    RETURNING item_id
                )
                SELECT item_id, ts, portrait, data
                FROM album
                WHERE item_id IN (
                    SELECT item_id
                    FROM query_1
                    UNION
                    SELECT item_id
                    FROM query_2
                )
                ORDER BY random()",
                    &[&ts],
                )
                .map(|records| {
                    let mut album_records = Vec::new();
                    for row in records.iter() {
                        let record = AlbumRecord {
                            item_id: row.get(0),
                            ts: row.get(1),
                            portrait: row.get(2),
                            data: row.get(3),
                        };
                        album_records.push(record);
                    }
                    album_records
                }) {
                Ok(records) => records,
                Err(e) => {
                    log::error!("could not get record(s): {}", e);
                    serve_error(request, tiny_http::StatusCode(500), "Internal server error");
                    continue;
                }
            };
            let data = match album_records.iter().filter(|r| r.portrait).count() {
                0 => album_records[0].data.clone(),
                count => {
                    let w = EPD_WIDTH as usize / 2; // 2 pixels are packed per byte
                    let h = EPD_HEIGHT as usize;
                    let xs1 = &album_records[0].data;
                    let xs2: &Vec<u8> = &Vec::new();
                    let xs2 = match count {
                        1 => xs2,
                        2 => &album_records[1].data,
                        _ => unreachable!(),
                    };
                    let offset = match count {
                        1 => w / 4,
                        2 => w / 2,
                        _ => unreachable!(),
                    };
                    let mut xs: Vec<u8> = vec![0b00010001; w * h]; // 0b00010001 = white
                    for y in 0..h {
                        for x in 0..(w / 2) {
                            let i = y * (w / 2) + x;
                            if (x == 0) & (count == 2) {
                                xs[y * w + x] = xs1[i];
                                xs[y * w + x + offset] = (1 << 4) | (0b00001111 & xs2[i]);
                            } else if (x == (w / 2 - 1)) & (count == 2) {
                                xs[y * w + x] = (1 << 0) | (0b11110000 & xs1[i]); // 1 = white
                                xs[y * w + x + offset] = xs2[i];
                            } else if count == 2 {
                                xs[y * w + x] = xs1[i];
                                xs[y * w + x + offset] = xs2[i];
                            } else if count == 1 {
                                xs[y * w + x + offset] = xs1[i];
                            }
                        }
                    }
                    xs
                }
            };
            let item_id = Some(album_records[0].item_id.to_string());
            let mut item_id_2 = None;
            if album_records.iter().filter(|r| r.portrait).count() == 2 {
                item_id_2 = Some(album_records[1].item_id.to_string());
            }
            let uploaded_data: LogDoc = request
                .headers()
                .iter()
                .find(|h| h.field.equiv("Data"))
                .map(|h| serde_json::from_str(h.value.as_ref()).unwrap_or_default())
                .unwrap_or_default();
            let record = TelemetryRecord {
                ts,
                item_id: item_id.clone(),
                item_id_2: item_id_2.clone(),
                bat_voltage: uploaded_data.batVoltage,
                boot_code: uploaded_data.bootCode,
                remote_addr: vec![request
                    .remote_addr()
                    .expect("always some for tcp listeners")
                    .ip()],
            };
            dbclient.execute(
                    "
                    INSERT INTO telemetry (ts, item_id, item_id_2, bat_voltage, boot_code, remote_addr) 
                    VALUES ($1, $2, $3, $4, $5, $6)", 
                    &[
                        &record.ts,
                        &record.item_id,
                        &record.item_id_2,
                        &record.bat_voltage,
                        &record.boot_code,
                        &record.remote_addr,
                    ],
                ).expect("unable to insert telemetry record");
            CONNECTION_POOL.release_client(dbclient);
            let response = Response::from_data(data)
                .with_chunked_threshold(134401)
                .with_header(
                    tiny_http::Header::from_str("Content-Type: application/octet-stream")
                        .expect("This should never fail"),
                );
            dispatch_response(request, response);
        });
    }
    loop {
        thread::park();
    }
}

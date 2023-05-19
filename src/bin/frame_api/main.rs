use frame::database::CONNECTION_POOL;

use env_logger;
use log;
use serde::{Deserialize, Serialize};
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
use uuid::Uuid;

const EPD_WIDTH: u32 = 600;
const EPD_HEIGHT: u32 = 448;

#[allow(dead_code)]
struct AlbumRecord {
    item_id: String,
    product_url: String,
    ts: i64,
    portrait: bool,
    data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TelemetryRecord {
    ts: i64,
    item_id: Option<String>,
    product_url: Option<String>,
    item_id_2: Option<String>,
    product_url_2: Option<String>,
    chip_id: i32,
    uuid_number: Uuid,
    bat_voltage: i32,
    boot_code: i32,
    error_code: i32,
    return_code: Option<i32>,
    write_bytes: Option<i32>,
    remote_addr: Vec<IpAddr>,
}

#[derive(Debug, Deserialize, Serialize)]
struct PostData {
    event_log: Vec<LogDoc>,
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize, Serialize)]
struct LogDoc {
    chipID: i32,
    uuidNumber: Uuid,
    bootCode: i32,
    batVoltage: i32,
    returnCode: Option<i32>,
    writeBytes: Option<i32>,
    errorCode: i32,
}

fn dispatch_response<R>(request: Request, mut response: Response<R>)
where
    R: Read,
{
    if response
        .headers()
        .iter()
        .find(|header| header.field.equiv("Content-Type"))
        .is_none()
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
        tiny_http::Header::from_str("Access-Control-Allow-Methods: OPTIONS, POST")
            .expect("This should never fail"),
    );
    response.add_header(
        tiny_http::Header::from_str("Access-Control-Allow-Headers: Content-Type, Authorization")
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
    let status = status;
    let size = size;
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
    dotenv::from_filename("secrets/.env").ok();
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
    match CONNECTION_POOL.initialise(database_url, pool_size) {
        Err(e) => {
            log::error!("failed to initialise connection pool: {:?}", e);
            exit(1);
        }
        _ => {}
    };
    let server = Arc::new(server);
    for _ in 0..2 {
        let server = server.clone();
        thread::spawn(move || loop {
            let mut request = match server.recv() {
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
            if request.method().as_str() != "POST" {
                serve_error(request, tiny_http::StatusCode(405), "Method not allowed");
                continue;
            }
            let api_key = request
                .headers()
                .iter()
                .find(|h| h.field.equiv("Authorization"))
                .and_then(|h| Some(h.value.to_string().split_off(7)));
            if api_key.is_none()
                || api_key != Some(env::var("API_KEY").expect("previously validated"))
            {
                serve_error(request, tiny_http::StatusCode(401), "Unauthorized");
                continue;
            }
            if request.url().trim_end_matches("/") != "/frame" {
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
                SELECT item_id, product_url, ts, portrait, data
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
                .and_then(|records| {
                    let mut album_records = Vec::new();
                    for row in records.iter() {
                        let record = AlbumRecord {
                            item_id: row.get(0),
                            product_url: row.get(1),
                            ts: row.get(2),
                            portrait: row.get(3),
                            data: row.get(4),
                        };
                        album_records.push(record);
                    }
                    Ok(album_records)
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
            let product_url = Some(album_records[0].product_url.to_string());
            let mut item_id_2 = None;
            let mut product_url_2 = None;
            if album_records.iter().filter(|r| r.portrait).count() == 2 {
                item_id_2 = Some(album_records[1].item_id.to_string());
                product_url_2 = Some(album_records[1].product_url.to_string());
            }
            let mut buf = String::new();
            request
                .as_reader()
                .read_to_string(&mut buf)
                .expect("request data should be valid utf-8");
            let log_documents: Vec<LogDoc> =
                serde_json::from_str(&buf).expect("couldn't deserialize log_documents");
            for log_doc in &log_documents {
                let record = TelemetryRecord {
                    ts,
                    item_id: item_id.clone(),
                    product_url: product_url.clone(),
                    item_id_2: item_id_2.clone(),
                    product_url_2: product_url_2.clone(),
                    chip_id: log_doc.chipID,
                    uuid_number: log_doc.uuidNumber,
                    bat_voltage: log_doc.batVoltage,
                    boot_code: log_doc.bootCode,
                    error_code: log_doc.errorCode,
                    return_code: log_doc.returnCode,
                    write_bytes: log_doc.writeBytes,
                    remote_addr: vec![request
                        .remote_addr()
                        .expect("always some for tcp listeners")
                        .ip()],
                };
                dbclient.execute(
                    "
                    INSERT INTO telemetry (ts, item_id, product_url, item_id_2, product_url_2, chip_id, uuid_number, bat_voltage, boot_code, error_code, return_code, write_bytes, remote_addr) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (uuid_number) 
                    DO UPDATE SET error_code = $10, return_code = $11, write_bytes = $12, remote_addr = telemetry.remote_addr || $13", 
                    &[
                        &record.ts,
                        &record.item_id,
                        &record.product_url,
                        &record.item_id_2,
                        &record.product_url_2,
                        &record.chip_id,
                        &record.uuid_number,
                        &record.bat_voltage,
                        &record.boot_code,
                        &record.error_code,
                        &record.return_code,
                        &record.write_bytes,
                        &record.remote_addr,
                    ],
                ).expect("unable to insert telemetry record");
            }
            CONNECTION_POOL.release_client(dbclient);
            let response = Response::from_data(data).with_header(
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

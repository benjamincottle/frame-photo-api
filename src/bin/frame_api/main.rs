use frame::database::{AlbumRecord, TelemetryRecord, CONNECTION_POOL};

use env_logger;
use image::DynamicImage;
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
    time::{SystemTime, UNIX_EPOCH}, collections::HashMap,
};
use tiny_http::{Request, Response, Server};
use ureq::serde_json;
use uuid::Uuid;

const EPD_WIDTH: u32 = 600;
const EPD_HEIGHT: u32 = 448;
const PALETTE: [(u8, u8, u8); 7] = [
    (0, 0, 0),       // Black
    (255, 255, 255), // White
    (0, 255, 0),     // Green
    (0, 0, 255),     // Blue
    (255, 0, 0),     // Red
    (255, 255, 0),   // Yellow
    (255, 128, 0),   // Orange
];

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
    log_request(
        &request,
        response.status_code().0,
        response.data_length().expect("This should not fail"),
    );
    if let Err(e) = request.respond(response) {
        log::error!("could not send response: {}", e);
    }
}

pub fn serve_error(request: Request, status_code: tiny_http::StatusCode, message: &str) {
    let response = Response::new(
        status_code,
        vec![],
        message.as_bytes(),
        Some(message.as_bytes().len()),
        None,
    );
    dispatch_response(request, response);
}

pub fn log_request(request: &tiny_http::Request, status: u16, size: usize) {
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

pub fn decode_image(data: Vec<u8>) -> Result<DynamicImage, Box<dyn std::error::Error>> {
    let map: HashMap<u8, (u8, u8, u8)> = PALETTE
        .iter()
        .enumerate()
        .map(|(i, &rgb)| (i as u8, rgb))
        .collect();
    let (nwidth, nheight, w, h) = match data.len() == (EPD_WIDTH * EPD_HEIGHT / 2) as usize {
        true => (
            EPD_WIDTH,
            EPD_HEIGHT,
            EPD_WIDTH as usize,
            EPD_HEIGHT as usize,
        ),
        false => (
            (EPD_WIDTH / 2),
            EPD_HEIGHT,
            (EPD_WIDTH / 2) as usize,
            EPD_HEIGHT as usize,
        ),
    };
    let mut pixels: Vec<u8> = Vec::with_capacity(3 * w * h / 2);
    let mut buf: Vec<u8> = Vec::with_capacity(2);
    for byte in data {
        let p1 = byte >> 4;
        let p2 = byte & 0x0F;
        buf.push(p1);
        buf.push(p2);
        if buf.len() == 2 {
            let p1 = map[&buf[0]];
            let p2 = map[&buf[1]];
            pixels.push(p1.0);
            pixels.push(p1.1);
            pixels.push(p1.2);
            pixels.push(p2.0);
            pixels.push(p2.1);
            pixels.push(p2.2);
            buf = Vec::with_capacity(2);
        }
    }
    let image_buf = image::ImageBuffer::from_raw(nwidth, nheight, pixels);
    let dimage = match image_buf {
        Some(buf) => DynamicImage::ImageRgb8(buf),
        None => {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "(decode_image) Invalid image data image_buf is None",
            )));
        }
    };
    Ok(dimage)
}

// TODO: Config implementation
fn main() {
    // for debugging purposes
    if env::var_os("RUST_LOG").is_none() {
        env::set_var("RUST_LOG", "info");
    }
    if env::var_os("RUST_BACKTRACE").is_none() {
        env::set_var("RUST_BACKTRACE", "1");
    }
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
            if request.method().as_str() != "POST" {
                serve_error(request, tiny_http::StatusCode(405), "Method not allowed");
                continue;
            }
            let api_key = request.headers().iter().find(|h| h.field.equiv("API_KEY"));
            if api_key.is_none()
                || api_key.expect("previously validated").value
                    != env::var("API_KEY").expect("previously validated")
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
            let mut dbclient = CONNECTION_POOL.get_client().unwrap();
            let album_records = match dbclient
                .0
                .query(
                    "
                WITH query_1 AS (
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
                ORDER BY random()
                ",
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
            let (data_to_send, item_id, product_url, item_id_2, product_url_2) =
                match album_records.len() == 2 {
                    true => {
                        let w = 600 / 2; // 2 pixels are packed per byte
                        let h = 448;
                        let xs1 = &album_records[0].data;
                        let xs2 = &album_records[1].data;
                        let mut xs: Vec<u8> = vec![0; w * h];
                        for y in 0..h {
                            for x in 0..(w / 2) {
                                xs[y * w + x] = xs1[y * (w / 2) + x];
                                xs[y * w + x + (w / 2)] = xs2[y * (w / 2) + x];
                            }
                        }
                        (
                            xs,
                            Some(album_records[0].item_id.clone()),
                            Some(album_records[0].product_url.clone()),
                            Some(album_records[1].item_id.clone()),
                            Some(album_records[1].product_url.clone()),
                        )
                    }
                    false => (
                        album_records[0].data.clone(),
                        Some(album_records[0].item_id.clone()),
                        Some(album_records[0].product_url.clone()),
                        None,
                        None,
                    ),
                };
            let mut buf = String::new();
            request.as_reader().read_to_string(&mut buf).unwrap();
            let log_documents: Vec<LogDoc> = serde_json::from_str(&buf).unwrap();
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
                    remote_addr: vec![request.remote_addr().unwrap().ip()],
                };
                dbclient.0.execute(
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
                ).unwrap();
            }
            CONNECTION_POOL.release_client(dbclient);

            let xx = decode_image(data_to_send).unwrap();
            xx.save("public/test.jpg").unwrap();


            // let response = Response::from_data(data_to_send)
            // .with_header(tiny_http::Header::from_str("Content-Type: application/octet-stream").expect("This should never fail"),
            // );

            let response = Response::from_string("Ok\n".to_string());

            dispatch_response(request, response);
        });
    }
    loop {
        thread::park();
    }
}

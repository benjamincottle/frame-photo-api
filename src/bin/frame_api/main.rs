mod config;

use frame::database::{CONNECTION_POOL, AlbumRecord, TelemetryRecord};

use dotenv;
use env_logger;
use log;
use serde::{Deserialize, Serialize};
use ureq::serde_json;
use uuid::Uuid;
use std::{env, sync::Arc, thread, time::{UNIX_EPOCH, SystemTime}, net::{SocketAddr, IpAddr, Ipv4Addr}, io::Read, str::FromStr};
use tiny_http::{Server, Response, Request};

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
        log::error!("[Error] (dispatch_reponse) could not send response: {}", e);
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


fn main() {
    // for debugging purposes
    if env::var_os("RUST_LOG").is_none() {
        env::set_var("RUST_LOG", "info");
    }
    if env::var_os("RUST_BACKTRACE").is_none() {
        env::set_var("RUST_BACKTRACE", "1");
    }    
    dotenv::from_path("secrets/.env").ok();
    env_logger::init();
    if env::var("API_KEY").is_err() || env::var("POSTGRES_CONNECTION_STRING").is_err() {
        log::error!("[Error] (main) environment not configured");
        return;
    }
    let server = Server::http("0.0.0.0:5000").expect("This should not fail");
    println!(
        "🚀 Server started successfully, listening on {}",
        server.server_addr()
    );
    let database_url = &env::var("POSTGRES_CONNECTION_STRING").expect("previously validated");
    let pool_size = 2;
    CONNECTION_POOL.initialise(database_url, pool_size);
    let server = Arc::new(server);
    for _ in 0..2 {
        let server = server.clone();
        thread::spawn(move || loop {
            let mut request = match server.recv() {
                Ok(r) => r,
                Err(e) => {
                    log::error!("[Error] (main) could not receive request: {}", e);
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

            // curl -v -X POST http://localhost:5000/frame -H "API_KEY: ***REMOVED***" -H "Content-Type: application/json" -d "[{\"chipID\":11073650,\"uuidNumber\":\"bc598d5a-aa10-4bfc-986b-114073a3a806\",\"bootCode\":5,\"batVoltage\":3840,\"returnCode\":200,\"writeBytes\":134400,\"errorCode\":1},{\"chipID\":11073650,\"uuidNumber\":\"5ac03c9b-f217-4da3-ae3e-498be5589c37\",\"bootCode\":5,\"batVoltage\":3840,\"returnCode\":null,\"writeBytes\":null,\"errorCode\":1}]"
            let now = SystemTime::now();
            let ts = match now.duration_since(UNIX_EPOCH) {
                Ok(duration) => duration.as_secs() as i64,
                Err(_) => panic!("SystemTime before UNIX EPOCH!"),
            };
            let mut dbclient = CONNECTION_POOL.get_client().unwrap();
            let album_record = match dbclient
            .0
            .query("SELECT item_id, product_url, ts, data FROM album WHERE ts = (SELECT MIN(ts) from album) LIMIT 1", &[])
            .and_then(|records| {
                let row = records.get(0).unwrap();
                let record = AlbumRecord {
                    item_id: row.get(0),
                    product_url: row.get(1),
                    ts: row.get(2),
                    data: row.get(3),
                };
                Ok(record)
            })
             {
                Ok(record) => {
                    record},
                Err(e) => { 
                    log::error!("[Error] (main) could not get record: {}", e); 
                    serve_error(request, tiny_http::StatusCode(500), "Internal server error"); 
                    continue; 
                }   
            };
            let mut buf = String::new();
            request.as_reader().read_to_string(&mut buf).unwrap();
            let log_documents: Vec<LogDoc> = serde_json::from_str(&buf).unwrap();
            for log_doc in &log_documents {
                let record = TelemetryRecord {
                    ts,
                    item_id: Some(album_record.item_id.to_string()),
                    product_url: Some(album_record.product_url.to_string()),
                    chip_id: log_doc.chipID,
                    uuid_number: log_doc.uuidNumber,
                    bat_voltage: log_doc.batVoltage,
                    boot_code: log_doc.bootCode,
                    error_code: log_doc.errorCode,
                    return_code: log_doc.returnCode,
                    write_bytes: log_doc.writeBytes,
                    remote_addr: vec!(request.remote_addr().unwrap().ip()),
                };
                // println!("{:?}", record);
                let r = dbclient.0.execute(
                    "
                    INSERT INTO telemetry (ts, item_id, product_url, chip_id, uuid_number, bat_voltage, boot_code, error_code, return_code, write_bytes, remote_addr) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (uuid_number) 
                    DO UPDATE SET error_code = $8, return_code = $9, write_bytes = $10, remote_addr = telemetry.remote_addr || $11", 
                    &[
                        &record.ts,
                        &record.item_id,
                        &record.product_url,
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
                println!("{} rows affected", r);

            }
            CONNECTION_POOL.release_client(dbclient);
            let response = Response::from_string("Ok".to_string());
            // let response = Response::from_data(album_record.data)
            // .with_header(tiny_http::Header::from_str("Content-Type: application/octet-stream").expect("This should never fail"),
            // );
            log_request(&request, 200, response.data_length().unwrap_or(0));
            if let Err(e) = request.respond(response) {
                log::error!("Could not send response: {}", e);
            }

        });
    }
    loop {
        thread::park();
    }
}

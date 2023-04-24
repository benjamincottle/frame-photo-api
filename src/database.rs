use lazy_static::lazy_static;
use postgres::{Client, Error, NoTls};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashSet, VecDeque},
    net::IpAddr,
    sync::Mutex,
};
use uuid::Uuid;

lazy_static! {
    pub static ref CONNECTION_POOL: Mutex<VecDeque<DBClient>> = {
        log::info!("empty pool created");
        Mutex::new(VecDeque::<DBClient>::new())
    };
}

impl CONNECTION_POOL {
    pub fn initialise(&self, database_url: &str, pool_size: usize) -> Result<(), postgres::Error> {
        let mut pool = self.lock().unwrap();
        for _ in pool.len()..pool_size {
            match DBClient::connect(database_url) {
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

    pub fn get_client(&self) -> Result<DBClient, std::io::Error> {
        let mut pool = self.lock().unwrap();
        match pool.pop_front() {
            Some(client) => return Ok(client),
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "connection pool is exhausted",
                ))
            }
        };
    }

    pub fn release_client(&self, client: DBClient) {
        let mut pool = self.lock().unwrap();
        pool.push_back(client);
    }
}

pub struct DBClient(pub Client);

impl DBClient {
    fn connect(database_url: &str) -> Result<DBClient, postgres::Error> {
        let client = DBClient(Client::connect(database_url, NoTls)?);
        Ok(client)
    }

    pub fn add_record(&mut self, record: AlbumRecord) -> Result<(), Error> {
        self.0.execute(
            "INSERT INTO album (item_id, product_url, ts, data) VALUES ($1, $2, $3, $4)",
            &[
                &record.item_id,
                &record.product_url,
                &record.ts,
                &record.data,
            ],
        )?;
        Ok(())
    }

    pub fn remove_record(&mut self, record_id: String) -> Result<(), postgres::Error> {
        self.0
            .execute("DELETE FROM album WHERE item_id = $1", &[&record_id])?;
        Ok(())
    }

    pub fn get_mediaitems_set(&mut self) -> Result<HashSet<String>, postgres::Error> {
        let mut media_item_ids = HashSet::new();
        for row in self.0.query("SELECT item_id FROM album", &[])? {
            let media_item_id: &str = row.get(0);
            media_item_ids.insert(media_item_id.to_string());
        }
        Ok(media_item_ids)
    }
}

pub struct AlbumRecord {
    pub item_id: String,
    pub product_url: String,
    pub ts: i64,
    pub data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TelemetryRecord {
    pub ts: i64,
    pub item_id: Option<String>,
    pub product_url: Option<String>,
    pub chip_id: i32,
    pub uuid_number: Uuid,
    pub bat_voltage: i32,
    pub boot_code: i32,
    pub error_code: i32,
    pub return_code: Option<i32>,
    pub write_bytes: Option<i32>,
    pub remote_addr: Vec<IpAddr>,
}

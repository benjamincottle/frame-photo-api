use lazy_static::lazy_static;
use postgres::{Client, Error, NoTls};
use std::{
    collections::{HashSet, VecDeque},
    env,
    process::exit,
    sync::Mutex,
};

lazy_static! {
    pub static ref CONNECTION_POOL: Mutex<VecDeque<DBClient>> = {
        let database_url = &env::var("POSTGRES_CONNECTION_STRING").expect("previously validated");
        let pool_size = 4;
        let mut connections: VecDeque<DBClient> = VecDeque::with_capacity(pool_size);
        for _ in 0..pool_size {
            if let Some(client) = DBClient::connect(database_url).ok() {
                connections.push_back(client);
            }
        }
        if connections.len() != pool_size {
            log::error!("[Error] (database) failed to create connection pool");
            exit(1);
        }
        Mutex::new(connections)
    };
}

impl CONNECTION_POOL {
    pub fn get_client(&self) -> Result<DBClient, std::io::Error> {
        let mut connections = self.lock().unwrap();
        if let Some(client) = connections.pop_front() {
            Ok(client)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "[Error] connection pool is exhausted",
            ))
        }
    }

    pub fn release_client(&self, client: DBClient) {
        let mut connections = self.lock().unwrap();
        connections.push_back(client);
    }
}

pub struct Record {
    pub item_id: String,
    pub ts: i64,
    pub data: Vec<u8>,
}

pub struct DBClient(pub Client);

impl DBClient {
    pub fn connect(database_url: &str) -> Result<DBClient, Error> {
        let client = DBClient(Client::connect(database_url, NoTls)?);
        Ok(client)
    }

    pub fn add_record(&mut self, record: Record) -> Result<(), Error> {
        self.0.execute(
            "INSERT INTO album (item_id, ts, data) VALUES ($1, $2, $3)",
            &[&record.item_id, &record.ts, &record.data],
        )?;
        Ok(())
    }

    pub fn remove_record(&mut self, record_id: String) -> Result<(), Error> {
        self.0
            .execute("DELETE FROM album WHERE item_id = $1", &[&record_id])?;
        Ok(())
    }

    pub fn get_mediaitems_set(&mut self) -> Result<HashSet<String>, Error> {
        let mut media_item_ids = HashSet::new();
        for row in self.0.query("SELECT item_id FROM album", &[])? {
            let media_item_id: &str = row.get(0);
            media_item_ids.insert(media_item_id.to_string());
        }
        Ok(media_item_ids)
    }
}

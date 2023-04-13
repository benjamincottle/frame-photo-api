use postgres::{Client, Error, NoTls};
use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex},
};

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

pub struct ConnectionPool {
    connections: Arc<Mutex<VecDeque<DBClient>>>,
}

impl ConnectionPool {
    pub fn new(database_url: &str, pool_size: usize) -> Result<Self, Error> {
        let mut connections = VecDeque::with_capacity(pool_size);
        for _ in 0..pool_size {
            let client = DBClient::connect(database_url)?;
            connections.push_back(client);
        }
        Ok(Self {
            connections: Arc::new(Mutex::new(connections)),
        })
    }

    pub fn get_connection(&self) -> Result<DBClient, std::io::Error> {
        let mut connections = self.connections.lock().unwrap();
        let client = connections.pop_front().ok_or(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "[Error] Connection pool is exhausted",
        ))?;
        Ok(client)
    }

    pub fn return_connection(&self, client: DBClient) {
        let mut connections = self.connections.lock().unwrap();
        connections.push_back(client);
    }
}

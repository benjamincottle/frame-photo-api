use lazy_static::lazy_static;
use postgres::{Client, NoTls};
use std::{collections::VecDeque, sync::Mutex};

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
            Some(client) => return Ok(client),
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "connection pool is exhausted",
                ))
            }
        };
    }

    pub fn release_client(&self, client: Client) {
        let mut pool = self.lock().unwrap();
        pool.push_back(client);
    }
}

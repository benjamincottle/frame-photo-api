use postgres::{Client, NoTls};
use std::collections::HashSet;

pub struct Record {
    pub id: String,
    pub ts: i64,
    pub data: Vec<u8>,
}

pub struct DBClient(Client);

impl DBClient {
    pub fn new() -> Result<DBClient, postgres::Error> {
        let client = DBClient(Client::connect(
            "host=localhost user=frame_user dbname=frame password=password",
            NoTls,
        )?);
        Ok(client)
    }

    pub fn close(self) -> Result<(), postgres::Error> {
        self.0.close()?;
        Ok(())
    }

    pub fn add_record(&mut self, record: Record) -> Result<(), postgres::Error> {
        self.0.execute(
            "INSERT INTO album (id, ts, data) VALUES ($1, $2, $3)",
            &[&record.id, &record.ts, &record.data],
        )?;
        Ok(())
    }

    pub fn remove_records(
        &mut self,
        media_item_ids: &HashSet<String>,
    ) -> Result<(), postgres::Error> {
        for media_item_id in media_item_ids {
            self.0
                .execute("DELETE FROM album WHERE id = $1", &[&media_item_id])?;
        }
        Ok(())
    }

    pub fn get_mediaitems_set(&mut self) -> Result<HashSet<String>, postgres::Error> {
        let mut media_item_ids = HashSet::new();
        for row in self.0.query("SELECT id FROM album", &[])? {
            let media_item_id: &str = row.get(0);
            media_item_ids.insert(media_item_id.to_string());
        }
        Ok(media_item_ids)
    }
}

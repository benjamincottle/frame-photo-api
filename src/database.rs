use postgres::{Client, NoTls};
use std::collections::HashSet;

pub struct Record {
    pub media_item_id: String,
    pub shown_timestamp: i64,
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
            "INSERT INTO album (media_item_id, shown_timestamp, data) VALUES ($1, $2, $3)",
            &[&record.media_item_id, &record.shown_timestamp, &record.data],
        )?;
        Ok(())
    }

    pub fn remove_record(&mut self, media_item_id: &str) -> Result<(), postgres::Error> {
        self.0.execute(
            "DELETE FROM album WHERE media_item_id = $1",
            &[&media_item_id],
        )?;
        Ok(())
    }

    pub fn remove_records(
        &mut self,
        media_item_ids: &HashSet<String>,
    ) -> Result<(), postgres::Error> {
        for media_item_id in media_item_ids {
            self.0.execute(
                "DELETE FROM album WHERE media_item_id = $1",
                &[&media_item_id],
            )?;
        }
        Ok(())
    }

    pub fn get_mediaitems_set(&mut self) -> Result<HashSet<String>, postgres::Error> {
        let mut media_item_ids = HashSet::new();
        for row in self.0.query("SELECT media_item_id FROM album", &[])? {
            let media_item_id: &str = row.get(0);
            media_item_ids.insert(media_item_id.to_string());
        }
        Ok(media_item_ids)
    }
}

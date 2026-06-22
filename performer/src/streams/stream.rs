use crate::commands::search::SearchStream;
use crate::errors::error::Result;
use tokio::sync::oneshot;

pub enum Stream {
    Search(SearchStream),
}

impl Stream {
    pub async fn request_items(&mut self, num_items: i32) -> Result<()> {
        match self {
            Stream::Search(s) => s.request_items(num_items).await,
        }
    }

    pub fn run_id(&self) -> &str {
        match self {
            Stream::Search(s) => s.run_id(),
        }
    }

    pub fn completion(&mut self) -> &mut oneshot::Receiver<()> {
        match self {
            Stream::Search(s) => s.completion(),
        }
    }
}

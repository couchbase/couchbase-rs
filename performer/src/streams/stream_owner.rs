use crate::streams::stream::Stream;
use futures::future::join_all;
use log::info;
use std::collections::HashMap;
use std::ops::DerefMut;
use tokio::sync::{Mutex, MutexGuard};

#[derive(Default)]
pub struct StreamOwner {
    streams: Mutex<HashMap<String, Stream>>,
}

impl StreamOwner {
    pub async fn add(&self, id: String, stream: Stream) {
        let mut self_streams = self.streams.lock().await;
        self_streams.insert(id, stream);
    }

    pub async fn get(&self, id: &str) -> Option<impl DerefMut<Target = Stream> + '_> {
        let streams = self.streams.lock().await;
        if streams.contains_key(id) {
            Some(MutexGuard::map(streams, |s| s.get_mut(id).unwrap()))
        } else {
            None
        }
    }

    pub async fn remove(&self, id: &str) {
        let mut streams = self.streams.lock().await;
        streams.remove(id);
    }

    pub async fn wait_for_completion(&self, run_id: String) {
        let mut streams = self.streams.lock().await;

        info!("Will wait for {} streams to complete", streams.len());

        let mut completions = Vec::new();
        let mut ids_to_remove = Vec::new();

        for (id, stream) in streams.iter_mut() {
            if stream.run_id() == run_id {
                completions.push(stream.completion());
                ids_to_remove.push(id.clone());
            }
        }

        join_all(completions).await;

        for id in ids_to_remove {
            streams.remove(&id);
        }
    }
}

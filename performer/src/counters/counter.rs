use crate::errors::error::{Error, Result};
use crate::proto::protocol::shared;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

pub struct Counter {
    count: AtomicI32,
}

impl Counter {
    pub fn new(init_count: i32) -> Self {
        Self {
            count: AtomicI32::new(init_count),
        }
    }

    pub fn get_and_increment(&self) -> i32 {
        self.count.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn get_and_decrement(&self) -> i32 {
        self.count.fetch_sub(1, Ordering::SeqCst) - 1
    }

    pub fn get(&self) -> i32 {
        self.count.load(Ordering::SeqCst)
    }
}

pub struct Counters {
    counters: Mutex<HashMap<String, Arc<Counter>>>,
}

impl Counters {
    pub fn new() -> Self {
        Self {
            counters: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, shared_counter: &shared::Counter) -> Result<Arc<Counter>> {
        let global = match &shared_counter.counter {
            Some(shared::counter::Counter::Global(global)) => global,
            _ => return Err(Error::invalid_argument("Unknown bounds type")),
        };

        let mut map = self.counters.lock().unwrap();

        if let Some(existing) = map.get(&shared_counter.counter_id) {
            return Ok(existing.clone());
        }

        let init_value = global.count;
        let counter = Arc::new(Counter::new(init_value));
        map.insert(shared_counter.counter_id.clone(), counter.clone());

        Ok(counter)
    }
}

use crate::counters::counter::Counter;
use std::sync::Arc;
use std::time::SystemTime;

pub trait BoundsExecutor {
    fn can_execute(&self) -> bool;
}

pub enum Bounds {
    Counter(CounterBoundsExecutor),
    CounterEq(CounterEqualsBoundsExecutor),
    Time(TimeBoundsExecutor),
}

impl Bounds {
    pub fn new_counter(counter: Arc<Counter>) -> Self {
        Bounds::Counter(CounterBoundsExecutor::new(counter))
    }

    pub fn new_counter_eq(counter: Arc<Counter>) -> Self {
        Bounds::CounterEq(CounterEqualsBoundsExecutor::new(counter))
    }

    pub fn new_time(deadline: SystemTime) -> Self {
        Bounds::Time(TimeBoundsExecutor::new(deadline))
    }
}

pub struct CounterBoundsExecutor {
    counter: Arc<Counter>,
}

impl CounterBoundsExecutor {
    pub fn new(counter: Arc<Counter>) -> Self {
        Self { counter }
    }

    fn can_execute(&self) -> bool {
        self.counter.get_and_decrement() >= 0
    }
}

pub struct CounterEqualsBoundsExecutor {
    counter: Arc<Counter>,
    initial_counter_value: i32,
}

impl CounterEqualsBoundsExecutor {
    pub fn new(counter: Arc<Counter>) -> Self {
        let initial_counter_value = counter.get();
        Self {
            counter,
            initial_counter_value,
        }
    }

    fn can_execute(&self) -> bool {
        self.counter.get() == self.initial_counter_value
    }
}

pub struct TimeBoundsExecutor {
    deadline: SystemTime,
}

impl TimeBoundsExecutor {
    pub fn new(deadline: SystemTime) -> Self {
        Self { deadline }
    }

    fn can_execute(&self) -> bool {
        SystemTime::now() < self.deadline
    }
}

impl BoundsExecutor for Bounds {
    fn can_execute(&self) -> bool {
        match self {
            Bounds::Counter(executor) => executor.can_execute(),
            Bounds::Time(executor) => executor.can_execute(),
            Bounds::CounterEq(executor) => executor.can_execute(),
        }
    }
}

/*
	熔断器实现
	状态： 1. 关闭 2.开启 3.半开启
	状态转移以及条件：
	关闭 -> 开启 ： 初始状态：关闭 ，当出现失败的计数达到 ReadyToTrip 条件的时候，转为开启态，开启态不会接受新请求
	开启 -> 半开启：当转为开启态一段时间后 [timeOut 控制] ，状态转换为半开启态。此时可以尝试接受请求，但是接受的请求次数不能超过 threshold，超过会被拒绝
	半开启 -> 开启：当处于半开启状态时，开始尝试接受请求，如果出现一例失败的请求则重新回到开启态，拒绝接受请求直到开启态再次达到一定时间后又会转为半开启
	半开启 -> 关闭：当处于半开启状态时，开始尝试接受请求，如果连续成功的请求次数达到阈值，则转到关闭态，此时可以正常接收请求
*/
use std::{result, thread};
use std::fmt::{Display, Formatter};
use std::ops::Add;
use std::os::unix::raw::mode_t;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests {
    use super::*;

    fn ready(counts: &Counts) -> bool {
        (counts.total_failures / counts.total_requests) as f32 > 0.5
    }

    #[test]
    fn cb_test() {
        let mut cb = CircuitBreakerBuilder::new().
            timeout(Duration::from_secs(1)).
            ready_to_trip(ready).
            threshold(1).
            // is_successful()
            build();
        if let Ok(mut f) = cb.is_allow() {
            f(false);
        };
        println!("{:?}", cb.counts());
        if let Err(r) = cb.is_allow() {
            println!("{}", r)
        };
        thread::sleep(Duration::from_secs(1));
        if let Ok(mut f) = cb.is_allow() {
            println!("half open");
            f(true);
        };
    }

    #[test]
    fn builder_test() {
        let mut b = CircuitBreakerBuilder::new();
        // name("my_cir").
        // timeout(Duration::from_secs(100)).
        // ready_to_trip(|counts| {
        //     counts.consecutive_failures > 10
        // });

        println!("{} ", b);
        let b = b.
            name("rpc_server").
            interval(Duration::from_secs(4)).
            timeout(Duration::from_secs(3));
        println!("{} ", b);
    }

    #[test]
    fn enum_works() {
        assert!(State::Close != State::HalfOpen);
        assert!(State::Close == State::Close);
        println!("stat {}", State::Close)
    }

    #[test]
    fn count_works() {
        let mut c = Counts::default();
        c.on_request();
        assert_eq!(c.total_requests, 1);
        c.on_success();
        assert_eq!(c.total_success, 1);
        c.on_failure();
        assert_eq!(c.consecutive_failures, 1);
        c.clear();
        assert_eq!(c.total_requests, 0);
    }
}

#[derive(Clone, Eq, PartialEq, Copy)]
pub enum State {
    Close = 0,
    Open = 1,
    HalfOpen = 2,
}

impl Display for State {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut do_print = |state| -> std::fmt::Result {
            write!(f, "{}", state)
        };
        match self {
            State::Open => {
                do_print("Open")
            }
            State::HalfOpen => {
                do_print("HalfOpen")
            }
            State::Close => {
                do_print("Close")
            }
        }
    }
}

// 默认的循环间隔
const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);
// 默认的熔断超时时间
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Default, Debug, Clone, Copy)]
pub struct Counts {
    total_requests: u64,
    total_success: u64,
    total_failures: u64,
    consecutive_success: u64,
    consecutive_failures: u64,
}

impl Counts {
    fn clear(&mut self) {
        *self = Counts::default();
    }

    fn on_request(&mut self) {
        self.total_requests += 1;
    }
    fn on_success(&mut self) {
        self.total_success += 1;
        self.consecutive_success += 1;
        self.consecutive_failures = 0;
    }
    fn on_failure(&mut self) {
        self.total_failures += 1;
        self.consecutive_failures += 1;
        self.consecutive_success = 0;
    }
}


#[derive(Clone)]
struct Options {
    // 熔断器名称
    name: String,
    // 循环间隔，每个时间间隔会重新统计一次计数信息
    interval: Duration,
    // 熔断超时时间
    timeout: Duration,
    // 是否开启熔断
    ready_to_trip: fn(&Counts) -> bool,
    // 状态切换回调
    on_state_change: Option<fn(String, State, State)>,
    // 返回的错误是否代表成功处理
    is_successful: fn(Result<(), String>) -> bool,
    // 熔断半开启 -> 关闭的请求成功数阈值，并且在半开启状态，请求数不能超过该阈值
    threshold: u64,
}

impl Display for Options {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f,
               "
        \nname :{},\n
        interval: {}s ,\n
        timeout:{}s,\n
        threshold: {}\n
            ",
               self.name.clone(),
               self.interval.clone().as_secs(),
               self.timeout.clone().as_secs(),
               self.threshold)
    }
}

// #[derive(Default)]
pub struct CircuitBreaker {
    opt: Options,
    // 计数信息
    counts: Counts,
    // 循环的代数
    generation: u64,
    // 当前周期的过期时间
    expiry: Duration,
    create_at: Instant,
    state: State,
    mu: Arc<Mutex<i64>>,
}


pub struct CircuitBreakerBuilder {
    opt: Options,
}

impl Display for CircuitBreakerBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.opt.fmt(f)
    }
}

fn defaultReadyToTrip(counts: &Counts) -> bool {
    return counts.consecutive_failures >= 10;
}

fn defaultIsSuccessful(result: Result<(), String>) -> bool {
    if let Ok(()) = result {
        true
    } else {
        false
    }
}

impl CircuitBreakerBuilder {
    pub fn new() -> Self {
        CircuitBreakerBuilder {
            opt: Options {
                name: "default_circuit_breaker".to_string(),
                interval: DEFAULT_INTERVAL,
                timeout: DEFAULT_TIMEOUT,
                ready_to_trip: defaultReadyToTrip,
                on_state_change: None,
                is_successful: defaultIsSuccessful,
                threshold: 1,
            }
        }
    }
    pub fn name(&mut self, name: &str) -> &mut CircuitBreakerBuilder {
        self.opt.name = name.to_string();
        self
    }
    pub fn interval(&mut self, interval: Duration) -> &mut CircuitBreakerBuilder {
        self.opt.interval = interval;
        self
    }
    pub fn timeout(&mut self, timeout: Duration) -> &mut CircuitBreakerBuilder {
        self.opt.timeout = timeout;
        self
    }
    pub fn threshold(&mut self, threshold: u64) -> &mut CircuitBreakerBuilder {
        self.opt.threshold = threshold;
        self
    }
    pub fn ready_to_trip(&mut self, f: fn(&Counts) -> bool) -> &mut CircuitBreakerBuilder {
        self.opt.ready_to_trip = f;
        self
    }
    pub fn on_state_change(&mut self, f: fn(String, State, State)) -> &mut CircuitBreakerBuilder {
        self.opt.on_state_change = Some(f);
        self
    }
    pub fn is_successful(&mut self, f: fn(Result<(), String>) -> bool) -> &mut CircuitBreakerBuilder {
        self.opt.is_successful = f;
        self
    }
    pub fn build(&self) -> CircuitBreaker {
        CircuitBreaker {
            opt: self.opt.clone(),
            counts: Default::default(),
            generation: 0,
            expiry: Duration::ZERO,
            create_at: Instant::now(),
            state: State::Close,
            mu: Arc::new(Mutex::new(0)),
        }
    }
}


impl CircuitBreaker {
    pub fn is_allow<'a>(&'a mut self) -> Result<Box<dyn FnMut(bool) + 'a>, String> {
        let mu = self.mu.clone();
        let _lock = mu.lock();
        let before_generation = self.before_execute(Instant::now() - self.create_at)?;
        Ok(Box::new(move |succ| {
            self.after_execute(before_generation.clone(), succ, Instant::now() - self.create_at);
        }))
    }
    pub fn counts(&self) -> Counts {
        let mu = self.mu.clone();
        let _lock = mu.lock();
        self.counts.clone()
    }
    fn after_execute(&mut self, before_generation: u64, success: bool, now: Duration) {
        let mu = self.mu.clone();
        let _lock = mu.lock();
        self.update_state(now);
        if before_generation != self.generation {
            return;
        }
        if success {
            self.on_success(now);
        } else {
            self.on_failure(now);
        }
    }

    fn on_failure(&mut self, now: Duration) {
        match self.state {
            State::Close => {
                self.counts.on_failure();
                let f = self.opt.ready_to_trip;
                if f(&self.counts) {
                    self.set_state(State::Open, now);
                }
            }
            State::Open => {}
            State::HalfOpen => {
                self.set_state(State::Open, now);
            }
        }
    }
    fn on_success(&mut self, now: Duration) {
        self.counts.on_success();
        match self.state {
            State::Close => {
                if self.expiry < now {
                    self.new_generation(now);
                }
            }
            State::Open => {}
            State::HalfOpen => {
                if self.counts.consecutive_success >= self.opt.threshold {
                    self.set_state(State::Close, now);
                }
            }
        }
    }
    fn before_execute(&mut self, now: Duration) -> Result<u64, String> {
        self.update_state(now);
        match self.state {
            State::Open => {
                return Err("circuit breaker".to_string());
            }
            State::HalfOpen => {
                if self.counts.total_requests >= self.opt.threshold {
                    return Err("to many request".to_string());
                }
            }
            _ => ()
        }
        self.counts.on_request();
        Ok(self.generation)
    }
    fn set_state(&mut self, new_state: State, now: Duration) {
        if self.state == new_state {
            return;
        }
        let old_state = self.state.clone();
        self.state = new_state.clone();

        self.new_generation(now);
        if let Some(f) = self.opt.on_state_change {
            f(self.opt.name.clone(), old_state.clone(), new_state.clone())
        }
    }
    fn update_state(&mut self, now: Duration) {
        match self.state {
            State::Close => {
                if !self.expiry.is_zero() && self.expiry < now {
                    self.new_generation(now);
                }
            }
            State::Open => {
                if self.expiry < now {
                    self.new_generation(now);
                    self.set_state(State::HalfOpen, now);
                }
            }
            State::HalfOpen => {}
        }
    }
    fn new_generation(&mut self, now: Duration) -> u64 {
        self.generation += 1;
        self.counts.clear();
        match self.state {
            State::Close => {
                if self.opt.interval.is_zero() {
                    self.expiry = now;
                } else {
                    self.expiry = now.add(self.opt.interval);
                }
            }
            State::Open => {
                self.expiry = self.expiry.add(self.opt.interval);
            }
            State::HalfOpen => {
                self.expiry = Duration::ZERO;
            }
        };
        self.generation
    }
}
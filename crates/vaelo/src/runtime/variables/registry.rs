// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, VecDeque};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};
use std::{fmt, ptr, thread};

use serde::Serialize;

use super::arena::{Slot, SlotRange};
use super::primitive::{
    Balance, Counter, Gauge, LatencyCumulative, LatencyRecorder, LatencySnapshot, MaxRecorder,
    average,
};

const SAMPLER_INTERVAL: Duration = Duration::from_secs(1);
pub(crate) const DEFAULT_WINDOW: Duration = Duration::from_secs(60);

static VARIABLES_STATE: LazyLock<Mutex<VariablesState>> =
    LazyLock::new(|| Mutex::new(VariablesState::new()));
static SAMPLER_THREAD: LazyLock<()> = LazyLock::new(|| {
    if let Err(error) =
        thread::Builder::new().name("vaelo-variables-sampler".to_owned()).spawn(|| {
            loop {
                thread::sleep(SAMPLER_INTERVAL);
                sample_once();
            }
        })
    {
        log::error!("failed to start variables sampler thread: {error}");
    }
});

#[derive(Clone, Debug, Serialize)]
pub struct VariableSnapshot {
    pub name: String,
    pub value: VariableValue,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", content = "value")]
pub enum VariableValue {
    Counter(u64),
    Balance(i64),
    Gauge(i64),
    Max(u64),
    Latency(LatencySnapshot),
    PerSecond(u64),
    WindowCounter(u64),
    WindowLatency(LatencySnapshot),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExposeError {
    AlreadyExposed(String),
}

impl fmt::Display for ExposeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyExposed(name) => write!(f, "variable `{name}` is already exposed"),
        }
    }
}

impl std::error::Error for ExposeError {}

#[derive(Clone, Copy, Debug)]
enum Variable {
    Counter(Counter),
    Balance(Balance),
    Gauge(Gauge),
    Max(MaxRecorder),
    Latency(LatencyRecorder),
    PerSecondCounter { sampler: SamplerId, window: Duration },
    WindowCounter { sampler: SamplerId, window: Duration },
    WindowLatency { sampler: SamplerId, window: Duration },
}

impl Variable {
    fn describe(self, samplers: &SamplerRegistry) -> VariableValue {
        match self {
            Self::Counter(counter) => VariableValue::Counter(counter.get()),
            Self::Balance(balance) => VariableValue::Balance(balance.get()),
            Self::Gauge(gauge) => VariableValue::Gauge(gauge.get()),
            Self::Max(max) => VariableValue::Max(max.get()),
            Self::Latency(latency) => VariableValue::Latency(latency.snapshot()),
            Self::PerSecondCounter { sampler, window } => {
                let value = samplers
                    .counter_delta(sampler, window)
                    .map(|delta| average(delta.value, delta.seconds))
                    .unwrap_or(0);
                VariableValue::PerSecond(value)
            }
            Self::WindowCounter { sampler, window } => {
                let value =
                    samplers.counter_delta(sampler, window).map(|delta| delta.value).unwrap_or(0);
                VariableValue::WindowCounter(value)
            }
            Self::WindowLatency { sampler, window } => {
                let value = samplers.latency_window(sampler, window).unwrap_or_default();
                VariableValue::WindowLatency(value)
            }
        }
    }

    fn sampler_id(self) -> Option<SamplerId> {
        match self {
            Self::PerSecondCounter { sampler, .. }
            | Self::WindowCounter { sampler, .. }
            | Self::WindowLatency { sampler, .. } => Some(sampler),
            Self::Counter(_)
            | Self::Balance(_)
            | Self::Gauge(_)
            | Self::Max(_)
            | Self::Latency(_) => None,
        }
    }
}

#[derive(Debug)]
struct VariablesState {
    variables: HashMap<String, Variable>,
    samplers: SamplerRegistry,
}

impl VariablesState {
    fn new() -> Self {
        Self { variables: HashMap::new(), samplers: SamplerRegistry::new() }
    }

    fn expose(&mut self, name: String, variable: Variable) -> Result<(), ExposeError> {
        if self.variables.contains_key(&name) {
            return Err(ExposeError::AlreadyExposed(name));
        }
        self.variables.insert(name, variable);
        Ok(())
    }

    fn hide(&mut self, name: &str) -> bool {
        let Some(variable) = self.variables.remove(name) else {
            return false;
        };
        if let Some(sampler) = variable.sampler_id() {
            self.samplers.release(sampler);
        }
        true
    }

    fn dump(&self) -> Vec<VariableSnapshot> {
        let mut variables = self
            .variables
            .iter()
            .map(|(name, variable)| VariableSnapshot {
                name: name.clone(),
                value: variable.describe(&self.samplers),
            })
            .collect::<Vec<_>>();
        variables.sort_by(|left, right| left.name.cmp(&right.name));
        variables
    }
}

#[derive(Clone, Copy, Debug)]
struct CounterDelta {
    value: u64,
    seconds: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct SamplerId {
    index: u32,
    generation: u32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum SamplerKey {
    Counter(Slot),
    Latency(SlotRange),
}

#[derive(Debug)]
struct SamplerRegistry {
    by_source: HashMap<SamplerKey, SamplerId>,
    slots: Vec<SamplerSlot>,
    free: Vec<u32>,
    sampled: Vec<SamplerId>,
}

impl SamplerRegistry {
    fn new() -> Self {
        Self { by_source: HashMap::new(), slots: Vec::new(), free: Vec::new(), sampled: Vec::new() }
    }

    fn counter_sampler(&mut self, source: Counter, window: Duration) -> SamplerId {
        self.sampler(SamplerKey::Counter(source.slot), SamplerState::new_counter(source), window)
    }

    fn latency_sampler(&mut self, source: LatencyRecorder, window: Duration) -> SamplerId {
        self.sampler(SamplerKey::Latency(source.slots), SamplerState::new_latency(source), window)
    }

    fn sampler(&mut self, key: SamplerKey, state: SamplerState, window: Duration) -> SamplerId {
        if let Some(id) = self.by_source.get(&key).copied() {
            if let Some(existing) = self.get_mut(id) {
                existing.set_max_window(window);
            }
            if let Some(slot) = self.slots.get_mut(id.index as usize) {
                slot.refs += 1;
            }
            return id;
        }

        let id = if let Some(index) = self.free.pop() {
            let slot = &mut self.slots[index as usize];
            slot.generation = slot.generation.wrapping_add(1);
            slot.state = Some(state);
            slot.refs = 1;
            SamplerId { index, generation: slot.generation }
        } else {
            let index = self.slots.len() as u32;
            self.slots.push(SamplerSlot { generation: 0, refs: 1, state: Some(state) });
            SamplerId { index, generation: 0 }
        };
        self.get_mut(id).expect("new sampler must exist").set_max_window(window);
        self.by_source.insert(key, id);
        self.sampled.push(id);
        id
    }

    fn release(&mut self, id: SamplerId) {
        let Some(slot) = self.slots.get_mut(id.index as usize) else {
            return;
        };
        if slot.generation != id.generation {
            return;
        }
        slot.refs = slot.refs.saturating_sub(1);
        if slot.refs > 0 {
            return;
        }
        if let Some(state) = slot.state.take() {
            self.by_source.remove(&state.key());
            self.free.push(id.index);
        }
    }

    fn take_samples(&mut self) {
        let now = Instant::now();
        let mut sampled = std::mem::take(&mut self.sampled);
        sampled.retain(|id| {
            let Some(state) = self.get_mut(*id) else {
                return false;
            };
            state.take_sample(now);
            true
        });
        self.sampled = sampled;
    }

    fn counter_delta(&self, id: SamplerId, window: Duration) -> Option<CounterDelta> {
        self.get(id).and_then(|state| state.counter_delta(window))
    }

    fn latency_window(&self, id: SamplerId, window: Duration) -> Option<LatencySnapshot> {
        self.get(id).and_then(|state| state.latency_window(window))
    }

    fn get(&self, id: SamplerId) -> Option<&SamplerState> {
        let slot = self.slots.get(id.index as usize)?;
        if slot.generation == id.generation { slot.state.as_ref() } else { None }
    }

    fn get_mut(&mut self, id: SamplerId) -> Option<&mut SamplerState> {
        let slot = self.slots.get_mut(id.index as usize)?;
        if slot.generation == id.generation { slot.state.as_mut() } else { None }
    }
}

#[derive(Debug)]
struct SamplerSlot {
    generation: u32,
    refs: usize,
    state: Option<SamplerState>,
}

#[derive(Debug)]
enum SamplerState {
    Counter { source: Counter, samples: SampleRing<u64> },
    Latency { source: LatencyRecorder, samples: SampleRing<LatencyCumulative> },
}

impl SamplerState {
    fn new_counter(source: Counter) -> Self {
        let mut samples = SampleRing::new(DEFAULT_WINDOW);
        samples.push(Instant::now(), source.get());
        Self::Counter { source, samples }
    }

    fn new_latency(source: LatencyRecorder) -> Self {
        let mut samples = SampleRing::new(DEFAULT_WINDOW);
        samples.push(Instant::now(), source.cumulative());
        Self::Latency { source, samples }
    }

    fn set_max_window(&mut self, window: Duration) {
        match self {
            Self::Counter { samples, .. } => samples.set_window(window),
            Self::Latency { samples, .. } => samples.set_window(window),
        }
    }

    fn key(&self) -> SamplerKey {
        match self {
            Self::Counter { source, .. } => SamplerKey::Counter(source.slot),
            Self::Latency { source, .. } => SamplerKey::Latency(source.slots),
        }
    }

    fn take_sample(&mut self, now: Instant) {
        match self {
            Self::Counter { source, samples } => samples.push(now, source.get()),
            Self::Latency { source, samples } => samples.push(now, source.cumulative()),
        }
    }

    fn counter_delta(&self, window: Duration) -> Option<CounterDelta> {
        let Self::Counter { samples, .. } = self else {
            return None;
        };
        let (oldest, latest) = samples.span(window)?;
        let seconds = latest.at.saturating_duration_since(oldest.at).as_secs().max(1);
        Some(CounterDelta { value: latest.value.saturating_sub(oldest.value), seconds })
    }

    fn latency_window(&self, window: Duration) -> Option<LatencySnapshot> {
        let Self::Latency { samples, .. } = self else {
            return None;
        };
        let (oldest, latest) = samples.span(window)?;
        Some(latest.value.window_since(Some(&oldest.value)))
    }
}

#[derive(Debug)]
struct SampleRing<T> {
    samples: VecDeque<Sample<T>>,
    max_window: Duration,
}

impl<T> SampleRing<T> {
    fn new(max_window: Duration) -> Self {
        Self { samples: VecDeque::with_capacity(sample_capacity(max_window)), max_window }
    }

    fn set_window(&mut self, window: Duration) {
        if window > self.max_window {
            self.max_window = window;
            let desired = sample_capacity(window);
            if desired > self.samples.capacity() {
                self.samples.reserve(desired - self.samples.capacity());
            }
        }
        self.trim();
    }

    fn push(&mut self, at: Instant, value: T) {
        self.samples.push_back(Sample { at, value });
        self.trim();
    }

    fn trim(&mut self) {
        let Some(latest_at) = self.samples.back().map(|sample| sample.at) else {
            return;
        };
        let keep = self.max_window + SAMPLER_INTERVAL;
        while self.samples.len() > 2 {
            let Some(front) = self.samples.front() else {
                break;
            };
            if latest_at.saturating_duration_since(front.at) <= keep {
                break;
            }
            self.samples.pop_front();
        }
    }

    fn span(&self, window: Duration) -> Option<(&Sample<T>, &Sample<T>)> {
        let latest = self.samples.back()?;
        let mut oldest = self.samples.front()?;
        for sample in self.samples.iter().rev() {
            if latest.at.saturating_duration_since(sample.at) >= window {
                oldest = sample;
                break;
            }
            oldest = sample;
        }
        if ptr::eq(oldest, latest) { None } else { Some((oldest, latest)) }
    }
}

#[derive(Debug)]
struct Sample<T> {
    at: Instant,
    value: T,
}

pub fn expose_counter(name: impl Into<String>, counter: Counter) -> Result<(), ExposeError> {
    expose(name, Variable::Counter(counter))
}

pub fn expose_balance(name: impl Into<String>, balance: Balance) -> Result<(), ExposeError> {
    expose(name, Variable::Balance(balance))
}

pub fn expose_gauge(name: impl Into<String>, gauge: Gauge) -> Result<(), ExposeError> {
    expose(name, Variable::Gauge(gauge))
}

pub fn expose_max(name: impl Into<String>, max: MaxRecorder) -> Result<(), ExposeError> {
    expose(name, Variable::Max(max))
}

pub fn expose_latency(
    name: impl Into<String>,
    latency: LatencyRecorder,
) -> Result<(), ExposeError> {
    expose(name, Variable::Latency(latency))
}

pub(crate) fn expose_counter_per_second(
    name: impl Into<String>,
    counter: Counter,
    window: Duration,
) -> Result<(), ExposeError> {
    let name = name.into();
    let mut state = VARIABLES_STATE.lock().expect("variables registry mutex poisoned");
    if state.variables.contains_key(&name) {
        return Err(ExposeError::AlreadyExposed(name));
    }
    let sampler = state.samplers.counter_sampler(counter, window);
    let result = state.expose(name, Variable::PerSecondCounter { sampler, window });
    drop(state);
    ensure_sampler_started();
    result
}

pub(crate) fn expose_counter_window(
    name: impl Into<String>,
    counter: Counter,
    window: Duration,
) -> Result<(), ExposeError> {
    let name = name.into();
    let mut state = VARIABLES_STATE.lock().expect("variables registry mutex poisoned");
    if state.variables.contains_key(&name) {
        return Err(ExposeError::AlreadyExposed(name));
    }
    let sampler = state.samplers.counter_sampler(counter, window);
    let result = state.expose(name, Variable::WindowCounter { sampler, window });
    drop(state);
    ensure_sampler_started();
    result
}

pub(crate) fn expose_latency_window(
    name: impl Into<String>,
    latency: LatencyRecorder,
    window: Duration,
) -> Result<(), ExposeError> {
    let name = name.into();
    let mut state = VARIABLES_STATE.lock().expect("variables registry mutex poisoned");
    if state.variables.contains_key(&name) {
        return Err(ExposeError::AlreadyExposed(name));
    }
    let sampler = state.samplers.latency_sampler(latency, window);
    let result = state.expose(name, Variable::WindowLatency { sampler, window });
    drop(state);
    ensure_sampler_started();
    result
}

pub fn hide(name: &str) -> bool {
    VARIABLES_STATE.lock().expect("variables registry mutex poisoned").hide(name)
}

pub fn dump() -> Vec<VariableSnapshot> {
    VARIABLES_STATE.lock().expect("variables registry mutex poisoned").dump()
}

fn expose(name: impl Into<String>, variable: Variable) -> Result<(), ExposeError> {
    VARIABLES_STATE.lock().expect("variables registry mutex poisoned").expose(name.into(), variable)
}

fn ensure_sampler_started() {
    LazyLock::force(&SAMPLER_THREAD);
}

fn sample_once() {
    VARIABLES_STATE.lock().expect("variables sampler mutex poisoned").samplers.take_samples();
}

fn sample_capacity(window: Duration) -> usize {
    window.as_secs().max(1) as usize + 2
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn registry_exposes_hides_and_dumps_variables() {
        let name = "variables_registry_exposes_hides_and_dumps_variables";
        hide(name);

        let counter = Counter::new();
        counter.add(3);
        expose_counter(name, counter).unwrap();

        let snapshot = dump().into_iter().find(|variable| variable.name == name).unwrap();
        assert!(matches!(snapshot.value, VariableValue::Counter(3)));
        assert!(hide(name));
        assert!(dump().into_iter().all(|variable| variable.name != name));
    }

    #[test]
    fn duplicate_expose_fails() {
        let name = "variables_duplicate_expose_fails";
        hide(name);
        let counter = Counter::new();
        expose_counter(name, counter).unwrap();
        assert_eq!(
            expose_counter(name, counter).unwrap_err(),
            ExposeError::AlreadyExposed(name.to_owned())
        );
        assert!(hide(name));
    }

    #[test]
    fn counter_sampler_is_shared_by_window_and_per_second() {
        let counter = Counter::new();
        let mut samplers = SamplerRegistry::new();
        let id = samplers.counter_sampler(counter, Duration::from_secs(10));
        let same = samplers.counter_sampler(counter, Duration::from_secs(60));
        assert_eq!(id, same);
    }
}

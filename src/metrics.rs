use std::sync::atomic::{AtomicU64, Ordering};

/// Lightweight process-wide counters for `GET /metrics`.
#[derive(Default)]
pub struct Metrics {
    total_requests: AtomicU64,
    total_latency_ms: AtomicU64,
}

impl Metrics {
    /// Record a completed synchronous transcription and its wall-clock latency.
    pub fn record(&self, latency_ms: u64) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ms
            .fetch_add(latency_ms, Ordering::Relaxed);
    }

    /// Returns `(total_requests, avg_latency_ms)`.
    pub fn snapshot(&self) -> (u64, u64) {
        let n = self.total_requests.load(Ordering::Relaxed);
        let total = self.total_latency_ms.load(Ordering::Relaxed);
        let avg = if n > 0 { total / n } else { 0 };
        (n, avg)
    }
}

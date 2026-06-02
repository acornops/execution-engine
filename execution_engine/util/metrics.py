"""Prometheus metrics for tracking Execution Engine performance."""

from prometheus_client import Counter, Gauge, Histogram

# Existing lifecycle metrics kept stable for current dashboards.
active_runs = Gauge("active_runs", "Number of active runs")
runs_started_total = Counter("runs_started_total", "Total number of runs started")
runs_completed_total = Counter("runs_completed_total", "Total number of runs completed")
runs_failed_total = Counter("runs_failed_total", "Total number of runs failed")
runs_cancelled_total = Counter("runs_cancelled_total", "Total number of runs cancelled")

# Production readiness and dependency visibility.
readiness_dependency_status = Gauge(
    "execution_engine_readiness_dependency_status",
    "Readiness status for each dependency, 1 for ready and 0 for failing",
    ["dependency"],
)

# Dispatch and queue behavior.
dispatch_requests_total = Counter(
    "execution_engine_dispatch_requests_total",
    "Dispatch requests by result",
    ["result"],
)
cancel_requests_total = Counter(
    "execution_engine_cancel_requests_total",
    "Cancel requests by result",
    ["result"],
)
queued_runs = Gauge("execution_engine_queued_runs", "Number of runs waiting in the local queue")
run_duration_seconds = Histogram(
    "execution_engine_run_duration_seconds",
    "Run duration by terminal status",
    ["status"],
    buckets=(1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1200),
)

# Durable delivery metrics.
event_outbox_pending = Gauge("execution_engine_event_outbox_pending", "Pending durable event outbox entries")
events_delivered_total = Counter("execution_engine_events_delivered_total", "Durable events delivered")
events_delivery_failed_total = Counter("execution_engine_events_delivery_failed_total", "Event delivery failures")
terminal_commits_pending = Gauge(
    "execution_engine_terminal_commits_pending",
    "Pending durable terminal commits",
)
terminal_commits_total = Counter(
    "execution_engine_terminal_commits_total",
    "Terminal commit attempts by result",
    ["result"],
)

# Dependency call metrics.
orchestrator_requests_total = Counter(
    "execution_engine_orchestrator_requests_total",
    "Orchestrator requests by endpoint and result",
    ["endpoint", "result"],
)
orchestrator_retries_total = Counter(
    "execution_engine_orchestrator_retries_total",
    "Orchestrator retry sleeps by endpoint",
    ["endpoint"],
)
gateway_streams_total = Counter(
    "execution_engine_gateway_streams_total",
    "LLM gateway streams by result",
    ["result"],
)
gateway_stream_malformed_chunks_total = Counter(
    "execution_engine_gateway_stream_malformed_chunks_total",
    "Malformed LLM gateway stream chunks",
)
tool_calls_total = Counter(
    "execution_engine_tool_calls_total",
    "Tool calls by result",
    ["result"],
)

from prometheus_client import Counter

# Collector metrics
COLLECTOR_EVENTS_FETCHED = Counter(
    "seismoops_collector_events_fetched_total",
    "Total number of earthquake events fetched from USGS",
)

COLLECTOR_EVENTS_PUBLISHED = Counter(
    "seismoops_collector_events_published_total",
    "Total number of earthquake events published to Redis",
)

COLLECTOR_ERRORS = Counter(
    "seismoops_collector_errors_total",
    "Total number of collector errors",
)


# Processor metrics
PROCESSOR_MESSAGES_PROCESSED = Counter(
    "seismoops_processor_messages_processed_total",
    "Total number of earthquake messages successfully processed",
)

PROCESSOR_FAILURES = Counter(
    "seismoops_processor_failures_total",
    "Total number of earthquake message processing failures",
)

PROCESSOR_RETRIES = Counter(
    "seismoops_processor_retries_total",
    "Total number of earthquake message retries",
)

PROCESSOR_DLQ_MESSAGES = Counter(
    "seismoops_processor_dlq_messages_total",
    "Total number of earthquake messages moved to the dead-letter queue",
)

PROCESSOR_RECOVERED_MESSAGES = Counter(
    "seismoops_processor_recovered_messages_total",
    "Total number of pending earthquake messages recovered",
)

# API metrics
API_REQUESTS = Counter(
    "seismoops_api_requests_total",
    "Total number of HTTP requests received by the API",
)
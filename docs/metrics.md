# Production Observability Metrics

## Overview

The trading-data-downloader includes comprehensive production observability through Prometheus-compatible metrics. This system tracks 429 errors, retry behavior, rate limiter health, and API weight consumption with minimal overhead (<1%).

## Quick Start

### 1. Enable Metrics

Set the metrics endpoint address via environment variable:

```bash
export METRICS_ADDR="0.0.0.0:9090"  # Default
./trading-data-downloader download bars ...
```

The metrics endpoint will be available at `http://localhost:9090/metrics`.

### 2. Prometheus Configuration

Add to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'trading-downloader'
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:9090']
        labels:
          environment: 'production'
          service: 'trading-data-downloader'
```

### 3. Verify Metrics

```bash
curl http://localhost:9090/metrics
```

## Metrics Catalog

### HTTP Request Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|--------|
| `http_requests_total` | Counter | Total HTTP requests to Binance API | `endpoint`, `status`, `attempt` |
| `http_429_errors_total` | Counter | Total 429 rate limit errors | `endpoint` |
| `http_retries_total` | Counter | Total retry attempts | `attempt` |
| `http_request_duration_seconds` | Histogram | Request duration in seconds | `endpoint` |
| `retry_backoff_duration_seconds` | Histogram | Retry backoff duration | `attempt` |

### API Weight Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|--------|
| `api_weight_consumed` | Gauge | Current API weight consumed (rolling window) | - |
| `api_weight_remaining` | Gauge | Remaining API weight in window | - |

### Rate Limiter Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|--------|
| `rate_limit_permits_acquired_total` | Counter | Total permits acquired | `weight` |
| `rate_limit_permits_available` | Gauge | Currently available permits | - |
| `rate_limit_queue_wait_seconds` | Histogram | Time waiting for permits | - |

### Download Job Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|--------|
| `downloads_completed_total` | Counter | Successful downloads | `job_type`, `symbol` |
| `downloads_failed_total` | Counter | Failed downloads | `job_type`, `symbol`, `error` |

## Grafana Dashboard

### Recommended Panels

#### 1. 429 Error Rate
```promql
rate(http_429_errors_total[5m])
```

#### 2. API Weight Usage
```promql
api_weight_consumed / 2400 * 100
```

#### 3. Request Success Rate
```promql
sum(rate(http_requests_total{status="200"}[5m])) /
sum(rate(http_requests_total[5m])) * 100
```

#### 4. P99 Request Latency
```promql
histogram_quantile(0.99,
  sum(rate(http_request_duration_seconds_bucket[5m])) by (le, endpoint)
)
```

#### 5. Rate Limiter Efficiency
```promql
rate_limit_permits_available /
(rate_limit_permits_available + sum(rate(rate_limit_permits_acquired_total[1m])))
```

#### 6. Retry Distribution
```promql
sum by(attempt) (rate(http_retries_total[5m]))
```

### Complete Dashboard JSON

```json
{
  "dashboard": {
    "title": "Trading Data Downloader",
    "panels": [
      {
        "title": "429 Errors (Last Hour)",
        "targets": [{
          "expr": "sum(increase(http_429_errors_total[1h]))"
        }]
      },
      {
        "title": "API Weight Usage %",
        "targets": [{
          "expr": "api_weight_consumed / 2400 * 100"
        }]
      },
      {
        "title": "Request Success Rate",
        "targets": [{
          "expr": "sum(rate(http_requests_total{status=\"200\"}[5m])) / sum(rate(http_requests_total[5m])) * 100"
        }]
      },
      {
        "title": "Downloads by Symbol",
        "targets": [{
          "expr": "sum by(symbol) (increase(downloads_completed_total[1h]))"
        }]
      }
    ]
  }
}
```

## Alert Rules

### Critical Alerts (Page On-Call)

```yaml
groups:
  - name: trading_downloader_critical
    rules:
      - alert: HighRateLimitErrors
        expr: rate(http_429_errors_total[5m]) > 0.1
        for: 5m
        annotations:
          summary: "High rate of 429 errors ({{ $value }} per second)"

      - alert: APIWeightExhaustion
        expr: api_weight_consumed / 2400 > 0.9
        for: 1m
        annotations:
          summary: "API weight usage above 90% ({{ $value }}%)"

      - alert: DownloadFailureRate
        expr: |
          sum(rate(downloads_failed_total[15m])) /
          sum(rate(downloads_completed_total[15m]) + rate(downloads_failed_total[15m])) > 0.1
        for: 10m
        annotations:
          summary: "Download failure rate above 10%"
```

### Warning Alerts

```yaml
  - name: trading_downloader_warnings
    rules:
      - alert: HighRetryRate
        expr: sum(rate(http_retries_total[10m])) > 1
        for: 10m
        annotations:
          summary: "High retry rate ({{ $value }} per second)"

      - alert: SlowRequests
        expr: |
          histogram_quantile(0.95,
            sum(rate(http_request_duration_seconds_bucket[5m])) by (le)
          ) > 5
        for: 10m
        annotations:
          summary: "P95 request latency above 5 seconds"
```

## Performance Validation

### Load Test Results

Metrics overhead measured with 10,000 requests/minute:

- **CPU overhead**: 0.3% additional
- **Memory overhead**: 2MB additional
- **Request latency impact**: <0.1ms
- **Metric emission**: Fully async, non-blocking

### Test Command

```bash
# Run with metrics
METRICS_ADDR=0.0.0.0:9090 cargo test --release test_metrics_performance

# Benchmark without metrics
cargo bench --bench download_speed

# Benchmark with metrics
METRICS_ADDR=0.0.0.0:9090 cargo bench --bench download_speed
```

## Troubleshooting

### Metrics Not Appearing

1. Check metrics initialization:
   ```bash
   RUST_LOG=trading_data_downloader=debug cargo run
   # Look for: "Metrics system initialized successfully"
   ```

2. Verify endpoint is accessible:
   ```bash
   curl -v http://localhost:9090/metrics
   ```

3. Check firewall/network:
   ```bash
   netstat -an | grep 9090
   ```

### High Memory Usage

If metrics consume too much memory (>10MB), consider:

1. Reducing cardinality by removing labels
2. Adjusting histogram buckets
3. Enabling metric expiration

### Integration with Cloud Providers

#### AWS CloudWatch
```bash
# Use cloudwatch-exporter
docker run -p 9106:9106 prom/cloudwatch-exporter
```

#### GCP Monitoring
```bash
# Use stackdriver-exporter
docker run -p 9255:9255 prometheuscommunity/stackdriver-exporter
```

#### Azure Monitor
```bash
# Use azure-metrics-exporter
docker run -p 9276:9276 webdevops/azure-metrics-exporter
```

## Architecture Notes

### Design Decisions

1. **Library Choice**: `metrics` crate with Prometheus exporter
   - Pros: Low overhead, standard format, wide ecosystem support
   - Cons: Additional dependency (acceptable trade-off)

2. **Async Emission**: All metrics are emitted asynchronously
   - Zero blocking on HTTP request path
   - Graceful degradation if sink unavailable

3. **Correlation IDs**: Each request has unique ID for tracing
   - Format: `req-{hex}`
   - Enables request tracking across retries

4. **Weight Feedback Loop**: Implemented but conservative
   - Tracks X-MBX-USED-WEIGHT-1M header
   - Emits gauges but doesn't adjust rate limiter (yet)

### Future Enhancements

1. **Dynamic Rate Limiting**: Use weight metrics to adjust permits
2. **Distributed Tracing**: Add OpenTelemetry spans
3. **Custom Dashboards**: Per-exchange, per-symbol views
4. **Predictive Alerting**: ML-based anomaly detection

## References

- [Prometheus Best Practices](https://prometheus.io/docs/practices/naming/)
- [Grafana Dashboard Examples](https://grafana.com/grafana/dashboards)
- [metrics Crate Documentation](https://docs.rs/metrics)
- [Binance API Weight Limits](https://binance-docs.github.io/apidocs/futures/en/#limits)
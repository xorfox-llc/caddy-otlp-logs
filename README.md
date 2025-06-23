# Caddy OTLP Log Writer Module

This module provides a Caddy log writer that exports logs directly to an OpenTelemetry Protocol (OTLP) endpoint.

## Features

- Exports Caddy logs via OTLP (gRPC or HTTP/protobuf)
- Full support for OpenTelemetry environment variables
- Automatic batching for efficient log export
- Trace context correlation (trace_id, span_id)
- Resource attributes and semantic conventions
- Structured log attributes preservation

## Configuration

The module can be configured using Docker labels when using caddy-docker-proxy:

```yaml
labels:
  caddy.log: default
  caddy.log.output: otlp
  caddy.log.output.protocol: grpc  # or "http/protobuf"
  caddy.log.output.endpoint: ${OTEL_ENDPOINT}:443
  caddy.log.output.insecure: false
  caddy.log.format: json
  caddy.log.level: INFO
```

## Environment Variables

The module respects standard OpenTelemetry environment variables:

- `OTEL_EXPORTER_OTLP_ENDPOINT` - Base endpoint for all signals
- `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` - Specific endpoint for logs
- `OTEL_EXPORTER_OTLP_PROTOCOL` - Protocol (grpc or http/protobuf)
- `OTEL_EXPORTER_OTLP_LOGS_PROTOCOL` - Specific protocol for logs
- `OTEL_EXPORTER_OTLP_HEADERS` - Headers to send with requests
- `OTEL_EXPORTER_OTLP_LOGS_HEADERS` - Specific headers for logs
- `OTEL_EXPORTER_OTLP_TIMEOUT` - Export timeout in milliseconds
- `OTEL_EXPORTER_OTLP_LOGS_TIMEOUT` - Specific timeout for logs
- `OTEL_EXPORTER_OTLP_INSECURE` - Disable TLS verification
- `OTEL_EXPORTER_OTLP_LOGS_INSECURE` - Specific insecure setting for logs
- `OTEL_SERVICE_NAME` - Service name resource attribute
- `OTEL_RESOURCE_ATTRIBUTES` - Additional resource attributes

## JSON Configuration

When using Caddy's JSON config:

```json
{
  "logging": {
    "logs": {
      "default": {
        "writer": {
          "output": "otlp",
          "endpoint": "localhost:4317",
          "protocol": "grpc",
          "headers": {
            "X-Auth-Token": "your-token"
          },
          "service_name": "caddy",
          "resource_attributes": {
            "environment": "production"
          },
          "batch_size": 100,
          "batch_timeout": "5s"
        }
      }
    }
  }
}
```

## Installation

### Using xcaddy

```bash
xcaddy build \
  --with github.com/xorfox-llc/caddy-otlp-logs
```

### Local Development

```bash
xcaddy build \
  --with github.com/xorfox-llc/caddy-otlp-logs=/path/to/caddy-otlp-logs
```

## How It Works

1. The module implements Caddy's `WriterOpener` interface
2. Log entries are parsed from JSON format
3. Logs are batched for efficient export
4. Trace context is extracted from `trace_id` and `span_id` fields
5. All log attributes are preserved as OTLP attributes
6. Logs are sent via gRPC or HTTP/protobuf to the OTLP endpoint

## Log Format

The module expects Caddy's JSON log format. It automatically:
- Extracts log level and converts to OTLP severity
- Preserves the log message
- Extracts trace context for correlation
- Converts all other fields to OTLP attributes

## Batching

Logs are batched for performance:
- Default batch size: 100 logs
- Default batch timeout: 5 seconds
- Batches are sent when full or on timeout
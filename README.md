# Caddy OTLP Log Writer Module

This module provides a Caddy log writer that exports logs directly to an OpenTelemetry Protocol (OTLP) endpoint.

## Features

- Exports Caddy logs via OTLP gRPC (HTTP/protobuf support is planned)
- Full support for OpenTelemetry environment variables
- Automatic batching for efficient log export
- Trace context correlation (trace_id, span_id)
- Resource attributes and semantic conventions
- Structured log attributes preservation
- TLS/mTLS support with custom certificates
- Compression support (gzip)
- Singleton pattern for configuration reload resilience

## Configuration

The module can be configured using Docker labels when using caddy-docker-proxy:

```yaml
labels:
  caddy.log: default
  caddy.log.output: otlp
  caddy.log.output.protocol: grpc
  caddy.log.output.endpoint: ${OTEL_ENDPOINT}:443
  caddy.log.output.insecure: false
  caddy.log.output.swarm_mode: replica  # Options: disabled, replica, hash, active
  caddy.log.format: json
  caddy.log.level: INFO
```

## Environment Variables

The module fully supports the standard OpenTelemetry environment variables as defined in the [OTLP specification](https://opentelemetry.io/docs/specs/otel/protocol/exporter/):

### Endpoint Configuration
- `OTEL_EXPORTER_OTLP_ENDPOINT` - Base endpoint URL for any signal type
- `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` - Endpoint URL specifically for logs (takes precedence)

### Protocol Configuration
- `OTEL_EXPORTER_OTLP_PROTOCOL` - Transport protocol (`grpc` only currently supported)
- `OTEL_EXPORTER_OTLP_LOGS_PROTOCOL` - Protocol specifically for logs (takes precedence)

### Headers Configuration
- `OTEL_EXPORTER_OTLP_HEADERS` - Headers as comma-separated key=value pairs
- `OTEL_EXPORTER_OTLP_LOGS_HEADERS` - Headers specifically for logs (takes precedence)

Example: `Authorization=Bearer token,X-Custom=value`

### Security Configuration
- `OTEL_EXPORTER_OTLP_INSECURE` - Set to `true` to disable TLS verification
- `OTEL_EXPORTER_OTLP_LOGS_INSECURE` - Insecure mode specifically for logs (takes precedence)

### Certificate Configuration
- `OTEL_EXPORTER_OTLP_CERTIFICATE` - Path to CA certificate file
- `OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE` - CA certificate specifically for logs (takes precedence)
- `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` - Path to client certificate for mTLS
- `OTEL_EXPORTER_OTLP_CLIENT_KEY` - Path to client key for mTLS

### Timeout Configuration
- `OTEL_EXPORTER_OTLP_TIMEOUT` - Export timeout (e.g., `30s`, `1m`)
- `OTEL_EXPORTER_OTLP_LOGS_TIMEOUT` - Timeout specifically for logs (takes precedence)

### Compression Configuration
- `OTEL_EXPORTER_OTLP_COMPRESSION` - Compression type (`gzip` or empty for none)
- `OTEL_EXPORTER_OTLP_LOGS_COMPRESSION` - Compression specifically for logs (takes precedence)

### Service Configuration
- `OTEL_SERVICE_NAME` - Sets the service.name resource attribute
- `OTEL_RESOURCE_ATTRIBUTES` - Additional resource attributes as comma-separated key=value pairs

Example: `service.name=myapp,service.version=1.0.0,deployment.environment=prod`

### Debug Configuration
- `CADDY_OTLP_DEBUG` - Set to `true` to enable debug logging (Caddy-specific)

### Precedence Rules
1. Configuration values explicitly set in Caddyfile or JSON take precedence over environment variables
2. Signal-specific environment variables (e.g., `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`) take precedence over general ones
3. Empty environment variable values are treated the same as unset variables

## Configuration Examples

### Minimal Configuration with Environment Variables

When using environment variables, your Caddyfile can be minimal:

```caddyfile
{
    log {
        output otlp
    }
}
```

Set environment variables:
```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp.example.com:4317
export OTEL_SERVICE_NAME=my-caddy-service
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer mytoken"
caddy run
```

### Full Caddyfile Configuration

```caddyfile
{
    log {
        output otlp {
            endpoint localhost:4317
            protocol grpc  # Note: http/protobuf is planned but not yet implemented
            insecure true
            timeout 30s
            service_name my-service
            batch_size 100
            batch_timeout 5s
            compression gzip
            swarm_mode replica
            ca_cert /path/to/ca.crt
            client_cert /path/to/client.crt
            client_key /path/to/client.key
            headers {
                Authorization "Bearer token"
                X-Custom-Header "value"
            }
            resource_attributes {
                environment production
                region us-west-2
            }
        }
    }
}
```

### JSON Configuration

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
          "batch_timeout": "5s",
          "compression": "gzip",
          "ca_cert": "/path/to/ca.crt"
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
6. Logs are sent via gRPC to the OTLP endpoint

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

## Docker Swarm Mode

When running in Docker Swarm, the module can coordinate between replicas to control log export behavior. This is automatically detected when Docker Swarm environment variables are present (`DOCKER_NODE_ID`, `DOCKER_SERVICE_NAME`, etc.).

### Swarm Mode Options

Configure using the `swarm_mode` directive:

```caddyfile
{
    log {
        output otlp {
            endpoint localhost:4317
            swarm_mode replica  # Options: disabled, replica, hash, active
        }
    }
}
```

#### Available Modes

- **disabled** (default): No swarm coordination. Each instance maintains its own singleton.
- **replica**: Only the primary replica (slot 1) sends logs. Other replicas discard logs.
- **hash**: Not yet implemented. Will use consistent hashing to distribute log responsibility.
- **active**: All replicas actively send logs (each maintains separate singletons).

### Use Cases

#### Single Writer Pattern (replica mode)
Best for centralized logging where you want to avoid duplicate logs:
```caddyfile
swarm_mode replica
```
Only the primary replica (slot 1) will send logs to your OTLP endpoint.

#### All Active Pattern (active mode)
Best when you need high availability and can handle duplicate logs:
```caddyfile
swarm_mode active
```
All replicas send logs independently.

### Environment Variables

The module detects these Docker Swarm environment variables:
- `DOCKER_NODE_ID`: The swarm node identifier
- `DOCKER_SERVICE_NAME`: The service name
- `DOCKER_TASK_SLOT`: The task slot number (1 for primary replica)
- `DOCKER_TASK_ID`: Unique task identifier

When these are present, the module adjusts its singleton key generation based on the configured swarm mode.
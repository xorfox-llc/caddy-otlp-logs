package otlplogs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOTLPWriter_EnvironmentVariables(t *testing.T) {
	tests := []struct {
		name     string
		envVars  map[string]string
		writer   *OTLPWriter
		expected *OTLPWriter
	}{
		{
			name: "endpoint from logs-specific env",
			envVars: map[string]string{
				"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "https://logs.example.com:4317",
			},
			writer: &OTLPWriter{},
			expected: &OTLPWriter{
				Endpoint: "https://logs.example.com:4317",
			},
		},
		{
			name: "endpoint from general env",
			envVars: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": "https://example.com:4317",
			},
			writer: &OTLPWriter{},
			expected: &OTLPWriter{
				Endpoint: "https://example.com:4317",
			},
		},
		{
			name: "logs-specific takes precedence",
			envVars: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT":      "https://general.com:4317",
				"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "https://logs.com:4317",
			},
			writer: &OTLPWriter{},
			expected: &OTLPWriter{
				Endpoint: "https://logs.com:4317",
			},
		},
		{
			name: "protocol from env",
			envVars: map[string]string{
				"OTEL_EXPORTER_OTLP_LOGS_PROTOCOL": "http/protobuf",
			},
			writer: &OTLPWriter{},
			expected: &OTLPWriter{
				Protocol: "http/protobuf",
			},
		},
		{
			name: "headers from env",
			envVars: map[string]string{
				"OTEL_EXPORTER_OTLP_LOGS_HEADERS": "Authorization=Bearer token,X-Custom=value",
			},
			writer: &OTLPWriter{},
			expected: &OTLPWriter{
				Headers: map[string]string{
					"Authorization": "Bearer token",
					"X-Custom":      "value",
				},
			},
		},
		{
			name: "timeout from env",
			envVars: map[string]string{
				"OTEL_EXPORTER_OTLP_LOGS_TIMEOUT": "30s",
			},
			writer: &OTLPWriter{},
			expected: &OTLPWriter{
				Timeout: caddy.Duration(30 * time.Second),
			},
		},
		{
			name: "service name from env",
			envVars: map[string]string{
				"OTEL_SERVICE_NAME": "my-service",
			},
			writer: &OTLPWriter{},
			expected: &OTLPWriter{
				ServiceName: "my-service",
			},
		},
		{
			name: "insecure from env",
			envVars: map[string]string{
				"OTEL_EXPORTER_OTLP_LOGS_INSECURE": "true",
			},
			writer: &OTLPWriter{},
			expected: &OTLPWriter{
				Insecure: true,
			},
		},
		{
			name: "compression from env",
			envVars: map[string]string{
				"OTEL_EXPORTER_OTLP_LOGS_COMPRESSION": "gzip",
			},
			writer: &OTLPWriter{},
			expected: &OTLPWriter{
				Compression: "gzip",
			},
		},
		{
			name: "certificates from env",
			envVars: map[string]string{
				"OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE": "/path/to/ca.crt",
				"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE": "/path/to/client.crt",
				"OTEL_EXPORTER_OTLP_CLIENT_KEY": "/path/to/client.key",
			},
			writer: &OTLPWriter{},
			expected: &OTLPWriter{
				CACert:     "/path/to/ca.crt",
				ClientCert: "/path/to/client.crt",
				ClientKey:  "/path/to/client.key",
			},
		},
		{
			name: "resource attributes from env",
			envVars: map[string]string{
				"OTEL_RESOURCE_ATTRIBUTES": "service.name=test,service.version=1.0.0,deployment.environment=prod",
			},
			writer: &OTLPWriter{},
			expected: &OTLPWriter{
				ResourceAttributes: map[string]string{
					"service.name":            "test",
					"service.version":         "1.0.0",
					"deployment.environment":  "prod",
				},
			},
		},
		{
			name: "config values take precedence over env",
			envVars: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": "https://env.com:4317",
				"OTEL_SERVICE_NAME":           "env-service",
			},
			writer: &OTLPWriter{
				Endpoint:    "https://config.com:4317",
				ServiceName: "config-service",
			},
			expected: &OTLPWriter{
				Endpoint:    "https://config.com:4317",
				ServiceName: "config-service",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables
			for k, v := range tt.envVars {
				os.Setenv(k, v)
				defer os.Unsetenv(k)
			}

			ctx := caddy.Context{
				Context: context.Background(),
			}

			err := tt.writer.Provision(ctx)
			require.NoError(t, err)

			// Check expected values
			if tt.expected.Endpoint != "" {
				assert.Equal(t, tt.expected.Endpoint, tt.writer.Endpoint)
			}
			if tt.expected.Protocol != "" {
				assert.Equal(t, tt.expected.Protocol, tt.writer.Protocol)
			}
			if tt.expected.Headers != nil {
				assert.Equal(t, tt.expected.Headers, tt.writer.Headers)
			}
			if tt.expected.Timeout != 0 {
				assert.Equal(t, tt.expected.Timeout, tt.writer.Timeout)
			}
			if tt.expected.ServiceName != "" {
				assert.Equal(t, tt.expected.ServiceName, tt.writer.ServiceName)
			}
			if tt.expected.Insecure {
				assert.Equal(t, tt.expected.Insecure, tt.writer.Insecure)
			}
			if tt.expected.Compression != "" {
				assert.Equal(t, tt.expected.Compression, tt.writer.Compression)
			}
			if tt.expected.CACert != "" {
				assert.Equal(t, tt.expected.CACert, tt.writer.CACert)
			}
			if tt.expected.ClientCert != "" {
				assert.Equal(t, tt.expected.ClientCert, tt.writer.ClientCert)
			}
			if tt.expected.ClientKey != "" {
				assert.Equal(t, tt.expected.ClientKey, tt.writer.ClientKey)
			}
			if tt.expected.ResourceAttributes != nil {
				assert.Equal(t, tt.expected.ResourceAttributes, tt.writer.ResourceAttributes)
			}
		})
	}
}
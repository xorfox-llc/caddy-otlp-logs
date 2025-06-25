package caddyotlplogs

import (
	"testing"
)

func TestOTLPWriter_normalizeEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		isHTTP   bool
		insecure bool
		expected string
	}{
		// gRPC tests
		{
			name:     "grpc empty endpoint",
			endpoint: "",
			isHTTP:   false,
			expected: "localhost:4317",
		},
		{
			name:     "grpc host:port without scheme",
			endpoint: "otel-collector:4317",
			isHTTP:   false,
			expected: "otel-collector:4317",
		},
		{
			name:     "grpc with http scheme",
			endpoint: "http://otel-collector:4317",
			isHTTP:   false,
			expected: "otel-collector:4317",
		},
		{
			name:     "grpc with https scheme",
			endpoint: "https://otel-collector:4317",
			isHTTP:   false,
			expected: "otel-collector:4317",
		},
		{
			name:     "grpc with grpc scheme",
			endpoint: "grpc://otel-collector:4317",
			isHTTP:   false,
			expected: "otel-collector:4317",
		},
		{
			name:     "grpc with grpcs scheme",
			endpoint: "grpcs://otel-collector:4317",
			isHTTP:   false,
			expected: "otel-collector:4317",
		},
		
		// HTTP tests
		{
			name:     "http empty endpoint",
			endpoint: "",
			isHTTP:   true,
			expected: "http://localhost:4318/v1/logs",
		},
		{
			name:     "http host:port without scheme (insecure)",
			endpoint: "otel-collector:4318",
			isHTTP:   true,
			insecure: true,
			expected: "http://otel-collector:4318/v1/logs",
		},
		{
			name:     "http host:port without scheme (secure)",
			endpoint: "otel-collector:4318",
			isHTTP:   true,
			insecure: false,
			expected: "https://otel-collector:4318/v1/logs",
		},
		{
			name:     "http with http scheme",
			endpoint: "http://otel-collector:4318",
			isHTTP:   true,
			expected: "http://otel-collector:4318/v1/logs",
		},
		{
			name:     "http with https scheme",
			endpoint: "https://otel-collector:4318",
			isHTTP:   true,
			expected: "https://otel-collector:4318/v1/logs",
		},
		{
			name:     "http with path without /v1/logs",
			endpoint: "http://otel-collector:4318/otlp",
			isHTTP:   true,
			expected: "http://otel-collector:4318/otlp/v1/logs",
		},
		{
			name:     "http with path already containing /v1/logs",
			endpoint: "http://otel-collector:4318/v1/logs",
			isHTTP:   true,
			expected: "http://otel-collector:4318/v1/logs",
		},
		{
			name:     "http with custom path ending with /v1/logs",
			endpoint: "http://otel-collector:4318/custom/v1/logs",
			isHTTP:   true,
			expected: "http://otel-collector:4318/custom/v1/logs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &OTLPWriter{
				Endpoint: tt.endpoint,
				Insecure: tt.insecure,
			}
			result := w.normalizeEndpoint(tt.endpoint, tt.isHTTP)
			if result != tt.expected {
				t.Errorf("normalizeEndpoint() = %v, want %v", result, tt.expected)
			}
		})
	}
}
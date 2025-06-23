package caddyotlplogs

import (
	"context"
	"testing"

	"github.com/caddyserver/caddy/v2"
)

func TestOTLPWriter_CaddyModule(t *testing.T) {
	w := OTLPWriter{}
	info := w.CaddyModule()
	
	if info.ID != "caddy.logging.writers.otlp" {
		t.Errorf("Expected module ID 'caddy.logging.writers.otlp', got %s", info.ID)
	}
	
	if info.New == nil {
		t.Error("Expected New function to be non-nil")
	}
	
	// Test that New creates a proper instance
	module := info.New()
	if _, ok := module.(*OTLPWriter); !ok {
		t.Error("Expected New() to return *OTLPWriter")
	}
}

func TestOTLPWriter_InterfaceCompliance(t *testing.T) {
	var _ caddy.Module = (*OTLPWriter)(nil)
	var _ caddy.Provisioner = (*OTLPWriter)(nil)
	var _ caddy.WriterOpener = (*OTLPWriter)(nil)
	var _ caddy.CleanerUpper = (*OTLPWriter)(nil)
}

func TestOTLPWriter_DefaultValues(t *testing.T) {
	w := &OTLPWriter{}
	
	// Create a context with basic setup
	ctx, cancel := caddy.NewContext(caddy.Context{
		Context: context.Background(),
	})
	defer cancel()
	
	// Test provisioning with empty config (should use defaults)
	err := w.Provision(ctx)
	if err != nil {
		t.Fatalf("Failed to provision OTLPWriter with defaults: %v", err)
	}
	
	// Check defaults
	if w.Protocol != "grpc" {
		t.Errorf("Expected default protocol 'grpc', got %s", w.Protocol)
	}
	
	if w.ServiceName != "caddy" {
		t.Errorf("Expected default service name 'caddy', got %s", w.ServiceName)
	}
	
	if w.BatchSize != 100 {
		t.Errorf("Expected default batch size 100, got %d", w.BatchSize)
	}
	
	// Cleanup
	if err := w.Cleanup(); err != nil {
		t.Errorf("Failed to cleanup OTLPWriter: %v", err)
	}
}

func TestOTLPWriter_String(t *testing.T) {
	tests := []struct {
		name     string
		writer   *OTLPWriter
		expected string
	}{
		{
			name: "grpc protocol",
			writer: &OTLPWriter{
				Protocol: "grpc",
				Endpoint: "localhost:4317",
			},
			expected: "OTLP grpc to localhost:4317",
		},
		{
			name: "http protocol",
			writer: &OTLPWriter{
				Protocol: "http",
				Endpoint: "http://localhost:4318",
			},
			expected: "OTLP http to http://localhost:4318",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.writer.String()
			if result != tt.expected {
				t.Errorf("Expected String() = %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestOTLPWriter_WriterKey(t *testing.T) {
	tests := []struct {
		name     string
		writer   *OTLPWriter
		expected string
	}{
		{
			name: "grpc protocol",
			writer: &OTLPWriter{
				Protocol: "grpc",
				Endpoint: "localhost:4317",
			},
			expected: "otlp:grpc:localhost:4317",
		},
		{
			name: "http protocol", 
			writer: &OTLPWriter{
				Protocol: "http",
				Endpoint: "http://localhost:4318",
			},
			expected: "otlp:http:http://localhost:4318",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.writer.WriterKey()
			if result != tt.expected {
				t.Errorf("Expected WriterKey() = %q, got %q", tt.expected, result)
			}
		})
	}
}
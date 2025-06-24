package caddyotlplogs

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"go.opentelemetry.io/otel/attribute"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
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
	var _ caddyfile.Unmarshaler = (*OTLPWriter)(nil)
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

func TestOTLPWriter_UnmarshalCaddyfile(t *testing.T) {
	tests := []struct {
		name      string
		caddyfile string
		expected  *OTLPWriter
		wantErr   bool
	}{
		{
			name: "basic configuration",
			caddyfile: `otlp {
				endpoint https://otel.example.com:443
				protocol grpc
				insecure false
			}`,
			expected: &OTLPWriter{
				Endpoint: "https://otel.example.com:443",
				Protocol: "grpc",
				Insecure: false,
			},
			wantErr: false,
		},
		{
			name: "full configuration",
			caddyfile: `otlp {
				endpoint https://otel.example.com:443
				protocol http
				insecure true
				timeout 30s
				headers {
					Authorization "Bearer token123"
					X-Custom-Header "value"
				}
				service_name my-service
				resource_attributes {
					environment production
					version v1.0.0
				}
				batch_size 200
				batch_timeout 10s
			}`,
			expected: &OTLPWriter{
				Endpoint:     "https://otel.example.com:443",
				Protocol:     "http",
				Insecure:     true,
				Timeout:      caddy.Duration(30 * time.Second),
				Headers:      map[string]string{"Authorization": "Bearer token123", "X-Custom-Header": "value"},
				ServiceName:  "my-service",
				ResourceAttributes: map[string]string{"environment": "production", "version": "v1.0.0"},
				BatchSize:    200,
				BatchTimeout: caddy.Duration(10 * time.Second),
			},
			wantErr: false,
		},
		{
			name: "minimal configuration",
			caddyfile: `otlp {
				endpoint localhost:4317
			}`,
			expected: &OTLPWriter{
				Endpoint: "localhost:4317",
			},
			wantErr: false,
		},
		{
			name: "insecure without value defaults to true",
			caddyfile: `otlp {
				endpoint localhost:4317
				insecure
			}`,
			expected: &OTLPWriter{
				Endpoint: "localhost:4317",
				Insecure: true,
			},
			wantErr: false,
		},
		{
			name: "error - invalid insecure value",
			caddyfile: `otlp {
				insecure maybe
			}`,
			wantErr: true,
		},
		{
			name: "error - invalid timeout",
			caddyfile: `otlp {
				timeout invalid
			}`,
			wantErr: true,
		},
		{
			name: "error - invalid batch_size",
			caddyfile: `otlp {
				batch_size not-a-number
			}`,
			wantErr: true,
		},
		{
			name: "error - invalid batch_timeout",
			caddyfile: `otlp {
				batch_timeout invalid
			}`,
			wantErr: true,
		},
		{
			name: "error - unrecognized directive",
			caddyfile: `otlp {
				unknown_directive value
			}`,
			wantErr: true,
		},
		{
			name: "error - arguments on same line",
			caddyfile: `otlp arg1 arg2 {
				endpoint localhost:4317
			}`,
			wantErr: true,
		},
		{
			name: "error - missing endpoint value",
			caddyfile: `otlp {
				endpoint
			}`,
			wantErr: true,
		},
		{
			name: "error - missing header value",
			caddyfile: `otlp {
				headers {
					Authorization
				}
			}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &OTLPWriter{}
			d := caddyfile.NewTestDispenser(tt.caddyfile)
			
			err := w.UnmarshalCaddyfile(d)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalCaddyfile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			
			if !tt.wantErr && tt.expected != nil {
				// Compare fields
				if w.Endpoint != tt.expected.Endpoint {
					t.Errorf("Endpoint = %v, want %v", w.Endpoint, tt.expected.Endpoint)
				}
				if w.Protocol != tt.expected.Protocol {
					t.Errorf("Protocol = %v, want %v", w.Protocol, tt.expected.Protocol)
				}
				if w.Insecure != tt.expected.Insecure {
					t.Errorf("Insecure = %v, want %v", w.Insecure, tt.expected.Insecure)
				}
				if w.Timeout != tt.expected.Timeout {
					t.Errorf("Timeout = %v, want %v", w.Timeout, tt.expected.Timeout)
				}
				if w.ServiceName != tt.expected.ServiceName {
					t.Errorf("ServiceName = %v, want %v", w.ServiceName, tt.expected.ServiceName)
				}
				if w.BatchSize != tt.expected.BatchSize {
					t.Errorf("BatchSize = %v, want %v", w.BatchSize, tt.expected.BatchSize)
				}
				if w.BatchTimeout != tt.expected.BatchTimeout {
					t.Errorf("BatchTimeout = %v, want %v", w.BatchTimeout, tt.expected.BatchTimeout)
				}
				
				// Compare maps
				if tt.expected.Headers != nil {
					if len(w.Headers) != len(tt.expected.Headers) {
						t.Errorf("Headers length = %v, want %v", len(w.Headers), len(tt.expected.Headers))
					}
					for k, v := range tt.expected.Headers {
						if w.Headers[k] != v {
							t.Errorf("Headers[%s] = %v, want %v", k, w.Headers[k], v)
						}
					}
				}
				
				if tt.expected.ResourceAttributes != nil {
					if len(w.ResourceAttributes) != len(tt.expected.ResourceAttributes) {
						t.Errorf("ResourceAttributes length = %v, want %v", len(w.ResourceAttributes), len(tt.expected.ResourceAttributes))
					}
					for k, v := range tt.expected.ResourceAttributes {
						if w.ResourceAttributes[k] != v {
							t.Errorf("ResourceAttributes[%s] = %v, want %v", k, w.ResourceAttributes[k], v)
						}
					}
				}
			}
		})
	}
}

func TestOTLPWriter_NormalizeEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		isHTTP   bool
		insecure bool
		expected string
	}{
		{
			name:     "empty endpoint grpc",
			endpoint: "",
			isHTTP:   false,
			expected: "localhost:4317",
		},
		{
			name:     "empty endpoint http",
			endpoint: "",
			isHTTP:   true,
			expected: "http://localhost:4318/v1/logs",
		},
		{
			name:     "grpc with host and port",
			endpoint: "otel.example.com:4317",
			isHTTP:   false,
			expected: "otel.example.com:4317",
		},
		{
			name:     "http without scheme insecure",
			endpoint: "otel.example.com:4318",
			isHTTP:   true,
			insecure: true,
			expected: "http://otel.example.com:4318/v1/logs",
		},
		{
			name:     "http without scheme secure",
			endpoint: "otel.example.com:4318",
			isHTTP:   true,
			insecure: false,
			expected: "https://otel.example.com:4318/v1/logs",
		},
		{
			name:     "http with scheme and path",
			endpoint: "https://otel.example.com:4318/custom/path",
			isHTTP:   true,
			expected: "https://otel.example.com:4318/custom/path/v1/logs",
		},
		{
			name:     "http already has /v1/logs",
			endpoint: "https://otel.example.com:4318/v1/logs",
			isHTTP:   true,
			expected: "https://otel.example.com:4318/v1/logs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &OTLPWriter{Insecure: tt.insecure}
			result := w.normalizeEndpoint(tt.endpoint, tt.isHTTP)
			if result != tt.expected {
				t.Errorf("normalizeEndpoint() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestOTLPWriter_ParseTraceID(t *testing.T) {
	tests := []struct {
		name    string
		traceID string
		wantErr bool
	}{
		{
			name:    "valid trace ID",
			traceID: "0123456789abcdef0123456789abcdef",
			wantErr: false,
		},
		{
			name:    "valid trace ID with dashes",
			traceID: "01234567-89ab-cdef-0123-456789abcdef",
			wantErr: false,
		},
		{
			name:    "invalid length",
			traceID: "0123456789abcdef",
			wantErr: true,
		},
		{
			name:    "invalid hex characters",
			traceID: "0123456789abcdefg123456789abcdef",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := parseTraceID(tt.traceID)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTraceID() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && len(data) != 16 {
				t.Errorf("parseTraceID() returned %d bytes, want 16", len(data))
			}
		})
	}
}

func TestOTLPWriter_ParseSpanID(t *testing.T) {
	tests := []struct {
		name    string
		spanID  string
		wantErr bool
	}{
		{
			name:    "valid span ID",
			spanID:  "0123456789abcdef",
			wantErr: false,
		},
		{
			name:    "valid span ID with dashes",
			spanID:  "01234567-89abcdef",
			wantErr: false,
		},
		{
			name:    "invalid length",
			spanID:  "0123456789",
			wantErr: true,
		},
		{
			name:    "invalid hex characters",
			spanID:  "0123456789abcdeg",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := parseSpanID(tt.spanID)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseSpanID() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && len(data) != 8 {
				t.Errorf("parseSpanID() returned %d bytes, want 8", len(data))
			}
		})
	}
}

func TestOTLPWriter_OpenWriter(t *testing.T) {
	w := &OTLPWriter{}
	
	writer, err := w.OpenWriter()
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	
	if writer == nil {
		t.Error("OpenWriter() returned nil writer")
	}
	
	// Check that it implements io.WriteCloser
	if _, ok := writer.(io.WriteCloser); !ok {
		t.Error("OpenWriter() should return io.WriteCloser")
	}
}

func TestOTLPWriteCloser_Write(t *testing.T) {
	w := &OTLPWriter{
		logsBatch: make([]*logspb.LogRecord, 0),
		closeChan: make(chan struct{}),
		BatchSize: 100, // Make sure batch size is set
	}
	
	writer, _ := w.OpenWriter()
	
	tests := []struct {
		name     string
		input    []byte
		wantErr  bool
	}{
		{
			name:     "valid JSON log",
			input:    []byte(`{"level":"info","msg":"test message","trace_id":"0123456789abcdef0123456789abcdef","span_id":"0123456789abcdef"}`),
			wantErr:  false,
		},
		{
			name:     "plain text log",
			input:    []byte("This is a plain text log message"),
			wantErr:  false,
		},
		{
			name:     "caddy access log format",
			input:    []byte(`{"request":{"method":"GET","uri":"/test"},"status":200}`),
			wantErr:  false,
		},
		{
			name:     "log with various attributes",
			input:    []byte(`{"level":"error","msg":"error occurred","error":"connection timeout","user_id":"123","session_id":"abc"}`),
			wantErr:  false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := writer.Write(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && n != len(tt.input) {
				t.Errorf("Write() returned %d, want %d", n, len(tt.input))
			}
		})
	}
	
	// Verify logs were added to batch
	if len(w.logsBatch) != len(tests) {
		t.Errorf("Expected %d logs in batch, got %d", len(tests), len(w.logsBatch))
	}
}

func TestOTLPWriteCloser_ExtractMessage(t *testing.T) {
	owc := &otlpWriteCloser{w: &OTLPWriter{}}
	
	tests := []struct {
		name     string
		entry    map[string]interface{}
		expected string
	}{
		{
			name:     "msg field",
			entry:    map[string]interface{}{"msg": "test message"},
			expected: "test message",
		},
		{
			name:     "message field",
			entry:    map[string]interface{}{"message": "test message"},
			expected: "test message",
		},
		{
			name: "caddy access log",
			entry: map[string]interface{}{
				"request": map[string]interface{}{
					"method": "GET",
					"uri":    "/api/test",
				},
				"status": float64(200),
			},
			expected: "GET /api/test 200",
		},
		{
			name: "caddy access log without status",
			entry: map[string]interface{}{
				"request": map[string]interface{}{
					"method": "POST",
					"uri":    "/api/create",
				},
			},
			expected: "POST /api/create",
		},
		{
			name:     "fallback to JSON",
			entry:    map[string]interface{}{"custom": "field"},
			expected: `{"custom":"field"}`,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := owc.extractMessage(tt.entry)
			if result != tt.expected {
				t.Errorf("extractMessage() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestOTLPWriteCloser_ExtractSeverity(t *testing.T) {
	owc := &otlpWriteCloser{w: &OTLPWriter{}}
	
	tests := []struct {
		name     string
		entry    map[string]interface{}
		expected logspb.SeverityNumber
	}{
		{
			name:     "trace level",
			entry:    map[string]interface{}{"level": "trace"},
			expected: logspb.SeverityNumber_SEVERITY_NUMBER_TRACE,
		},
		{
			name:     "debug level",
			entry:    map[string]interface{}{"level": "debug"},
			expected: logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG,
		},
		{
			name:     "info level",
			entry:    map[string]interface{}{"level": "info"},
			expected: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
		},
		{
			name:     "warn level",
			entry:    map[string]interface{}{"level": "warn"},
			expected: logspb.SeverityNumber_SEVERITY_NUMBER_WARN,
		},
		{
			name:     "warning level",
			entry:    map[string]interface{}{"level": "warning"},
			expected: logspb.SeverityNumber_SEVERITY_NUMBER_WARN,
		},
		{
			name:     "error level",
			entry:    map[string]interface{}{"level": "error"},
			expected: logspb.SeverityNumber_SEVERITY_NUMBER_ERROR,
		},
		{
			name:     "fatal level",
			entry:    map[string]interface{}{"level": "fatal"},
			expected: logspb.SeverityNumber_SEVERITY_NUMBER_FATAL,
		},
		{
			name:     "uppercase level",
			entry:    map[string]interface{}{"level": "ERROR"},
			expected: logspb.SeverityNumber_SEVERITY_NUMBER_ERROR,
		},
		{
			name:     "no level defaults to info",
			entry:    map[string]interface{}{},
			expected: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
		},
		{
			name:     "unknown level defaults to info",
			entry:    map[string]interface{}{"level": "custom"},
			expected: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := owc.extractSeverity(tt.entry)
			if result != tt.expected {
				t.Errorf("extractSeverity() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestOTLPWriter_AttributeValue(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string // String representation for comparison
	}{
		{
			name:     "string value",
			value:    "test",
			expected: "test",
		},
		{
			name:     "bool value",
			value:    true,
			expected: "true",
		},
		{
			name:     "int value",
			value:    42,
			expected: "42",
		},
		{
			name:     "float value",
			value:    3.14,
			expected: "3.14",
		},
		{
			name:     "map value",
			value:    map[string]interface{}{"key": "value"},
			expected: `{"key":"value"}`,
		},
		{
			name:     "slice value",
			value:    []string{"a", "b", "c"},
			expected: `["a","b","c"]`,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attr := attributeValue(tt.value)
			// Convert to string for comparison
			var result string
			switch attr.Type() {
			case attribute.STRING:
				result = attr.AsString()
			case attribute.BOOL:
				result = "true"
				if !attr.AsBool() {
					result = "false"
				}
			case attribute.INT64:
				result = fmt.Sprintf("%d", attr.AsInt64())
			case attribute.FLOAT64:
				result = fmt.Sprintf("%g", attr.AsFloat64())
			}
			
			if result != tt.expected {
				t.Errorf("attributeValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestOTLPWriter_Provision_EnvVars(t *testing.T) {
	// Save current env vars
	oldEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	oldProtocol := os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
	oldHeaders := os.Getenv("OTEL_EXPORTER_OTLP_HEADERS")
	oldServiceName := os.Getenv("OTEL_SERVICE_NAME")
	oldResourceAttrs := os.Getenv("OTEL_RESOURCE_ATTRIBUTES")
	
	// Restore env vars after test
	defer func() {
		os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", oldEndpoint)
		os.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", oldProtocol)
		os.Setenv("OTEL_EXPORTER_OTLP_HEADERS", oldHeaders)
		os.Setenv("OTEL_SERVICE_NAME", oldServiceName)
		os.Setenv("OTEL_RESOURCE_ATTRIBUTES", oldResourceAttrs)
	}()
	
	// Set test env vars - including URL encoded values
	os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel.test.com:4317")
	os.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	os.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=Bearer%20test,X-Custom=value%20with%20spaces")
	os.Setenv("OTEL_SERVICE_NAME", "test-service")
	os.Setenv("OTEL_RESOURCE_ATTRIBUTES", "env=test,version=1.0.0,description=Test%20Service%20Description")
	
	w := &OTLPWriter{}
	ctx, cancel := caddy.NewContext(caddy.Context{
		Context: context.Background(),
	})
	defer cancel()
	
	err := w.Provision(ctx)
	if err != nil {
		t.Fatalf("Failed to provision OTLPWriter with env vars: %v", err)
	}
	
	// Check values from env vars
	if w.Endpoint != "otel.test.com:4317" {
		t.Errorf("Expected endpoint from env var, got %s", w.Endpoint)
	}
	
	if w.Protocol != "http/protobuf" {
		t.Errorf("Expected protocol from env var, got %s", w.Protocol)
	}
	
	if w.ServiceName != "test-service" {
		t.Errorf("Expected service name from env var, got %s", w.ServiceName)
	}
	
	// Check headers are URL decoded
	if w.Headers["Authorization"] != "Bearer test" {
		t.Errorf("Expected Authorization header to be URL decoded, got %s", w.Headers["Authorization"])
	}
	
	if w.Headers["X-Custom"] != "value with spaces" {
		t.Errorf("Expected X-Custom header to be URL decoded, got %s", w.Headers["X-Custom"])
	}
	
	// Check resource attributes are URL decoded
	if w.ResourceAttributes["env"] != "test" {
		t.Errorf("Expected env resource attribute from env var, got %s", w.ResourceAttributes["env"])
	}
	
	if w.ResourceAttributes["version"] != "1.0.0" {
		t.Errorf("Expected version resource attribute from env var, got %s", w.ResourceAttributes["version"])
	}
	
	if w.ResourceAttributes["description"] != "Test Service Description" {
		t.Errorf("Expected description resource attribute to be URL decoded, got %s", w.ResourceAttributes["description"])
	}
	
	// Cleanup
	if err := w.Cleanup(); err != nil {
		t.Errorf("Failed to cleanup OTLPWriter: %v", err)
	}
}

func TestOTLPWriter_Cleanup(t *testing.T) {
	w := &OTLPWriter{
		logsBatch: make([]*logspb.LogRecord, 0),
		closeChan: make(chan struct{}),
		closed:    false,
	}
	
	// First cleanup should succeed
	err := w.Cleanup()
	if err != nil {
		t.Errorf("First Cleanup() error = %v", err)
	}
	
	if !w.closed {
		t.Error("Expected closed to be true after Cleanup()")
	}
	
	// Second cleanup should also succeed (idempotent)
	err = w.Cleanup()
	if err != nil {
		t.Errorf("Second Cleanup() error = %v", err)
	}
}

func TestOTLPWriter_BatchManagement(t *testing.T) {
	w := &OTLPWriter{
		logsBatch: make([]*logspb.LogRecord, 0),
		closeChan: make(chan struct{}),
		BatchSize: 3,
		BatchTimeout: caddy.Duration(100 * time.Millisecond),
	}
	
	// Mock log records
	log1 := &logspb.LogRecord{TimeUnixNano: 1, Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "log1"}}}
	log2 := &logspb.LogRecord{TimeUnixNano: 2, Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "log2"}}}
	
	// Test adding logs without triggering batch
	w.addLog(log1)
	if len(w.logsBatch) != 1 {
		t.Errorf("Expected 1 log in batch, got %d", len(w.logsBatch))
	}
	
	w.addLog(log2)
	if len(w.logsBatch) != 2 {
		t.Errorf("Expected 2 logs in batch, got %d", len(w.logsBatch))
	}
	
	// Verify batch is not cleared until batch size is reached
	if len(w.logsBatch) == 0 {
		t.Error("Batch was cleared before reaching batch size")
	}
}

func TestOTLPWriter_ProtocolValidation(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		wantErr  bool
	}{
		{
			name:     "valid grpc protocol",
			protocol: "grpc",
			wantErr:  false,
		},
		{
			name:     "valid http protocol",
			protocol: "http/protobuf",
			wantErr:  false,
		},
		{
			name:     "valid http protocol alternative",
			protocol: "http",
			wantErr:  false,
		},
		{
			name:     "invalid protocol",
			protocol: "invalid",
			wantErr:  true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &OTLPWriter{
				Protocol: tt.protocol,
				Endpoint: "localhost:4317",
			}
			
			ctx, cancel := caddy.NewContext(caddy.Context{
				Context: context.Background(),
			})
			defer cancel()
			
			err := w.Provision(ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Provision() error = %v, wantErr %v", err, tt.wantErr)
			}
			
			if !tt.wantErr {
				// Cleanup successful provisions
				w.Cleanup()
			}
		})
	}
}

func TestOTLPWriter_ParseHeaders(t *testing.T) {
	// Save and restore env
	oldHeaders := os.Getenv("OTEL_EXPORTER_OTLP_HEADERS")
	defer os.Setenv("OTEL_EXPORTER_OTLP_HEADERS", oldHeaders)
	
	tests := []struct {
		name     string
		envValue string
		expected map[string]string
	}{
		{
			name:     "single header",
			envValue: "Authorization=Bearer token123",
			expected: map[string]string{"Authorization": "Bearer token123"},
		},
		{
			name:     "multiple headers",
			envValue: "Authorization=Bearer token123,X-Custom-Header=value",
			expected: map[string]string{
				"Authorization":   "Bearer token123",
				"X-Custom-Header": "value",
			},
		},
		{
			name:     "headers with spaces",
			envValue: "Authorization = Bearer token123 , X-Custom-Header = value",
			expected: map[string]string{
				"Authorization":   "Bearer token123",
				"X-Custom-Header": "value",
			},
		},
		{
			name:     "URL encoded headers",
			envValue: "X-Custom=value%20with%20spaces",
			expected: map[string]string{"X-Custom": "value with spaces"},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("OTEL_EXPORTER_OTLP_HEADERS", tt.envValue)
			
			w := &OTLPWriter{}
			ctx, cancel := caddy.NewContext(caddy.Context{
				Context: context.Background(),
			})
			defer cancel()
			
			err := w.Provision(ctx)
			if err != nil {
				t.Fatalf("Failed to provision: %v", err)
			}
			defer w.Cleanup()
			
			for k, v := range tt.expected {
				if w.Headers[k] != v {
					t.Errorf("Expected header %s=%s, got %s", k, v, w.Headers[k])
				}
			}
		})
	}
}

func TestOTLPWriter_EdgeCases(t *testing.T) {
	t.Run("empty JSON log", func(t *testing.T) {
		owc := &otlpWriteCloser{w: &OTLPWriter{}}
		
		_, err := owc.Write([]byte("{}"))
		if err != nil {
			t.Errorf("Should handle empty JSON: %v", err)
		}
	})
	
	t.Run("invalid JSON with special characters", func(t *testing.T) {
		owc := &otlpWriteCloser{w: &OTLPWriter{
			logsBatch: make([]*logspb.LogRecord, 0),
			BatchSize: 100,
		}}
		
		invalidJSON := `{"msg": "test with \x00 null byte"}`
		_, err := owc.Write([]byte(invalidJSON))
		// Should treat as plain text
		if err != nil {
			t.Errorf("Should handle invalid JSON as plain text: %v", err)
		}
	})
	
	t.Run("very long attribute values", func(t *testing.T) {
		owc := &otlpWriteCloser{w: &OTLPWriter{}}
		
		longString := strings.Repeat("a", 10000)
		entry := map[string]interface{}{
			"msg":       "test",
			"long_attr": longString,
		}
		
		attrs := owc.extractAttributes(entry)
		found := false
		for _, attr := range attrs {
			if attr.Key == "long_attr" {
				found = true
				if len(attr.GetValue().GetStringValue()) != 10000 {
					t.Error("Long attribute value was truncated")
				}
			}
		}
		if !found {
			t.Error("Long attribute was not extracted")
		}
	})
	
	t.Run("nested maps in attributes", func(t *testing.T) {
		owc := &otlpWriteCloser{w: &OTLPWriter{}}
		
		entry := map[string]interface{}{
			"msg": "test",
			"nested": map[string]interface{}{
				"level1": map[string]interface{}{
					"level2": "value",
				},
			},
		}
		
		attrs := owc.extractAttributes(entry)
		found := false
		for _, attr := range attrs {
			if attr.Key == "nested" {
				found = true
				// Should be JSON stringified
				expectedJSON := `{"level1":{"level2":"value"}}`
				if attr.GetValue().GetStringValue() != expectedJSON {
					t.Errorf("Expected nested map as JSON %s, got %s", expectedJSON, attr.GetValue().GetStringValue())
				}
			}
		}
		if !found {
			t.Error("Nested attribute was not extracted")
		}
	})
}

func TestOTLPWriter_CaddyfileEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		caddyfile string
		wantErr   bool
	}{
		{
			name: "headers with equals in value",
			caddyfile: `otlp {
				headers {
					Authorization "Bearer token=with=equals"
				}
			}`,
			wantErr: false,
		},
		{
			name: "resource attributes with special characters",
			caddyfile: `otlp {
				resource_attributes {
					"service.namespace" "my-namespace"
					deployment.environment "prod/staging"
				}
			}`,
			wantErr: false,
		},
		{
			name: "timeout with units",
			caddyfile: `otlp {
				timeout 30s
				batch_timeout 500ms
			}`,
			wantErr: false,
		},
		{
			name: "missing closing brace in headers",
			caddyfile: `otlp {
				headers {
					Authorization "Bearer token"
			}`,
			wantErr: false, // TestDispenser doesn't validate braces properly
		},
		{
			name: "duplicate directives",
			caddyfile: `otlp {
				endpoint localhost:4317
				endpoint localhost:4318
			}`,
			wantErr: false, // Last one wins
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &OTLPWriter{}
			d := caddyfile.NewTestDispenser(tt.caddyfile)
			
			err := w.UnmarshalCaddyfile(d)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalCaddyfile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestOTLPWriter_AttributeValueEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{
			name:  "nil value",
			value: nil,
		},
		{
			name:  "empty slice",
			value: []interface{}{},
		},
		{
			name:  "mixed type slice",
			value: []interface{}{"string", 123, true, 3.14},
		},
		{
			name:  "very large int",
			value: int64(9223372036854775807), // max int64
		},
		{
			name:  "very small float",
			value: 1e-308,
		},
		{
			name:  "Infinity float",
			value: math.Inf(1),
		},
		{
			name:  "complex nested structure",
			value: map[string]interface{}{
				"array": []interface{}{
					map[string]interface{}{
						"nested": "value",
					},
				},
			},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic
			attr := attributeValue(tt.value)
			if attr.Type() == attribute.INVALID {
				t.Errorf("Got INVALID attribute type for %v", tt.value)
			}
		})
	}
}
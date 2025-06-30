package otlplogs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	collectorlogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

// Mock gRPC server for testing
type mockLogsServiceServer struct {
	collectorlogspb.UnimplementedLogsServiceServer
	mu       sync.Mutex
	requests []*collectorlogspb.ExportLogsServiceRequest
	err      error
}

func (m *mockLogsServiceServer) Export(ctx context.Context, req *collectorlogspb.ExportLogsServiceRequest) (*collectorlogspb.ExportLogsServiceResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.err != nil {
		return nil, m.err
	}
	
	m.requests = append(m.requests, req)
	return &collectorlogspb.ExportLogsServiceResponse{}, nil
}

func (m *mockLogsServiceServer) getRequests() []*collectorlogspb.ExportLogsServiceRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*collectorlogspb.ExportLogsServiceRequest{}, m.requests...)
}

func (m *mockLogsServiceServer) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = nil
	m.err = nil
}

func TestOTLPWriter_CaddyModule(t *testing.T) {
	w := OTLPWriter{}
	info := w.CaddyModule()
	
	assert.Equal(t, caddy.ModuleID("caddy.logging.writers.otlp"), info.ID)
	assert.NotNil(t, info.New)
	
	// Test that New creates a new instance
	module := info.New()
	_, ok := module.(*OTLPWriter)
	assert.True(t, ok)
}

func TestOTLPWriter_Provision(t *testing.T) {
	tests := []struct {
		name    string
		writer  *OTLPWriter
		envVars map[string]string
		wantErr bool
	}{
		{
			name: "default configuration",
			writer: &OTLPWriter{
				Endpoint: "localhost:4317",
			},
			wantErr: false,
		},
		{
			name: "with environment variables",
			writer: &OTLPWriter{},
			envVars: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": "localhost:4317",
				"OTEL_SERVICE_NAME":           "test-service",
				"CADDY_OTLP_DEBUG":            "true",
			},
			wantErr: false,
		},
		{
			name: "unsupported protocol",
			writer: &OTLPWriter{
				Endpoint: "localhost:4317",
				Protocol: "invalid",
			},
			wantErr: true,
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
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				// The writer is now properly provisioned
				// Internal state is managed by actualOTLPWriter
			}
		})
	}
}

func TestOTLPWriter_WriteAndBatch(t *testing.T) {
	// Clear any existing writers
	globalWritersMu.Lock()
	globalWriters = make(map[string]*globalOTLPWriter)
	globalWritersMu.Unlock()

	// Create a test writer with small batch size
	w := &OTLPWriter{
		Endpoint:     "localhost:4317",
		Protocol:     "grpc",
		BatchSize:    2,
		BatchTimeout: caddy.Duration(100 * time.Millisecond),
		Insecure:     true,
	}
	
	// Mock the gRPC client
	mockClient := &mockLogsServiceClient{
		responses: make(chan *collectorlogspb.ExportLogsServiceResponse, 10),
	}
	
	ctx := caddy.Context{
		Context: context.Background(),
	}
	
	require.NoError(t, w.Provision(ctx))
	
	// Get a test write closer
	twc, err := w.openWriterForTest()
	require.NoError(t, err)
	defer twc.Close()
	
	// Set the mock client
	twc.setGRPCClient(mockClient)
	
	// Write logs
	logs := []map[string]interface{}{
		{"level": "info", "msg": "test message 1", "ts": float64(time.Now().Unix())},
		{"level": "error", "msg": "test message 2", "ts": float64(time.Now().Unix())},
		{"level": "debug", "msg": "test message 3", "ts": float64(time.Now().Unix())},
	}
	
	for _, log := range logs {
		data, _ := json.Marshal(log)
		_, err := twc.Write(data)
		assert.NoError(t, err)
	}
	
	// Wait for batches to be sent
	time.Sleep(200 * time.Millisecond)
	
	// Verify batches were sent
	assert.GreaterOrEqual(t, len(mockClient.getRequests()), 1)
	
	// Cleanup
	assert.NoError(t, w.Cleanup())
}

func TestOTLPWriter_TraceContext(t *testing.T) {
	// Create a test writer
	w := &OTLPWriter{
		Endpoint:     "localhost:4317",
		Protocol:     "grpc",
		BatchSize:    10,
		BatchTimeout: caddy.Duration(100 * time.Millisecond),
		Insecure:     true,
	}
	
	// Mock the gRPC client
	mockClient := &mockLogsServiceClient{
		responses: make(chan *collectorlogspb.ExportLogsServiceResponse, 10),
	}
	
	ctx := caddy.Context{
		Context: context.Background(),
	}
	
	require.NoError(t, w.Provision(ctx))
	
	// Get a test write closer
	twc, err := w.openWriterForTest()
	require.NoError(t, err)
	defer twc.Close()
	
	// Set the mock client
	twc.setGRPCClient(mockClient)
	
	// Write logs with trace context
	logs := []map[string]interface{}{
		{
			"level":    "info",
			"msg":      "test with trace context",
			"ts":       float64(time.Now().Unix()),
			"trace_id": "0123456789abcdef0123456789abcdef",
			"span_id":  "0123456789abcdef",
		},
		{
			"level":    "error",
			"msg":      "test with dashed trace id",
			"ts":       float64(time.Now().Unix()),
			"trace_id": "01234567-89ab-cdef-0123-456789abcdef",
			"span_id":  "01234567-89abcdef",
		},
		{
			"level": "debug",
			"msg":   "test without trace context",
			"ts":    float64(time.Now().Unix()),
		},
		{
			"level":    "info",
			"msg":      "test with invalid trace id",
			"ts":       float64(time.Now().Unix()),
			"trace_id": "invalid",
			"span_id":  "0123456789abcdef",
		},
	}
	
	for _, log := range logs {
		data, _ := json.Marshal(log)
		_, err := twc.Write(data)
		assert.NoError(t, err)
	}
	
	// Wait for batch to be sent
	time.Sleep(200 * time.Millisecond)
	
	// Verify the logs were sent with correct trace context
	requests := mockClient.getRequests()
	require.Len(t, requests, 1)
	require.Len(t, requests[0].ResourceLogs, 1)
	require.Len(t, requests[0].ResourceLogs[0].ScopeLogs, 1)
	logRecords := requests[0].ResourceLogs[0].ScopeLogs[0].LogRecords
	require.Len(t, logRecords, 4)
	
	// First log should have valid trace and span IDs
	assert.Len(t, logRecords[0].TraceId, 16)
	assert.Len(t, logRecords[0].SpanId, 8)
	assert.Equal(t, []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}, logRecords[0].TraceId)
	assert.Equal(t, []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}, logRecords[0].SpanId)
	
	// Second log should handle dashed IDs correctly
	assert.Len(t, logRecords[1].TraceId, 16)
	assert.Len(t, logRecords[1].SpanId, 8)
	
	// Third log should have no trace context
	assert.Len(t, logRecords[2].TraceId, 0)
	assert.Len(t, logRecords[2].SpanId, 0)
	
	// Fourth log should have no trace ID (invalid) but valid span ID
	assert.Len(t, logRecords[3].TraceId, 0)
	assert.Len(t, logRecords[3].SpanId, 8)
	
	// Cleanup
	assert.NoError(t, w.Cleanup())
}

func TestOTLPWriter_BatchTimeout(t *testing.T) {
	w := &OTLPWriter{
		Endpoint:     "localhost:4317",
		Protocol:     "grpc",
		BatchSize:    10, // High batch size
		BatchTimeout: caddy.Duration(50 * time.Millisecond), // Short timeout
		Insecure:     true,
	}
	
	mockClient := &mockLogsServiceClient{
		responses: make(chan *collectorlogspb.ExportLogsServiceResponse, 10),
	}
	
	ctx := caddy.Context{
		Context: context.Background(),
	}
	
	require.NoError(t, w.Provision(ctx))
	
	// Get a test write closer
	twc, err := w.openWriterForTest()
	require.NoError(t, err)
	defer twc.Close()
	
	// Set the mock client
	twc.setGRPCClient(mockClient)
	
	// Write only one log (less than batch size)
	log := map[string]interface{}{
		"level": "info",
		"msg":   "timeout test",
		"ts":    float64(time.Now().Unix()),
	}
	data, _ := json.Marshal(log)
	_, err = twc.Write(data)
	assert.NoError(t, err)
	
	// Wait for timeout to trigger
	time.Sleep(100 * time.Millisecond)
	
	// Verify the log was sent due to timeout
	requests := mockClient.getRequests()
	assert.Len(t, requests, 1)
	assert.Len(t, requests[0].ResourceLogs[0].ScopeLogs[0].LogRecords, 1)
	
	assert.NoError(t, w.Cleanup())
}

func TestOTLPWriter_ConcurrentWrites(t *testing.T) {
	w := &OTLPWriter{
		Endpoint:     "localhost:4317",
		Protocol:     "grpc",
		BatchSize:    5,
		BatchTimeout: caddy.Duration(100 * time.Millisecond),
		Insecure:     true,
		Debug:        true,
	}
	
	mockClient := &mockLogsServiceClient{
		responses: make(chan *collectorlogspb.ExportLogsServiceResponse, 100),
	}
	
	ctx := caddy.Context{
		Context: context.Background(),
	}
	
	require.NoError(t, w.Provision(ctx))
	
	// Get a test write closer
	twc, err := w.openWriterForTest()
	require.NoError(t, err)
	defer twc.Close()
	
	// Set the mock client
	twc.setGRPCClient(mockClient)
	
	// Concurrent writes
	var wg sync.WaitGroup
	numGoroutines := 10
	logsPerGoroutine := 10
	
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < logsPerGoroutine; j++ {
				log := map[string]interface{}{
					"level":     "info",
					"msg":       fmt.Sprintf("concurrent test %d-%d", id, j),
					"ts":        float64(time.Now().Unix()),
					"goroutine": id,
				}
				data, _ := json.Marshal(log)
				_, err := twc.Write(data)
				assert.NoError(t, err)
			}
		}(i)
	}
	
	wg.Wait()
	
	// Wait for all batches to be sent
	time.Sleep(200 * time.Millisecond)
	
	// Verify all logs were sent
	var totalLogs int
	for _, req := range mockClient.getRequests() {
		for _, rl := range req.ResourceLogs {
			for _, sl := range rl.ScopeLogs {
				totalLogs += len(sl.LogRecords)
			}
		}
	}
	assert.Equal(t, numGoroutines*logsPerGoroutine, totalLogs)
	
	assert.NoError(t, w.Cleanup())
}

func TestOTLPWriter_Cleanup(t *testing.T) {
	w := &OTLPWriter{
		Endpoint:     "localhost:4317",
		Protocol:     "grpc",
		BatchSize:    10,
		BatchTimeout: caddy.Duration(1 * time.Second),
		Insecure:     true,
	}
	
	ctx := caddy.Context{
		Context: context.Background(),
	}
	
	require.NoError(t, w.Provision(ctx))
	
	// Write some logs
	wc, err := w.OpenWriter()
	require.NoError(t, err)
	
	log := map[string]interface{}{
		"level": "info",
		"msg":   "cleanup test",
	}
	data, _ := json.Marshal(log)
	_, err = wc.Write(data)
	assert.NoError(t, err)
	
	// Cleanup should flush remaining logs
	err = w.Cleanup()
	assert.NoError(t, err)
	
	// In singleton pattern, closed state is per writer, not per config
	
	// Second cleanup should be safe
	err = w.Cleanup()
	assert.NoError(t, err)
}

func TestOTLPWriter_UnmarshalCaddyfile(t *testing.T) {
	tests := []struct {
		name      string
		caddyfile string
		expected  *OTLPWriter
		wantErr   bool
	}{
		{
			name: "full configuration",
			caddyfile: `otlp {
				endpoint localhost:4317
				protocol grpc
				insecure true
				timeout 30s
				service_name my-service
				batch_size 100
				batch_timeout 5s
				debug true
				headers {
					Authorization "Bearer token"
					X-Custom-Header "value"
				}
				resource_attributes {
					environment production
					region us-west-2
				}
			}`,
			expected: &OTLPWriter{
				Endpoint:     "localhost:4317",
				Protocol:     "grpc",
				Insecure:     true,
				Timeout:      caddy.Duration(30 * time.Second),
				ServiceName:  "my-service",
				BatchSize:    100,
				BatchTimeout: caddy.Duration(5 * time.Second),
				Debug:        true,
				Headers: map[string]string{
					"Authorization":   "Bearer token",
					"X-Custom-Header": "value",
				},
				ResourceAttributes: map[string]string{
					"environment": "production",
					"region":      "us-west-2",
				},
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
			name: "invalid batch size",
			caddyfile: `otlp {
				endpoint localhost:4317
				batch_size invalid
			}`,
			wantErr: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &OTLPWriter{}
			d := caddyfile.NewTestDispenser(tt.caddyfile)
			
			err := w.UnmarshalCaddyfile(d)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected.Endpoint, w.Endpoint)
				assert.Equal(t, tt.expected.Protocol, w.Protocol)
				assert.Equal(t, tt.expected.Insecure, w.Insecure)
				assert.Equal(t, tt.expected.ServiceName, w.ServiceName)
				assert.Equal(t, tt.expected.BatchSize, w.BatchSize)
				assert.Equal(t, tt.expected.Debug, w.Debug)
				
				if tt.expected.Headers != nil {
					assert.Equal(t, tt.expected.Headers, w.Headers)
				}
				if tt.expected.ResourceAttributes != nil {
					assert.Equal(t, tt.expected.ResourceAttributes, w.ResourceAttributes)
				}
			}
		})
	}
}

func TestOTLPWriter_ParseHeadersFromEnv(t *testing.T) {
	tests := []struct {
		name     string
		envVar   string
		envValue string
		existing map[string]string
		expected map[string]string
	}{
		{
			name:     "parse single header",
			envVar:   "TEST_HEADERS",
			envValue: "Authorization=Bearer token",
			existing: map[string]string{},
			expected: map[string]string{
				"Authorization": "Bearer token",
			},
		},
		{
			name:     "parse multiple headers",
			envVar:   "TEST_HEADERS",
			envValue: "Authorization=Bearer token,X-Custom=value,X-Another=test",
			existing: map[string]string{},
			expected: map[string]string{
				"Authorization": "Bearer token",
				"X-Custom":      "value",
				"X-Another":     "test",
			},
		},
		{
			name:     "don't override existing headers",
			envVar:   "TEST_HEADERS",
			envValue: "Authorization=Bearer new",
			existing: map[string]string{
				"Authorization": "Bearer existing",
			},
			expected: map[string]string{
				"Authorization": "Bearer existing",
			},
		},
		{
			name:     "handle whitespace",
			envVar:   "TEST_HEADERS",
			envValue: " Authorization = Bearer token , X-Custom = value ",
			existing: map[string]string{},
			expected: map[string]string{
				"Authorization": "Bearer token",
				"X-Custom":      "value",
			},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv(tt.envVar, tt.envValue)
			defer os.Unsetenv(tt.envVar)
			
			w := &OTLPWriter{
				Headers: tt.existing,
			}
			
			w.parseHeadersFromEnv(tt.envVar)
			assert.Equal(t, tt.expected, w.Headers)
		})
	}
}

func TestOTLPWriter_String(t *testing.T) {
	w := &OTLPWriter{
		Endpoint: "localhost:4317",
		Protocol: "grpc",
	}
	
	expected := "otlp writer to localhost:4317 via grpc"
	assert.Equal(t, expected, w.String())
}

func TestOTLPWriter_WriterKey(t *testing.T) {
	w := &OTLPWriter{
		Endpoint: "localhost:4317",
		Protocol: "grpc",
	}
	
	expected := "otlp:grpc:localhost:4317"
	assert.Equal(t, expected, w.WriterKey())
}

// Mock gRPC client for testing
type mockLogsServiceClient struct {
	mu        sync.Mutex
	requests  []*collectorlogspb.ExportLogsServiceRequest
	responses chan *collectorlogspb.ExportLogsServiceResponse
	err       error
}

func (m *mockLogsServiceClient) Export(ctx context.Context, in *collectorlogspb.ExportLogsServiceRequest, opts ...grpc.CallOption) (*collectorlogspb.ExportLogsServiceResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.err != nil {
		return nil, m.err
	}
	
	m.requests = append(m.requests, in)
	
	select {
	case resp := <-m.responses:
		return resp, nil
	default:
		return &collectorlogspb.ExportLogsServiceResponse{}, nil
	}
}

func (m *mockLogsServiceClient) getRequests() []*collectorlogspb.ExportLogsServiceRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*collectorlogspb.ExportLogsServiceRequest{}, m.requests...)
}

func (m *mockLogsServiceClient) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = nil
	m.err = nil
}

// Helper function to capture stderr output
func captureStderr(f func()) string {
	old := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w
	
	f()
	
	w.Close()
	os.Stderr = old
	
	var buf bytes.Buffer
	buf.ReadFrom(r)
	return buf.String()
}

func TestOTLPWriter_DebugLogging(t *testing.T) {
	// Test using actualOTLPWriter directly since it has the logging methods
	aw := &actualOTLPWriter{
		config: OTLPWriter{
			Debug: true,
		},
	}
	
	// Test debug logging
	output := captureStderr(func() {
		aw.debugf("test debug message: %s", "value")
	})
	assert.Contains(t, output, "[DEBUG OTLP] test debug message: value")
	
	// Test with debug disabled
	aw.config.Debug = false
	output = captureStderr(func() {
		aw.debugf("should not appear")
	})
	assert.Empty(t, output)
	
	// Test other log levels
	output = captureStderr(func() {
		aw.infof("info message")
		aw.warnf("warning message")
		aw.errorf("error message")
	})
	assert.Contains(t, output, "[INFO OTLP] info message")
	assert.Contains(t, output, "[WARN OTLP] warning message")
	assert.Contains(t, output, "[ERROR OTLP] error message")
}

func TestOTLPWriter_RetryLogic(t *testing.T) {
	// Clear any existing writers
	globalWritersMu.Lock()
	globalWriters = make(map[string]*globalOTLPWriter)
	globalWritersMu.Unlock()

	w := &OTLPWriter{
		Endpoint:     "localhost:4317",
		Protocol:     "grpc",
		BatchSize:    1,
		BatchTimeout: caddy.Duration(100 * time.Millisecond),
		MaxRetries:   2,
		RetryDelay:   caddy.Duration(10 * time.Millisecond),
		Insecure:     true,
		Debug:        true,
	}
	
	// Track retry attempts
	var attemptCount int32
	
	// Create a custom mock that tracks attempts
	mockClient := &trackingMockClient{
		failCount: 1, // Fail first attempt only
		onExport: func() {
			atomic.AddInt32(&attemptCount, 1)
		},
	}
	
	ctx := caddy.Context{
		Context: context.Background(),
	}
	
	require.NoError(t, w.Provision(ctx))
	
	// Get a test write closer
	twc, err := w.openWriterForTest()
	require.NoError(t, err)
	defer twc.Close()
	
	// Set the mock client
	twc.setGRPCClient(mockClient)
	
	// Write a log
	log := map[string]interface{}{
		"level": "info",
		"msg":   "retry test",
	}
	data, _ := json.Marshal(log)
	_, err = twc.Write(data)
	assert.NoError(t, err)
	
	// Wait for retries
	time.Sleep(200 * time.Millisecond)
	
	// Verify retry happened and eventually succeeded
	assert.Equal(t, int32(2), atomic.LoadInt32(&attemptCount))
	
	// Get the actual writer to check stats
	aw := twc.getActualWriter()
	assert.NotNil(t, aw)
	assert.Equal(t, int64(1), atomic.LoadInt64(&aw.sentBatches))
}

// trackingMockClient is a mock that can fail a specified number of times
type trackingMockClient struct {
	mu        sync.Mutex
	failCount int
	attempts  int
	onExport  func()
}

func (t *trackingMockClient) Export(ctx context.Context, in *collectorlogspb.ExportLogsServiceRequest, opts ...grpc.CallOption) (*collectorlogspb.ExportLogsServiceResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	if t.onExport != nil {
		t.onExport()
	}
	
	t.attempts++
	if t.attempts <= t.failCount {
		return nil, fmt.Errorf("simulated failure %d", t.attempts)
	}
	
	return &collectorlogspb.ExportLogsServiceResponse{}, nil
}
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
				assert.NotNil(t, tt.writer.resource)
				assert.NotNil(t, tt.writer.sendCh)
				assert.NotNil(t, tt.writer.stopCh)
				assert.NotNil(t, tt.writer.workerDone)
			}
		})
	}
}

func TestOTLPWriter_WriteAndBatch(t *testing.T) {
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
	w.grpcClient = mockClient
	
	// Get a write closer
	wc, err := w.OpenWriter()
	require.NoError(t, err)
	
	// Write logs
	logs := []map[string]interface{}{
		{"level": "info", "msg": "test message 1", "ts": float64(time.Now().Unix())},
		{"level": "error", "msg": "test message 2", "ts": float64(time.Now().Unix())},
		{"level": "debug", "msg": "test message 3", "ts": float64(time.Now().Unix())},
	}
	
	for _, log := range logs {
		data, _ := json.Marshal(log)
		_, err := wc.Write(data)
		assert.NoError(t, err)
	}
	
	// Wait for batches to be sent
	time.Sleep(200 * time.Millisecond)
	
	// Verify batches were sent
	assert.GreaterOrEqual(t, len(mockClient.getRequests()), 1)
	
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
	w.grpcClient = mockClient
	
	wc, err := w.OpenWriter()
	require.NoError(t, err)
	
	// Write only one log (less than batch size)
	log := map[string]interface{}{
		"level": "info",
		"msg":   "timeout test",
		"ts":    float64(time.Now().Unix()),
	}
	data, _ := json.Marshal(log)
	_, err = wc.Write(data)
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
	w.grpcClient = mockClient
	
	wc, err := w.OpenWriter()
	require.NoError(t, err)
	
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
				_, err := wc.Write(data)
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
	
	// Verify closed state
	assert.True(t, w.closed)
	
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
	w := &OTLPWriter{
		Debug: true,
	}
	
	// Test debug logging
	output := captureStderr(func() {
		w.debugf("test debug message: %s", "value")
	})
	assert.Contains(t, output, "[DEBUG OTLP] test debug message: value")
	
	// Test with debug disabled
	w.Debug = false
	output = captureStderr(func() {
		w.debugf("should not appear")
	})
	assert.Empty(t, output)
	
	// Test other log levels
	output = captureStderr(func() {
		w.infof("info message")
		w.warnf("warning message")
		w.errorf("error message")
	})
	assert.Contains(t, output, "[INFO OTLP] info message")
	assert.Contains(t, output, "[WARN OTLP] warning message")
	assert.Contains(t, output, "[ERROR OTLP] error message")
}

func TestOTLPWriter_RetryLogic(t *testing.T) {
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
	attemptCount := 0
	
	// Create a custom mock that tracks attempts
	mockClient := &trackingMockClient{
		failCount: 1, // Fail first attempt only
		onExport: func() {
			attemptCount++
		},
	}
	
	ctx := caddy.Context{
		Context: context.Background(),
	}
	
	require.NoError(t, w.Provision(ctx))
	w.grpcClient = mockClient
	
	wc, err := w.OpenWriter()
	require.NoError(t, err)
	
	// Write a log
	log := map[string]interface{}{
		"level": "info",
		"msg":   "retry test",
	}
	data, _ := json.Marshal(log)
	_, err = wc.Write(data)
	assert.NoError(t, err)
	
	// Wait for retries
	time.Sleep(200 * time.Millisecond)
	
	// Verify retry happened and eventually succeeded
	assert.Equal(t, 2, attemptCount)
	assert.Equal(t, int64(1), atomic.LoadInt64(&w.sentBatches))
	
	assert.NoError(t, w.Cleanup())
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
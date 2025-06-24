package caddyotlplogs

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	collectorlogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// mockOTLPServer implements a mock OTLP logs receiver for testing
type mockOTLPServer struct {
	collectorlogspb.UnimplementedLogsServiceServer
	mu               sync.Mutex
	receivedRequests []*collectorlogspb.ExportLogsServiceRequest
	errorsToReturn   []error
	callCount        int
}

func (m *mockOTLPServer) Export(ctx context.Context, req *collectorlogspb.ExportLogsServiceRequest) (*collectorlogspb.ExportLogsServiceResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.callCount++
	m.receivedRequests = append(m.receivedRequests, req)
	
	if m.callCount <= len(m.errorsToReturn) && m.errorsToReturn[m.callCount-1] != nil {
		return nil, m.errorsToReturn[m.callCount-1]
	}
	
	return &collectorlogspb.ExportLogsServiceResponse{
		PartialSuccess: &collectorlogspb.ExportLogsPartialSuccess{},
	}, nil
}

// getReceivedRequests returns a copy of received requests safely
func (m *mockOTLPServer) getReceivedRequests() []*collectorlogspb.ExportLogsServiceRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	requests := make([]*collectorlogspb.ExportLogsServiceRequest, len(m.receivedRequests))
	copy(requests, m.receivedRequests)
	return requests
}

// clearRequests clears the received requests safely
func (m *mockOTLPServer) clearRequests() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.receivedRequests = make([]*collectorlogspb.ExportLogsServiceRequest, 0)
}

// startMockGRPCServer starts a mock gRPC OTLP server
func startMockGRPCServer(t *testing.T) (*mockOTLPServer, string, func()) {
	mock := &mockOTLPServer{
		receivedRequests: make([]*collectorlogspb.ExportLogsServiceRequest, 0),
	}
	
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	
	grpcServer := grpc.NewServer()
	collectorlogspb.RegisterLogsServiceServer(grpcServer, mock)
	
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("gRPC server error: %v", err)
		}
	}()
	
	cleanup := func() {
		grpcServer.GracefulStop()
		lis.Close()
	}
	
	return mock, lis.Addr().String(), cleanup
}

// startMockHTTPServer starts a mock HTTP OTLP server
func startMockHTTPServer(t *testing.T) (*mockOTLPServer, string, func()) {
	mock := &mockOTLPServer{
		receivedRequests: make([]*collectorlogspb.ExportLogsServiceRequest, 0),
	}
	
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/logs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		
		var req collectorlogspb.ExportLogsServiceRequest
		body := make([]byte, r.ContentLength)
		_, err := r.Body.Read(body)
		if err != nil && err.Error() != "EOF" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		
		if err := proto.Unmarshal(body, &req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		
		resp, err := mock.Export(context.Background(), &req)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		
		respData, err := proto.Marshal(resp)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		w.Write(respData)
	})
	
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	
	server := &http.Server{
		Handler: mux,
	}
	
	go func() {
		if err := server.Serve(lis); err != http.ErrServerClosed {
			t.Logf("HTTP server error: %v", err)
		}
	}()
	
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(ctx)
		lis.Close()
	}
	
	endpoint := fmt.Sprintf("http://%s", lis.Addr().String())
	return mock, endpoint, cleanup
}

func TestOTLPWriter_Integration_GRPC(t *testing.T) {
	mock, endpoint, cleanup := startMockGRPCServer(t)
	defer cleanup()
	
	// Give server time to start
	time.Sleep(100 * time.Millisecond)
	
	w := &OTLPWriter{
		Endpoint:     endpoint,
		Protocol:     "grpc",
		Insecure:     true,
		ServiceName:  "test-service",
		BatchSize:    2,
		BatchTimeout: caddy.Duration(1 * time.Second),
		ResourceAttributes: map[string]string{
			"environment": "test",
			"version":     "1.0.0",
		},
	}
	
	ctx, cancel := caddy.NewContext(caddy.Context{
		Context: context.Background(),
	})
	defer cancel()
	
	err := w.Provision(ctx)
	if err != nil {
		t.Fatalf("Failed to provision OTLPWriter: %v", err)
	}
	defer w.Cleanup()
	
	writer, err := w.OpenWriter()
	if err != nil {
		t.Fatalf("Failed to open writer: %v", err)
	}
	
	// Write multiple log entries
	logs := []string{
		`{"level":"info","msg":"Test message 1","trace_id":"0123456789abcdef0123456789abcdef","span_id":"0123456789abcdef","user_id":"123"}`,
		`{"level":"error","msg":"Test error message","error":"connection timeout","request_id":"req-456"}`,
		`{"level":"debug","msg":"Debug message","component":"auth"}`,
	}
	
	for _, log := range logs {
		_, err := writer.Write([]byte(log))
		if err != nil {
			t.Errorf("Failed to write log: %v", err)
		}
	}
	
	// Force flush by closing
	err = writer.Close()
	if err != nil {
		t.Errorf("Failed to close writer: %v", err)
	}
	
	// Give time for async operations
	time.Sleep(500 * time.Millisecond)
	
	// Verify received logs
	requests := mock.getReceivedRequests()
	if len(requests) == 0 {
		t.Fatal("No requests received by mock server")
	}
	
	totalLogs := 0
	for _, req := range requests {
		for _, rl := range req.ResourceLogs {
			// Check resource attributes
			resourceAttrs := make(map[string]string)
			for _, attr := range rl.Resource.Attributes {
				if strVal := attr.GetValue().GetStringValue(); strVal != "" {
					resourceAttrs[attr.Key] = strVal
				}
			}
			
			if resourceAttrs["service.name"] != "test-service" {
				t.Errorf("Expected service.name 'test-service', got %s", resourceAttrs["service.name"])
			}
			if resourceAttrs["environment"] != "test" {
				t.Errorf("Expected environment 'test', got %s", resourceAttrs["environment"])
			}
			
			for _, sl := range rl.ScopeLogs {
				totalLogs += len(sl.LogRecords)
				
				for _, log := range sl.LogRecords {
					// Verify log has expected fields
					if log.TimeUnixNano == 0 {
						t.Error("Log missing timestamp")
					}
					if log.Body == nil {
						t.Error("Log missing body")
					}
					
					// Check first log for trace context
					if totalLogs == 1 {
						if len(log.TraceId) != 16 {
							t.Errorf("Expected trace ID length 16, got %d", len(log.TraceId))
						}
						if len(log.SpanId) != 8 {
							t.Errorf("Expected span ID length 8, got %d", len(log.SpanId))
						}
					}
				}
			}
		}
	}
	
	if totalLogs != len(logs) {
		t.Errorf("Expected %d logs, received %d", len(logs), totalLogs)
	}
}

func TestOTLPWriter_Integration_HTTP(t *testing.T) {
	mock, endpoint, cleanup := startMockHTTPServer(t)
	defer cleanup()
	
	// Give server time to start
	time.Sleep(100 * time.Millisecond)
	
	w := &OTLPWriter{
		Endpoint:     endpoint,
		Protocol:     "http/protobuf",
		Insecure:     true,
		ServiceName:  "test-service-http",
		BatchSize:    5,
		BatchTimeout: caddy.Duration(2 * time.Second),
		Headers: map[string]string{
			"Authorization": "Bearer test-token",
		},
	}
	
	ctx, cancel := caddy.NewContext(caddy.Context{
		Context: context.Background(),
	})
	defer cancel()
	
	err := w.Provision(ctx)
	if err != nil {
		t.Fatalf("Failed to provision OTLPWriter: %v", err)
	}
	defer w.Cleanup()
	
	writer, err := w.OpenWriter()
	if err != nil {
		t.Fatalf("Failed to open writer: %v", err)
	}
	
	// Write plain text log
	_, err = writer.Write([]byte("Plain text log message"))
	if err != nil {
		t.Errorf("Failed to write plain text log: %v", err)
	}
	
	// Write Caddy access log format
	accessLog := `{"request":{"method":"GET","uri":"/api/users","headers":{"User-Agent":["Mozilla/5.0"]}},"status":200,"duration":0.123}`
	_, err = writer.Write([]byte(accessLog))
	if err != nil {
		t.Errorf("Failed to write access log: %v", err)
	}
	
	// Close to force flush
	err = writer.Close()
	if err != nil {
		t.Errorf("Failed to close writer: %v", err)
	}
	
	// Give time for async operations
	time.Sleep(500 * time.Millisecond)
	
	// Verify received logs
	requests := mock.getReceivedRequests()
	if len(requests) == 0 {
		t.Fatal("No requests received by mock HTTP server")
	}
	
	totalLogs := 0
	for _, req := range requests {
		for _, rl := range req.ResourceLogs {
			for _, sl := range rl.ScopeLogs {
				totalLogs += len(sl.LogRecords)
			}
		}
	}
	
	if totalLogs != 2 {
		t.Errorf("Expected 2 logs, received %d", totalLogs)
	}
}

func TestOTLPWriter_Integration_BatchingBehavior(t *testing.T) {
	mock, endpoint, cleanup := startMockGRPCServer(t)
	defer cleanup()
	
	// Give server time to start
	time.Sleep(100 * time.Millisecond)
	
	w := &OTLPWriter{
		Endpoint:     endpoint,
		Protocol:     "grpc",
		Insecure:     true,
		ServiceName:  "batch-test",
		BatchSize:    3,
		BatchTimeout: caddy.Duration(500 * time.Millisecond),
	}
	
	ctx, cancel := caddy.NewContext(caddy.Context{
		Context: context.Background(),
	})
	defer cancel()
	
	err := w.Provision(ctx)
	if err != nil {
		t.Fatalf("Failed to provision OTLPWriter: %v", err)
	}
	defer w.Cleanup()
	
	writer, err := w.OpenWriter()
	if err != nil {
		t.Fatalf("Failed to open writer: %v", err)
	}
	
	// Write logs to trigger batch by size
	for i := 0; i < 3; i++ {
		log := fmt.Sprintf(`{"level":"info","msg":"Batch log %d"}`, i)
		_, err := writer.Write([]byte(log))
		if err != nil {
			t.Errorf("Failed to write log %d: %v", i, err)
		}
	}
	
	// Give time for batch to be sent
	time.Sleep(200 * time.Millisecond)
	
	// Check that batch was sent
	requests := mock.getReceivedRequests()
	if len(requests) != 1 {
		t.Errorf("Expected 1 batch sent by size trigger, got %d", len(requests))
	}
	
	// Reset for timeout test
	mock.clearRequests()
	
	// Write fewer logs than batch size
	for i := 0; i < 2; i++ {
		log := fmt.Sprintf(`{"level":"info","msg":"Timeout batch log %d"}`, i)
		_, err := writer.Write([]byte(log))
		if err != nil {
			t.Errorf("Failed to write log %d: %v", i, err)
		}
	}
	
	// Wait for batch timeout
	time.Sleep(600 * time.Millisecond)
	
	// Check that batch was sent by timeout
	requests = mock.getReceivedRequests()
	if len(requests) != 1 {
		t.Errorf("Expected 1 batch sent by timeout trigger, got %d", len(requests))
	}
	
	// Close writer
	err = writer.Close()
	if err != nil {
		t.Errorf("Failed to close writer: %v", err)
	}
}

func TestOTLPWriter_Integration_ErrorHandling(t *testing.T) {
	mock, endpoint, cleanup := startMockGRPCServer(t)
	defer cleanup()
	
	// Configure mock to return errors
	mock.errorsToReturn = []error{
		status.Error(codes.Unavailable, "service unavailable"),
		nil, // Second call succeeds
	}
	
	// Give server time to start
	time.Sleep(100 * time.Millisecond)
	
	w := &OTLPWriter{
		Endpoint:     endpoint,
		Protocol:     "grpc",
		Insecure:     true,
		ServiceName:  "error-test",
		BatchSize:    1,
		BatchTimeout: caddy.Duration(100 * time.Millisecond),
	}
	
	ctx, cancel := caddy.NewContext(caddy.Context{
		Context: context.Background(),
	})
	defer cancel()
	
	err := w.Provision(ctx)
	if err != nil {
		t.Fatalf("Failed to provision OTLPWriter: %v", err)
	}
	defer w.Cleanup()
	
	writer, err := w.OpenWriter()
	if err != nil {
		t.Fatalf("Failed to open writer: %v", err)
	}
	
	// Write log that will fail
	_, err = writer.Write([]byte(`{"level":"error","msg":"This will fail"}`))
	if err != nil {
		t.Errorf("Write should not return error immediately: %v", err)
	}
	
	// Wait for batch to be sent
	time.Sleep(200 * time.Millisecond)
	
	// Write another log that should succeed
	_, err = writer.Write([]byte(`{"level":"info","msg":"This should succeed"}`))
	if err != nil {
		t.Errorf("Write should not return error: %v", err)
	}
	
	// Close to flush
	err = writer.Close()
	if err != nil {
		t.Logf("Expected close might return error from failed batch: %v", err)
	}
	
	// Verify error was reported
	if w.failedBatches == 0 {
		t.Error("Expected failedBatches counter to be incremented")
	}
}

func TestOTLPWriter_Integration_LargeBatch(t *testing.T) {
	mock, endpoint, cleanup := startMockGRPCServer(t)
	defer cleanup()
	
	// Give server time to start
	time.Sleep(100 * time.Millisecond)
	
	w := &OTLPWriter{
		Endpoint:     endpoint,
		Protocol:     "grpc",
		Insecure:     true,
		ServiceName:  "large-batch-test",
		BatchSize:    100,
		BatchTimeout: caddy.Duration(5 * time.Second),
	}
	
	ctx, cancel := caddy.NewContext(caddy.Context{
		Context: context.Background(),
	})
	defer cancel()
	
	err := w.Provision(ctx)
	if err != nil {
		t.Fatalf("Failed to provision OTLPWriter: %v", err)
	}
	defer w.Cleanup()
	
	writer, err := w.OpenWriter()
	if err != nil {
		t.Fatalf("Failed to open writer: %v", err)
	}
	
	// Write many logs with different attributes
	for i := 0; i < 100; i++ {
		log := map[string]interface{}{
			"level":      "info",
			"msg":        fmt.Sprintf("Log message %d", i),
			"index":      i,
			"batch_test": true,
			"metadata": map[string]interface{}{
				"host":   "server-01",
				"region": "us-east-1",
			},
		}
		
		if i%10 == 0 {
			log["trace_id"] = fmt.Sprintf("%032x", i)
			log["span_id"] = fmt.Sprintf("%016x", i)
		}
		
		logData, _ := json.Marshal(log)
		_, err := writer.Write(logData)
		if err != nil {
			t.Errorf("Failed to write log %d: %v", i, err)
		}
	}
	
	// Give time for batch to be sent
	time.Sleep(500 * time.Millisecond)
	
	// Verify all logs were received
	requests := mock.getReceivedRequests()
	totalLogs := 0
	for _, req := range requests {
		for _, rl := range req.ResourceLogs {
			for _, sl := range rl.ScopeLogs {
				totalLogs += len(sl.LogRecords)
			}
		}
	}
	
	if totalLogs != 100 {
		t.Errorf("Expected 100 logs, received %d", totalLogs)
	}
	
	// Close writer
	err = writer.Close()
	if err != nil {
		t.Errorf("Failed to close writer: %v", err)
	}
}

func TestOTLPWriter_Integration_ConcurrentWrites(t *testing.T) {
	mock, endpoint, cleanup := startMockGRPCServer(t)
	defer cleanup()
	
	// Give server time to start
	time.Sleep(100 * time.Millisecond)
	
	w := &OTLPWriter{
		Endpoint:     endpoint,
		Protocol:     "grpc",
		Insecure:     true,
		ServiceName:  "concurrent-test",
		BatchSize:    50,
		BatchTimeout: caddy.Duration(1 * time.Second),
	}
	
	ctx, cancel := caddy.NewContext(caddy.Context{
		Context: context.Background(),
	})
	defer cancel()
	
	err := w.Provision(ctx)
	if err != nil {
		t.Fatalf("Failed to provision OTLPWriter: %v", err)
	}
	defer w.Cleanup()
	
	writer, err := w.OpenWriter()
	if err != nil {
		t.Fatalf("Failed to open writer: %v", err)
	}
	
	// Concurrent write test
	numGoroutines := 10
	logsPerGoroutine := 20
	done := make(chan bool, numGoroutines)
	
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for i := 0; i < logsPerGoroutine; i++ {
				log := fmt.Sprintf(`{"level":"info","msg":"Concurrent log","goroutine":%d,"index":%d}`, goroutineID, i)
				_, err := writer.Write([]byte(log))
				if err != nil {
					t.Errorf("Goroutine %d failed to write log %d: %v", goroutineID, i, err)
				}
			}
			done <- true
		}(g)
	}
	
	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
	
	// Close to flush remaining
	err = writer.Close()
	if err != nil {
		t.Errorf("Failed to close writer: %v", err)
	}
	
	// Give time for final batch
	time.Sleep(500 * time.Millisecond)
	
	// Verify all logs were received
	requests := mock.getReceivedRequests()
	totalLogs := 0
	for _, req := range requests {
		for _, rl := range req.ResourceLogs {
			for _, sl := range rl.ScopeLogs {
				totalLogs += len(sl.LogRecords)
			}
		}
	}
	
	expectedLogs := numGoroutines * logsPerGoroutine
	if totalLogs != expectedLogs {
		t.Errorf("Expected %d logs from concurrent writes, received %d", expectedLogs, totalLogs)
	}
}

func TestOTLPWriter_Integration_EnvironmentVariables(t *testing.T) {
	// Save current environment variables
	oldResourceAttrs := os.Getenv("OTEL_RESOURCE_ATTRIBUTES")
	oldHeaders := os.Getenv("OTEL_EXPORTER_OTLP_HEADERS")
	oldLogsHeaders := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_HEADERS")
	oldServiceName := os.Getenv("OTEL_SERVICE_NAME")
	
	// Restore environment variables after test
	defer func() {
		os.Setenv("OTEL_RESOURCE_ATTRIBUTES", oldResourceAttrs)
		os.Setenv("OTEL_EXPORTER_OTLP_HEADERS", oldHeaders)
		os.Setenv("OTEL_EXPORTER_OTLP_LOGS_HEADERS", oldLogsHeaders)
		os.Setenv("OTEL_SERVICE_NAME", oldServiceName)
	}()
	
	// Set test environment variables
	os.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=production,service.version=v1.2.3,service.namespace=test-namespace,custom.attribute=test%20value")
	os.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "X-Global-Header=global-value,Authorization=Bearer%20test-token")
	os.Setenv("OTEL_EXPORTER_OTLP_LOGS_HEADERS", "X-Logs-Header=logs-specific-value")
	os.Setenv("OTEL_SERVICE_NAME", "env-test-service")
	
	mock, endpoint, cleanup := startMockGRPCServer(t)
	defer cleanup()
	
	// Give server time to start
	time.Sleep(100 * time.Millisecond)
	
	// Create writer with minimal configuration - let it pick up from env vars
	w := &OTLPWriter{
		Endpoint:     endpoint,
		Protocol:     "grpc",
		Insecure:     true,
		BatchSize:    1, // Send immediately to verify
		BatchTimeout: caddy.Duration(100 * time.Millisecond),
	}
	
	ctx, cancel := caddy.NewContext(caddy.Context{
		Context: context.Background(),
	})
	defer cancel()
	
	err := w.Provision(ctx)
	if err != nil {
		t.Fatalf("Failed to provision OTLPWriter: %v", err)
	}
	defer w.Cleanup()
	
	// Verify headers were loaded from environment
	expectedHeaders := map[string]string{
		"X-Global-Header": "global-value",
		"Authorization":   "Bearer test-token",
		"X-Logs-Header":   "logs-specific-value",
	}
	
	for key, expectedValue := range expectedHeaders {
		if actualValue, ok := w.Headers[key]; !ok {
			t.Errorf("Expected header %s not found", key)
		} else if actualValue != expectedValue {
			t.Errorf("Header %s: expected %q, got %q", key, expectedValue, actualValue)
		}
	}
	
	// Write a test log
	writer, err := w.OpenWriter()
	if err != nil {
		t.Fatalf("Failed to open writer: %v", err)
	}
	
	testLog := `{"level":"info","msg":"Testing environment variables","test_id":"env-test-123"}`
	_, err = writer.Write([]byte(testLog))
	if err != nil {
		t.Errorf("Failed to write log: %v", err)
	}
	
	// Close to ensure flush
	err = writer.Close()
	if err != nil {
		t.Errorf("Failed to close writer: %v", err)
	}
	
	// Give time for the log to be sent
	time.Sleep(200 * time.Millisecond)
	
	// Verify the log was received with correct resource attributes
	requests := mock.getReceivedRequests()
	if len(requests) == 0 {
		t.Fatal("No requests received by mock server")
	}
	
	// Check resource attributes in the received request
	for _, req := range requests {
		for _, rl := range req.ResourceLogs {
			if rl.Resource == nil {
				t.Error("Resource is nil in received logs")
				continue
			}
			
			// Build a map of received attributes for easier verification
			receivedAttrs := make(map[string]string)
			for _, attr := range rl.Resource.Attributes {
				if strVal := attr.GetValue().GetStringValue(); strVal != "" {
					receivedAttrs[attr.Key] = strVal
				}
			}
			
			// Verify expected resource attributes from environment
			expectedResourceAttrs := map[string]string{
				"service.name":           "env-test-service",
				"deployment.environment": "production",
				"service.version":        "v1.2.3",
				"service.namespace":      "test-namespace",
				"custom.attribute":       "test value", // Should be URL decoded
			}
			
			for key, expectedValue := range expectedResourceAttrs {
				if actualValue, ok := receivedAttrs[key]; !ok {
					t.Errorf("Expected resource attribute %s not found", key)
				} else if actualValue != expectedValue {
					t.Errorf("Resource attribute %s: expected %q, got %q", key, expectedValue, actualValue)
				}
			}
			
			// Verify we have the expected number of logs
			for _, sl := range rl.ScopeLogs {
				if len(sl.LogRecords) != 1 {
					t.Errorf("Expected 1 log record, got %d", len(sl.LogRecords))
				}
				
				// Verify the log content
				for _, log := range sl.LogRecords {
					if log.Body == nil {
						t.Error("Log body is nil")
						continue
					}
					
					bodyStr := log.Body.GetStringValue()
					if !strings.Contains(bodyStr, "Testing environment variables") {
						t.Errorf("Log body doesn't contain expected message: %s", bodyStr)
					}
					
					// Check for the test_id attribute
					foundTestId := false
					for _, attr := range log.Attributes {
						if attr.Key == "test_id" && attr.GetValue().GetStringValue() == "env-test-123" {
							foundTestId = true
							break
						}
					}
					if !foundTestId {
						t.Error("test_id attribute not found in log")
					}
				}
			}
		}
	}
}
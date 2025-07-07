package otlplogs

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	collectorlogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

// TestOTLPWriter_TracePropagation tests that trace context is properly propagated
func TestOTLPWriter_TracePropagation(t *testing.T) {
	// Ensure propagator is set up
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	w := &OTLPWriter{
		Endpoint:     "localhost:4317",
		Protocol:     "grpc",
		BatchSize:    1,
		BatchTimeout: caddy.Duration(50 * time.Millisecond),
		Insecure:     true,
		Debug:        true,
	}

	// Mock client that captures the context
	var capturedCtx context.Context
	mockClient := &mockPropagationClient{
		onExport: func(ctx context.Context, req *collectorlogspb.ExportLogsServiceRequest) {
			capturedCtx = ctx
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

	// Write a log with trace context
	log := map[string]interface{}{
		"level":    "info",
		"msg":      "test with trace propagation",
		"ts":       float64(time.Now().Unix()),
		"trace_id": "0123456789abcdef0123456789abcdef",
		"span_id":  "0123456789abcdef",
	}
	data, _ := json.Marshal(log)
	_, err = twc.Write(data)
	assert.NoError(t, err)

	// Wait for the log to be sent
	time.Sleep(100 * time.Millisecond)

	// Verify trace context was propagated
	require.NotNil(t, capturedCtx)
	
	// Extract the span context from the captured context
	spanCtx := trace.SpanContextFromContext(capturedCtx)
	assert.True(t, spanCtx.IsValid())
	assert.Equal(t, "0123456789abcdef0123456789abcdef", spanCtx.TraceID().String())
	assert.Equal(t, "0123456789abcdef", spanCtx.SpanID().String())

	assert.NoError(t, w.Cleanup())
}

// TestOTLPWriter_TracePropagationWithHeaders tests that trace context propagation works with headers
func TestOTLPWriter_TracePropagationWithHeaders(t *testing.T) {
	w := &OTLPWriter{
		Endpoint:     "localhost:4317",
		Protocol:     "grpc",
		BatchSize:    1,
		BatchTimeout: caddy.Duration(50 * time.Millisecond),
		Insecure:     true,
		Headers: map[string]string{
			"Authorization": "Bearer test-token",
		},
	}

	// Mock client that captures the context
	var capturedCtx context.Context
	mockClient := &mockPropagationClient{
		onExport: func(ctx context.Context, req *collectorlogspb.ExportLogsServiceRequest) {
			capturedCtx = ctx
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

	// Write a log with trace context
	log := map[string]interface{}{
		"level":    "info",
		"msg":      "test with headers and trace",
		"ts":       float64(time.Now().Unix()),
		"trace_id": "fedcba9876543210fedcba9876543210",
		"span_id":  "fedcba9876543210",
	}
	data, _ := json.Marshal(log)
	_, err = twc.Write(data)
	assert.NoError(t, err)

	// Wait for the log to be sent
	time.Sleep(100 * time.Millisecond)

	// Verify trace context was propagated
	spanCtx := trace.SpanContextFromContext(capturedCtx)
	assert.True(t, spanCtx.IsValid())

	// Note: Headers are injected by the interceptor at the transport level,
	// so they won't be visible in the context passed to Export method.
	// The interceptor adds them to the outgoing RPC metadata.

	assert.NoError(t, w.Cleanup())
}

// TestOTLPWriter_NoTracePropagation tests behavior when no trace context is present
func TestOTLPWriter_NoTracePropagation(t *testing.T) {
	w := &OTLPWriter{
		Endpoint:     "localhost:4317",
		Protocol:     "grpc",
		BatchSize:    1,
		BatchTimeout: caddy.Duration(50 * time.Millisecond),
		Insecure:     true,
	}

	// Mock client that captures the context
	var capturedCtx context.Context
	mockClient := &mockPropagationClient{
		onExport: func(ctx context.Context, req *collectorlogspb.ExportLogsServiceRequest) {
			capturedCtx = ctx
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

	// Write a log without trace context
	log := map[string]interface{}{
		"level": "info",
		"msg":   "test without trace context",
		"ts":    float64(time.Now().Unix()),
	}
	data, _ := json.Marshal(log)
	_, err = twc.Write(data)
	assert.NoError(t, err)

	// Wait for the log to be sent
	time.Sleep(100 * time.Millisecond)

	// Verify no trace context was propagated
	require.NotNil(t, capturedCtx)
	spanCtx := trace.SpanContextFromContext(capturedCtx)
	assert.False(t, spanCtx.IsValid())

	assert.NoError(t, w.Cleanup())
}

// mockPropagationClient is a mock gRPC client for testing propagation
type mockPropagationClient struct {
	onExport func(ctx context.Context, req *collectorlogspb.ExportLogsServiceRequest)
}

func (m *mockPropagationClient) Export(ctx context.Context, in *collectorlogspb.ExportLogsServiceRequest, opts ...grpc.CallOption) (*collectorlogspb.ExportLogsServiceResponse, error) {
	if m.onExport != nil {
		m.onExport(ctx, in)
	}
	return &collectorlogspb.ExportLogsServiceResponse{}, nil
}
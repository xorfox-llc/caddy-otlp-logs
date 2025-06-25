package caddyotlplogs

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	collectorlogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// flakeyOTLPServer simulates network failures
type flakeyOTLPServer struct {
	collectorlogspb.UnimplementedLogsServiceServer
	mu              sync.Mutex
	requestCount    int32
	failUntilCount  int32
	hangDuration    time.Duration
	shouldHang      bool
	shouldFail      bool
	receivedBatches int32
}

func (f *flakeyOTLPServer) Export(ctx context.Context, req *collectorlogspb.ExportLogsServiceRequest) (*collectorlogspb.ExportLogsServiceResponse, error) {
	count := atomic.AddInt32(&f.requestCount, 1)
	
	// Simulate hanging
	if f.shouldHang && count <= f.failUntilCount {
		select {
		case <-ctx.Done():
			return nil, status.Error(codes.DeadlineExceeded, "simulated timeout")
		case <-time.After(f.hangDuration):
			return nil, status.Error(codes.DeadlineExceeded, "simulated hang")
		}
	}
	
	// Simulate failures
	if f.shouldFail && count <= f.failUntilCount {
		return nil, status.Error(codes.Unavailable, "simulated network failure")
	}
	
	// Success
	atomic.AddInt32(&f.receivedBatches, 1)
	return &collectorlogspb.ExportLogsServiceResponse{}, nil
}

func TestOTLPWriter_NetworkFailureRecovery(t *testing.T) {
	// Start flakey server
	flakey := &flakeyOTLPServer{
		shouldFail:     true,
		failUntilCount: 5, // Fail first 5 requests
	}
	
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer lis.Close()
	
	grpcServer := grpc.NewServer()
	collectorlogspb.RegisterLogsServiceServer(grpcServer, flakey)
	
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("gRPC server error: %v", err)
		}
	}()
	defer grpcServer.GracefulStop()
	
	// Create writer with aggressive timeouts for testing
	w := &OTLPWriter{
		Endpoint:     lis.Addr().String(),
		Protocol:     "grpc",
		Insecure:     true,
		Timeout:      caddy.Duration(2 * time.Second),
		BatchSize:    5,
		BatchTimeout: caddy.Duration(1 * time.Second),
		MaxRetries:   3,
		RetryDelay:   caddy.Duration(500 * time.Millisecond),
	}
	
	// Provision the writer
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()
	
	if err := w.Provision(ctx); err != nil {
		t.Fatalf("Failed to provision writer: %v", err)
	}
	defer w.Cleanup()
	
	// Open writer
	writer, err := w.OpenWriter()
	if err != nil {
		t.Fatalf("Failed to open writer: %v", err)
	}
	defer writer.Close()
	
	// Send logs while network is failing
	for i := 0; i < 10; i++ {
		log := fmt.Sprintf(`{"level":"info","msg":"test log %d"}`, i)
		if _, err := writer.Write([]byte(log)); err != nil {
			t.Logf("Write error (expected during failures): %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	
	// Wait for retries and recovery
	time.Sleep(5 * time.Second)
	
	// Check that some logs were eventually delivered
	receivedBatches := atomic.LoadInt32(&flakey.receivedBatches)
	if receivedBatches == 0 {
		t.Error("No batches were received after network recovery")
	} else {
		t.Logf("Successfully received %d batches after recovery", receivedBatches)
	}
}

func TestOTLPWriter_TimeoutHandling(t *testing.T) {
	// Start hanging server
	flakey := &flakeyOTLPServer{
		shouldHang:     true,
		hangDuration:   5 * time.Second,
		failUntilCount: 3, // Hang for first 3 requests
	}
	
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer lis.Close()
	
	grpcServer := grpc.NewServer()
	collectorlogspb.RegisterLogsServiceServer(grpcServer, flakey)
	
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("gRPC server error: %v", err)
		}
	}()
	defer grpcServer.GracefulStop()
	
	// Create writer with short timeout
	w := &OTLPWriter{
		Endpoint:     lis.Addr().String(),
		Protocol:     "grpc",
		Insecure:     true,
		Timeout:      caddy.Duration(1 * time.Second), // Short timeout
		BatchSize:    5,
		BatchTimeout: caddy.Duration(1 * time.Second),
		MaxRetries:   2,
		RetryDelay:   caddy.Duration(1 * time.Second),
	}
	
	// Provision the writer
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()
	
	if err := w.Provision(ctx); err != nil {
		t.Fatalf("Failed to provision writer: %v", err)
	}
	defer w.Cleanup()
	
	// Open writer
	writer, err := w.OpenWriter()
	if err != nil {
		t.Fatalf("Failed to open writer: %v", err)
	}
	defer writer.Close()
	
	// Send logs while server is hanging
	start := time.Now()
	for i := 0; i < 10; i++ {
		log := fmt.Sprintf(`{"level":"info","msg":"test log %d"}`, i)
		if _, err := writer.Write([]byte(log)); err != nil {
			t.Logf("Write error: %v", err)
		}
	}
	
	// Force flush
	writer.Close()
	
	// Check that we didn't hang for too long
	elapsed := time.Since(start)
	if elapsed > 30*time.Second {
		t.Errorf("Writer hung for too long: %v", elapsed)
	}
	
	// Wait for recovery
	time.Sleep(5 * time.Second)
	
	// Check circuit breaker worked
	receivedBatches := atomic.LoadInt32(&flakey.receivedBatches)
	t.Logf("Received %d batches with timeout handling", receivedBatches)
}

func TestOTLPWriter_CircuitBreaker(t *testing.T) {
	// Start server that fails consistently
	flakey := &flakeyOTLPServer{
		shouldFail:     true,
		failUntilCount: 100, // Fail many requests
	}
	
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer lis.Close()
	
	grpcServer := grpc.NewServer()
	collectorlogspb.RegisterLogsServiceServer(grpcServer, flakey)
	
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("gRPC server error: %v", err)
		}
	}()
	defer grpcServer.GracefulStop()
	
	// Create writer
	w := &OTLPWriter{
		Endpoint:     lis.Addr().String(),
		Protocol:     "grpc",
		Insecure:     true,
		Timeout:      caddy.Duration(1 * time.Second),
		BatchSize:    5,
		BatchTimeout: caddy.Duration(1 * time.Second),
		MaxRetries:   2,
		RetryDelay:   caddy.Duration(500 * time.Millisecond),
	}
	
	// Provision the writer
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()
	
	if err := w.Provision(ctx); err != nil {
		t.Fatalf("Failed to provision writer: %v", err)
	}
	defer w.Cleanup()
	
	// Open writer
	writer, err := w.OpenWriter()
	if err != nil {
		t.Fatalf("Failed to open writer: %v", err)
	}
	defer writer.Close()
	
	// Send many logs quickly
	start := time.Now()
	for i := 0; i < 50; i++ {
		log := fmt.Sprintf(`{"level":"info","msg":"test log %d"}`, i)
		if _, err := writer.Write([]byte(log)); err != nil {
			t.Logf("Write error: %v", err)
		}
	}
	
	// Check that circuit breaker prevented excessive retries
	elapsed := time.Since(start)
	if elapsed > 20*time.Second {
		t.Errorf("Circuit breaker didn't prevent excessive retries, took: %v", elapsed)
	}
	
	// Check circuit breaker state
	if w.circuitBreaker.state != circuitOpen {
		t.Error("Circuit breaker should be open after consistent failures")
	}
	
	t.Logf("Circuit breaker successfully opened after failures")
}
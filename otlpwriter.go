// Package caddyotlplogs implements a Caddy log writer that exports logs via OTLP.
package caddyotlplogs

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	collectorlogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/grpc/keepalive"
)

// debugf writes debug output to stderr to avoid circular logging dependencies
func (w *OTLPWriter) debugf(format string, args ...interface{}) {
	if w.Debug {
		fmt.Fprintf(os.Stderr, "[DEBUG OTLP] "+format+"\n", args...)
	}
}

// infof writes info output to stderr to avoid circular logging dependencies  
func (w *OTLPWriter) infof(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[INFO OTLP] "+format+"\n", args...)
}

// warnf writes warning output to stderr to avoid circular logging dependencies
func (w *OTLPWriter) warnf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[WARN OTLP] "+format+"\n", args...)
}

func init() {
	caddy.RegisterModule(&OTLPWriter{})
}

// OTLPWriter is a Caddy log writer that sends logs to an OTLP endpoint.
type OTLPWriter struct {
	// Endpoint is the OTLP endpoint URL.
	// If not specified, uses OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_EXPORTER_OTLP_LOGS_ENDPOINT
	Endpoint string `json:"endpoint,omitempty"`

	// Protocol specifies the OTLP transport protocol (grpc or http/protobuf).
	// If not specified, uses OTEL_EXPORTER_OTLP_PROTOCOL or OTEL_EXPORTER_OTLP_LOGS_PROTOCOL
	Protocol string `json:"protocol,omitempty"`

	// Headers are additional headers to send with each request.
	// If not specified, uses OTEL_EXPORTER_OTLP_HEADERS or OTEL_EXPORTER_OTLP_LOGS_HEADERS
	Headers map[string]string `json:"headers,omitempty"`

	// Timeout for export operations.
	// If not specified, uses OTEL_EXPORTER_OTLP_TIMEOUT or OTEL_EXPORTER_OTLP_LOGS_TIMEOUT
	Timeout caddy.Duration `json:"timeout,omitempty"`

	// Insecure disables TLS verification when true.
	// If not specified, uses OTEL_EXPORTER_OTLP_INSECURE or OTEL_EXPORTER_OTLP_LOGS_INSECURE
	Insecure bool `json:"insecure,omitempty"`

	// ServiceName sets the service.name resource attribute.
	// If not specified, uses OTEL_SERVICE_NAME or defaults to "caddy"
	ServiceName string `json:"service_name,omitempty"`

	// ResourceAttributes are additional resource attributes.
	// If not specified, uses OTEL_RESOURCE_ATTRIBUTES
	ResourceAttributes map[string]string `json:"resource_attributes,omitempty"`

	// BatchSize is the number of logs to batch before sending.
	BatchSize int `json:"batch_size,omitempty"`

	// BatchTimeout is the maximum time to wait before sending a batch.
	BatchTimeout caddy.Duration `json:"batch_timeout,omitempty"`

	// MaxRetries is the maximum number of retry attempts for failed exports.
	MaxRetries int `json:"max_retries,omitempty"`

	// RetryDelay is the initial delay between retry attempts.
	RetryDelay caddy.Duration `json:"retry_delay,omitempty"`

	// Debug enables verbose debug logging for troubleshooting.
	// Can also be enabled via CADDY_OTLP_DEBUG environment variable.
	Debug bool `json:"debug,omitempty"`

	logger       *zap.Logger
	resource     *resourcepb.Resource
	grpcConn     *grpc.ClientConn
	grpcClient   collectorlogspb.LogsServiceClient
	httpClient   *http.Client
	httpEndpoint string
	
	mu           sync.Mutex
	logsBatch    []*logspb.LogRecord
	batchTimer   *time.Timer
	closed       bool
	closeChan    chan struct{}
	failedBatches int64
	
	// Initialization state
	initialized     bool
	initMu          sync.RWMutex
	initChan        chan struct{}

	// Retry queue for failed batches
	retryQueue     chan *retryItem
	retryWorkerDone chan struct{}

	// Circuit breaker state
	circuitBreaker *circuitBreaker
	
	// Connection health
	lastSuccessfulSend time.Time
	connectionHealthy  bool
}

// retryItem represents a batch to be retried
type retryItem struct {
	logs     []*logspb.LogRecord
	attempts int
	nextRetry time.Time
}

// circuitBreaker implements a simple circuit breaker pattern
type circuitBreaker struct {
	mu              sync.Mutex
	state           int // 0=closed, 1=open, 2=half-open
	failures        int
	successCount    int
	lastFailureTime time.Time
	cooldownUntil   time.Time
}

const (
	circuitClosed = iota
	circuitOpen
	circuitHalfOpen
)

// getCircuitBreakerStateName returns a human-readable name for the circuit breaker state
func (w *OTLPWriter) getCircuitBreakerStateName(state int) string {
	switch state {
	case circuitClosed:
		return "closed"
	case circuitOpen:
		return "open"
	case circuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// drainOldestFromRetryQueue attempts to remove the oldest item from retry queue
func (w *OTLPWriter) drainOldestFromRetryQueue() bool {
	select {
	case <-w.retryQueue:
		return true
	default:
		return false
	}
}

// checkConnectionHealth performs a health check on the connection
func (w *OTLPWriter) checkConnectionHealth() {
	w.mu.Lock()
	timeSinceLastSuccess := time.Since(w.lastSuccessfulSend)
	healthy := w.connectionHealthy
	w.mu.Unlock()
	
	w.debugf("performing connection health check: time_since_last_success=%v, currently_healthy=%v", timeSinceLastSuccess, healthy)
	
	// If we haven't sent successfully in a while, try to reconnect
	if timeSinceLastSuccess > 2*time.Minute {
		w.warnf("no successful sends recently, attempting reconnection: time_since_last_success=%v", timeSinceLastSuccess)
		if err := w.reconnect(); err != nil {
			w.logger.Error("health check reconnection failed", zap.Error(err))
		} else {
			w.debugf("health check reconnection successful")
		}
	}
}

// CaddyModule returns the Caddy module information.
func (*OTLPWriter) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.logging.writers.otlp",
		New: func() caddy.Module { return new(OTLPWriter) },
	}
}

// Provision sets up the OTLP writer.
func (w *OTLPWriter) Provision(ctx caddy.Context) error {
	w.logger = ctx.Logger(w)
	if w.logger == nil {
		return fmt.Errorf("failed to get logger from context")
	}
	w.closeChan = make(chan struct{})

	// Check for debug flag from environment variable
	if !w.Debug {
		if debug := os.Getenv("CADDY_OTLP_DEBUG"); debug == "true" || debug == "1" {
			w.Debug = true
		}
	}

	// Note: Cannot use logger during Provision as we're part of the logging infrastructure
	// being set up. Debug output will be shown when workers start.

	// Set defaults from environment variables
	if w.Endpoint == "" {
		if endpoint := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"); endpoint != "" {
			w.Endpoint = endpoint
		} else if endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); endpoint != "" {
			w.Endpoint = endpoint
		}
	}

	if w.Protocol == "" {
		if protocol := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL"); protocol != "" {
			w.Protocol = protocol
		} else if protocol := os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"); protocol != "" {
			w.Protocol = protocol
		} else {
			w.Protocol = "grpc"
		}
	}

	if w.Headers == nil {
		w.Headers = make(map[string]string)
	}
	// Parse headers from environment
	w.parseHeadersFromEnv("OTEL_EXPORTER_OTLP_LOGS_HEADERS")
	w.parseHeadersFromEnv("OTEL_EXPORTER_OTLP_HEADERS")

	if w.Timeout == 0 {
		if timeout := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_TIMEOUT"); timeout != "" {
			if d, err := time.ParseDuration(timeout + "ms"); err == nil {
				w.Timeout = caddy.Duration(d)
			}
		} else if timeout := os.Getenv("OTEL_EXPORTER_OTLP_TIMEOUT"); timeout != "" {
			if d, err := time.ParseDuration(timeout + "ms"); err == nil {
				w.Timeout = caddy.Duration(d)
			}
		} else {
			w.Timeout = caddy.Duration(10 * time.Second)
		}
	}

	if !w.Insecure {
		if insecure := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_INSECURE"); insecure == "true" {
			w.Insecure = true
		} else if insecure := os.Getenv("OTEL_EXPORTER_OTLP_INSECURE"); insecure == "true" {
			w.Insecure = true
		}
	}

	if w.ServiceName == "" {
		if serviceName := os.Getenv("OTEL_SERVICE_NAME"); serviceName != "" {
			w.ServiceName = serviceName
		} else {
			w.ServiceName = "caddy"
		}
	}

	if w.ResourceAttributes == nil {
		w.ResourceAttributes = make(map[string]string)
	}
	// Parse resource attributes from environment
	if attrs := os.Getenv("OTEL_RESOURCE_ATTRIBUTES"); attrs != "" {
		for _, attr := range strings.Split(attrs, ",") {
			if kv := strings.SplitN(attr, "=", 2); len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])
				// URL decode the value as per OTLP spec
				if decoded, err := url.QueryUnescape(value); err == nil {
					value = decoded
				}
				w.ResourceAttributes[key] = value
			}
		}
	}

	// Set batch defaults
	if w.BatchSize == 0 {
		w.BatchSize = 100
	}
	if w.BatchTimeout == 0 {
		w.BatchTimeout = caddy.Duration(5 * time.Second)
	}

	// Set retry defaults
	if w.MaxRetries == 0 {
		w.MaxRetries = 3
	}
	if w.RetryDelay == 0 {
		w.RetryDelay = caddy.Duration(1 * time.Second)
	}

	// Initialize retry queue and circuit breaker
	w.retryQueue = make(chan *retryItem, 1000) // Buffer up to 1000 failed batches
	w.retryWorkerDone = make(chan struct{})
	w.circuitBreaker = &circuitBreaker{
		state: circuitClosed,
	}
	w.lastSuccessfulSend = time.Now()
	w.connectionHealthy = true

	// Create resource
	attrs := []attribute.KeyValue{
		attribute.String("service.name", w.ServiceName),
	}
	for k, v := range w.ResourceAttributes {
		attrs = append(attrs, attribute.String(k, v))
	}
	res, err := resource.New(context.Background(),
		resource.WithAttributes(attrs...),
		resource.WithHost(),
		resource.WithProcess(),
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %w", err)
	}

	// Convert to protobuf resource
	w.resource = &resourcepb.Resource{
		Attributes: make([]*commonpb.KeyValue, 0, len(res.Attributes())),
	}
	for _, attr := range res.Attributes() {
		w.resource.Attributes = append(w.resource.Attributes, toKeyValue(attr))
	}

	// Note: Cannot use logger OR initialize external connections during Provision 
	// as we're part of the logging infrastructure being set up. Both logging calls
	// and external operations (like gRPC connections) can trigger logging internally.
	// All initialization will be deferred to worker threads.

	// Initialize synchronization primitives
	w.initChan = make(chan struct{})
	
	// Start initialization worker that will safely set up clients and then start other workers
	go w.initializeAndRun()

	return nil
}

// initializeAndRun safely initializes clients and starts workers after logging is ready
func (w *OTLPWriter) initializeAndRun() {
	// Note: Cannot use logger here as we're still part of the logging infrastructure setup
	// Use stderr for critical debugging output instead
	if w.Debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] OTLP writer starting safe initialization (endpoint: %s, protocol: %s)\n", 
			w.Endpoint, w.Protocol)
	}
	
	// Initialize client based on protocol
	var err error
	switch strings.ToLower(w.Protocol) {
	case "grpc":
		err = w.initGRPCClient()
	case "http/protobuf", "http":
		err = w.initHTTPClient()
	default:
		err = fmt.Errorf("unsupported protocol: %s (supported: grpc, http/protobuf)", w.Protocol)
	}
	
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] OTLP writer failed to initialize client: %v\n", err)
		return
	}
	
	// Mark as initialized
	w.initMu.Lock()
	w.initialized = true
	close(w.initChan)
	w.initMu.Unlock()
	
	if w.Debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] OTLP client initialized successfully (protocol: %s, endpoint: %s)\n", 
			w.Protocol, w.Endpoint)
	}
	
	// Now start all the worker threads
	go w.batchProcessor()
	go w.retryWorker()
	go w.connectionMonitor()
}

// waitForInitialization waits for the writer to be fully initialized
func (w *OTLPWriter) waitForInitialization() bool {
	w.initMu.RLock()
	if w.initialized {
		w.initMu.RUnlock()
		return true
	}
	initChan := w.initChan
	w.initMu.RUnlock()
	
	// Wait for initialization with timeout
	select {
	case <-initChan:
		return true
	case <-time.After(30 * time.Second):
		return false
	}
}

// String returns a human-readable string representation of this writer.
func (w *OTLPWriter) String() string {
	return fmt.Sprintf("OTLP %s to %s", w.Protocol, w.Endpoint)
}

// WriterKey returns a unique key for this writer.
func (w *OTLPWriter) WriterKey() string {
	return fmt.Sprintf("otlp:%s:%s", w.Protocol, w.Endpoint)
}

// OpenWriter returns an io.WriteCloser that writes logs to the OTLP endpoint.
func (w *OTLPWriter) OpenWriter() (io.WriteCloser, error) {
	return &otlpWriteCloser{w: w}, nil
}

// Cleanup shuts down the OTLP exporter.
func (w *OTLPWriter) Cleanup() error {
	w.mu.Lock()
	
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	
	// Make a final attempt to send remaining logs
	if len(w.logsBatch) > 0 {
		logCount := len(w.logsBatch)
		// Force circuit breaker to closed for final attempt
		w.circuitBreaker.mu.Lock()
		w.circuitBreaker.state = circuitClosed
		w.circuitBreaker.mu.Unlock()
		
		if err := w.sendBatch(); err != nil {
			w.reportBatchError(err, logCount, "Cleanup")
		}
	}
	w.mu.Unlock()
	
	// Signal shutdown
	close(w.closeChan)

	// Process any remaining items in retry queue with extended timeout
	if w.retryQueue != nil && len(w.retryQueue) > 0 {
		w.infof("processing remaining retry queue items: count=%d", len(w.retryQueue))
		
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		
		for len(w.retryQueue) > 0 {
			select {
			case item := <-w.retryQueue:
				if err := w.sendBatchWithRetryContext(ctx, item.logs); err != nil {
					w.logger.Error("failed to send batch during cleanup",
						zap.Error(err),
						zap.Int("logs", len(item.logs)))
				}
			case <-ctx.Done():
				w.logger.Warn("cleanup timeout reached, some logs may be lost",
					zap.Int("remaining", len(w.retryQueue)))
				break
			}
		}
	}
	
	// Close retry queue and wait for worker
	if w.retryQueue != nil {
		close(w.retryQueue)
	}
	
	if w.retryWorkerDone != nil {
		select {
		case <-w.retryWorkerDone:
		case <-time.After(5 * time.Second):
			w.warnf("retry worker did not finish in time")
		}
	}

	// Clean up connections
	if w.grpcConn != nil {
		return w.grpcConn.Close()
	}

	return nil
}

// initGRPCClient initializes the gRPC client with connection management.
func (w *OTLPWriter) initGRPCClient() error {
	endpoint := w.normalizeEndpoint(w.Endpoint, false)
	
	// Debug logging moved to worker threads to avoid panic during provision
	
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64 * 1024 * 1024)),
		// Add keepalive parameters to detect stale connections
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second, // Send pings every 30 seconds
			Timeout:             10 * time.Second, // Wait 10 seconds for ping ack
			PermitWithoutStream: false,            // Only send pings when there are active streams
		}),
		// Enable retries for transient failures
		grpc.WithDefaultServiceConfig(`{
			"methodConfig": [{
				"name": [{"service": "opentelemetry.proto.collector.logs.v1.LogsService"}],
				"retryPolicy": {
					"maxAttempts": 3,
					"initialBackoff": "0.1s",
					"maxBackoff": "1s",
					"backoffMultiplier": 2,
					"retryableStatusCodes": ["UNAVAILABLE", "DEADLINE_EXCEEDED"]
				}
			}]
		}`),
	}

	if w.Insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}

	// Add headers as metadata
	if len(w.Headers) > 0 {
		opts = append(opts, grpc.WithUnaryInterceptor(w.grpcHeadersInterceptor()))
	}

	// Create connection using non-blocking approach
	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		return fmt.Errorf("failed to create gRPC connection to %s: %w", endpoint, err)
	}

	w.grpcConn = conn
	w.grpcClient = collectorlogspb.NewLogsServiceClient(conn)
	
	// Success logging moved to worker threads to avoid panic during provision
	
	return nil
}

// grpcHeadersInterceptor creates a gRPC interceptor for headers.
func (w *OTLPWriter) grpcHeadersInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		for k, v := range w.Headers {
			ctx = metadata.AppendToOutgoingContext(ctx, k, v)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// initHTTPClient initializes the HTTP client.
func (w *OTLPWriter) initHTTPClient() error {
	w.httpEndpoint = w.normalizeEndpoint(w.Endpoint, true)
	
	// Debug logging moved to worker threads to avoid panic during provision
	
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: w.Insecure,
		},
		// Connection pooling settings
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
		
		// Timeouts
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: time.Duration(w.Timeout),
		
		// Enable HTTP/2
		ForceAttemptHTTP2: true,
	}

	w.httpClient = &http.Client{
		Transport: transport,
		Timeout:   time.Duration(w.Timeout),
	}

	// Success logging moved to worker threads to avoid panic during provision

	return nil
}

// parseHeadersFromEnv parses headers from an environment variable.
func (w *OTLPWriter) parseHeadersFromEnv(envVar string) {
	if headers := os.Getenv(envVar); headers != "" {
		for _, header := range strings.Split(headers, ",") {
			if kv := strings.SplitN(header, "=", 2); len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])
				// URL decode the value as per OTLP spec
				if decoded, err := url.QueryUnescape(value); err == nil {
					value = decoded
				}
				w.Headers[key] = value
			}
		}
	}
}

// normalizeEndpoint normalizes the endpoint URL for the given protocol.
func (w *OTLPWriter) normalizeEndpoint(endpoint string, isHTTP bool) string {
	if endpoint == "" {
		if isHTTP {
			return "http://localhost:4318/v1/logs"
		}
		return "localhost:4317"
	}

	// Parse the URL to check for scheme
	if strings.Contains(endpoint, "://") {
		u, err := url.Parse(endpoint)
		if err == nil {
			if isHTTP {
				// For HTTP, ensure path includes /v1/logs
				if !strings.HasSuffix(u.Path, "/v1/logs") {
					if !strings.HasSuffix(u.Path, "/") {
						u.Path += "/"
					}
					u.Path += "v1/logs"
				}
				return u.String()
			} else {
				// For gRPC, extract host:port from URL
				host := u.Host
				if host == "" {
					// Handle case where scheme is present but host parsing failed
					// This might happen with malformed URLs
					return endpoint
				}
				return host
			}
		}
	}

	// No scheme present, handle as before
	if isHTTP {
		// Add scheme based on insecure setting
		if w.Insecure {
			endpoint = "http://" + endpoint
		} else {
			endpoint = "https://" + endpoint
		}
		
		// Ensure path includes /v1/logs
		u, err := url.Parse(endpoint)
		if err == nil && !strings.HasSuffix(u.Path, "/v1/logs") {
			if !strings.HasSuffix(u.Path, "/") {
				u.Path += "/"
			}
			u.Path += "v1/logs"
			endpoint = u.String()
		}
	}

	return endpoint
}

// batchProcessor processes log batches.
func (w *OTLPWriter) batchProcessor() {
	// Log configuration details on first run (safe here as logger is now initialized)
	w.logger.Info("OTLP log writer configured",
		zap.String("endpoint", w.Endpoint),
		zap.String("protocol", w.Protocol),
		zap.String("service_name", w.ServiceName),
	)
	
	w.debugf("batch processor worker started: batch_timeout=%v", time.Duration(w.BatchTimeout))
	ticker := time.NewTicker(time.Duration(w.BatchTimeout))
	defer ticker.Stop()

	for {
		select {
		case <-w.closeChan:
			w.debugf("batch processor worker shutting down")
			return
		case <-ticker.C:
			w.mu.Lock()
			if len(w.logsBatch) > 0 {
				// Check if circuit breaker allows sending
				if w.circuitBreaker.canSend() {
					logCount := len(w.logsBatch)
					if err := w.sendBatch(); err != nil {
						if !strings.Contains(err.Error(), "circuit breaker is open") {
							w.reportBatchError(err, logCount, "batchProcessor timeout")
						}
					}
				} else {
					// Circuit breaker is open, keep batch for later
					w.debugf("skipping batch send due to open circuit breaker: pending_logs=%d", len(w.logsBatch))
				}
			}
			w.mu.Unlock()
		}
	}
}

// addLog adds a log record to the batch.
func (w *OTLPWriter) addLog(record *logspb.LogRecord) {
	// Wait for initialization to complete before processing logs
	if !w.waitForInitialization() {
		// Initialization failed or timed out - log critical error to stderr
		fmt.Fprintf(os.Stderr, "[CRITICAL] OTLP writer not initialized, dropping log entry\n")
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.logsBatch = append(w.logsBatch, record)
	
	w.debugf("added log to batch: current_batch_size=%d, max_batch_size=%d", len(w.logsBatch), w.BatchSize)

	if len(w.logsBatch) >= w.BatchSize {
		w.debugf("batch size threshold reached, attempting to send: batch_size=%d", len(w.logsBatch))
		// Check if circuit breaker allows sending
		if w.circuitBreaker.canSend() {
			logCount := len(w.logsBatch)
			if err := w.sendBatch(); err != nil {
				if !strings.Contains(err.Error(), "circuit breaker is open") {
					w.reportBatchError(err, logCount, "addLog batch size threshold")
				}
			}
		} else {
			// Circuit breaker is open, but batch is full
			// Try to queue for retry
			if len(w.logsBatch) > w.BatchSize*2 {
				// Batch is getting too large, force send to retry queue
				batchCopy := make([]*logspb.LogRecord, len(w.logsBatch))
				copy(batchCopy, w.logsBatch)
				
				select {
				case w.retryQueue <- &retryItem{
					logs:      batchCopy,
					attempts:  0,
					nextRetry: time.Now().Add(5 * time.Second),
				}:
					w.logsBatch = w.logsBatch[:0]
					w.warnf("circuit breaker open, queued oversized batch: logs=%d", len(batchCopy))
				default:
					// Retry queue full, have to drop oldest logs
					dropped := len(w.logsBatch) - w.BatchSize
					w.logsBatch = w.logsBatch[dropped:]
					w.logger.Error("circuit breaker open and retry queue full, dropping oldest logs",
						zap.Int("dropped", dropped))
					w.reportBatchError(fmt.Errorf("forced drop due to buffer overflow"), dropped, "addLog overflow")
				}
			}
		}
	}
}

// reportBatchError reports batch send errors to stderr for visibility in containerized environments
func (w *OTLPWriter) reportBatchError(err error, logCount int, location string) {
	if err == nil {
		return
	}

	w.failedBatches++
	
	// Write to stderr for immediate container visibility
	fmt.Fprintf(os.Stderr, "[CRITICAL] OTLP log export failed at %s\n", location)
	fmt.Fprintf(os.Stderr, "  Endpoint: %s (%s)\n", w.Endpoint, w.Protocol)
	fmt.Fprintf(os.Stderr, "  Error: %v\n", err)
	fmt.Fprintf(os.Stderr, "  Lost logs: %d\n", logCount)
	fmt.Fprintf(os.Stderr, "  Total failed batches: %d\n", w.failedBatches)
	fmt.Fprintf(os.Stderr, "  Service: %s\n", w.ServiceName)
	fmt.Fprintf(os.Stderr, "  Time: %s\n", time.Now().Format(time.RFC3339))
	
	// Also log structured error for debugging
	w.logger.Error("OTLP log export failed",
		zap.Error(err),
		zap.String("location", location),
		zap.String("endpoint", w.Endpoint),
		zap.String("protocol", w.Protocol),
		zap.Int("lost_logs", logCount),
		zap.Int64("total_failed_batches", w.failedBatches),
	)
}

// sendBatch sends the current batch of logs.
func (w *OTLPWriter) sendBatch() error {
	if len(w.logsBatch) == 0 {
		w.debugf("sendBatch called with empty batch")
		return nil
	}
	
	w.debugf("sending batch: logs_count=%d, protocol=%s", len(w.logsBatch), w.Protocol)

	// Make a copy of the batch for sending
	batchCopy := make([]*logspb.LogRecord, len(w.logsBatch))
	copy(batchCopy, w.logsBatch)
	
	// Try to send the batch first before clearing
	err := w.sendBatchWithRetryUnsafe(batchCopy)
	
	// Only clear batch if send was successful
	if err == nil {
		w.logsBatch = w.logsBatch[:0]
		w.debugf("batch sent successfully, batch cleared")
		return nil
	}
	
	// Send failed - handle the error
	// Check if circuit breaker is open
	if strings.Contains(err.Error(), "circuit breaker is open") {
		// Don't clear batch yet - will retry when circuit breaker closes
		w.debugf("circuit breaker open, keeping batch for later retry: logs=%d", len(w.logsBatch))
		return err
	}
	
	// Try to queue for retry
	select {
	case w.retryQueue <- &retryItem{
		logs:      batchCopy,
		attempts:  0,
		nextRetry: time.Now().Add(time.Duration(w.RetryDelay)),
	}:
		// Successfully queued - clear the batch
		w.logsBatch = w.logsBatch[:0]
		w.warnf("queued failed batch for retry: logs=%d, error=%v", len(batchCopy), err)
	case <-w.closeChan:
		// Channel is being closed, clear batch and return
		w.logsBatch = w.logsBatch[:0]
		w.debugf("retry queue closed during send, dropping batch: logs=%d", len(batchCopy))
		return err
	default:
		// Retry queue is full - try to send oldest items first
		if w.drainOldestFromRetryQueue() {
			// Try again after making space
			select {
			case w.retryQueue <- &retryItem{
				logs:      batchCopy,
				attempts:  0,
				nextRetry: time.Now().Add(time.Duration(w.RetryDelay)),
			}:
				w.logsBatch = w.logsBatch[:0]
				w.logger.Warn("queued failed batch for retry after draining old items",
					zap.Int("logs", len(batchCopy)))
			case <-w.closeChan:
				// Channel is being closed, clear batch and return
				w.logsBatch = w.logsBatch[:0]
				w.logger.Debug("retry queue closed during send, dropping batch",
					zap.Int("logs", len(batchCopy)))
				return err
			default:
				// Still full - have to drop
				w.logsBatch = w.logsBatch[:0]
				w.logger.Error("retry queue full after drain attempt, dropping batch",
					zap.Int("logs", len(batchCopy)))
				w.reportBatchError(fmt.Errorf("retry queue full"), len(batchCopy), "sendBatch")
			}
		} else {
			// Couldn't drain - drop the batch
			w.logsBatch = w.logsBatch[:0]
			w.logger.Error("retry queue full, dropping batch",
				zap.Int("logs", len(batchCopy)))
			w.reportBatchError(fmt.Errorf("retry queue full"), len(batchCopy), "sendBatch")
		}
	}
	
	return err
}

// otlpWriteCloser implements io.WriteCloser for the OTLP writer.
type otlpWriteCloser struct {
	w *OTLPWriter
}

// Write sends log entries to the OTLP endpoint.
func (owc *otlpWriteCloser) Write(p []byte) (n int, err error) {
	// Parse the log entry
	var entry map[string]interface{}
	if err := json.Unmarshal(p, &entry); err != nil {
		// If not JSON, treat as plain text
		entry = map[string]interface{}{
			"message": string(p),
		}
	}

	// Create log record
	record := &logspb.LogRecord{
		TimeUnixNano:   uint64(time.Now().UnixNano()),
		SeverityNumber: owc.extractSeverity(entry),
		SeverityText:   owc.extractLevel(entry),
		Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: owc.extractMessage(entry)}},
		Attributes:     owc.extractAttributes(entry),
	}

	// Extract trace context if available
	if traceID, ok := entry["trace_id"].(string); ok && traceID != "" {
		if data, err := parseTraceID(traceID); err == nil {
			record.TraceId = data
		}
	}
	if spanID, ok := entry["span_id"].(string); ok && spanID != "" {
		if data, err := parseSpanID(spanID); err == nil {
			record.SpanId = data
		}
	}

	// Add to batch
	owc.w.addLog(record)

	return len(p), nil
}

// Close implements io.Closer.
func (owc *otlpWriteCloser) Close() error {
	// Force flush
	owc.w.mu.Lock()
	defer owc.w.mu.Unlock()
	logCount := len(owc.w.logsBatch)
	if err := owc.w.sendBatch(); err != nil {
		owc.w.reportBatchError(err, logCount, "Close")
		return err
	}
	return nil
}

// extractLevel extracts the log level string.
func (owc *otlpWriteCloser) extractLevel(entry map[string]interface{}) string {
	if lvl, ok := entry["level"].(string); ok {
		return lvl
	}
	return "INFO"
}

// extractSeverity extracts the severity number.
func (owc *otlpWriteCloser) extractSeverity(entry map[string]interface{}) logspb.SeverityNumber {
	level := owc.extractLevel(entry)
	switch strings.ToUpper(level) {
	case "TRACE":
		return logspb.SeverityNumber_SEVERITY_NUMBER_TRACE
	case "DEBUG":
		return logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG
	case "INFO":
		return logspb.SeverityNumber_SEVERITY_NUMBER_INFO
	case "WARN", "WARNING":
		return logspb.SeverityNumber_SEVERITY_NUMBER_WARN
	case "ERROR":
		return logspb.SeverityNumber_SEVERITY_NUMBER_ERROR
	case "FATAL":
		return logspb.SeverityNumber_SEVERITY_NUMBER_FATAL
	default:
		return logspb.SeverityNumber_SEVERITY_NUMBER_INFO
	}
}

// extractMessage extracts the message from the entry.
func (owc *otlpWriteCloser) extractMessage(entry map[string]interface{}) string {
	if msg, ok := entry["msg"].(string); ok {
		return msg
	}
	if msg, ok := entry["message"].(string); ok {
		return msg
	}
	// For Caddy access logs, construct message from request info
	if request, ok := entry["request"].(map[string]interface{}); ok {
		if method, ok := request["method"].(string); ok {
			if uri, ok := request["uri"].(string); ok {
				if status, ok := entry["status"].(float64); ok {
					return fmt.Sprintf("%s %s %d", method, uri, int(status))
				}
				return fmt.Sprintf("%s %s", method, uri)
			}
		}
	}
	// Fallback to JSON representation
	if data, err := json.Marshal(entry); err == nil {
		return string(data)
	}
	return ""
}

// extractAttributes extracts attributes from the entry.
func (owc *otlpWriteCloser) extractAttributes(entry map[string]interface{}) []*commonpb.KeyValue {
	attrs := make([]*commonpb.KeyValue, 0)
	
	// Skip common fields that are handled separately
	skipFields := map[string]bool{
		"level": true, "msg": true, "message": true, 
		"ts": true, "timestamp": true, "time": true,
		"trace_id": true, "span_id": true,
	}

	for k, v := range entry {
		if skipFields[k] {
			continue
		}
		attrs = append(attrs, toKeyValue(attribute.KeyValue{
			Key:   attribute.Key(k),
			Value: attributeValue(v),
		}))
	}

	return attrs
}

// Helper functions

func toKeyValue(attr attribute.KeyValue) *commonpb.KeyValue {
	kv := &commonpb.KeyValue{
		Key: string(attr.Key),
	}

	switch attr.Value.Type() {
	case attribute.STRING:
		kv.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: attr.Value.AsString()}}
	case attribute.BOOL:
		kv.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: attr.Value.AsBool()}}
	case attribute.INT64:
		kv.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: attr.Value.AsInt64()}}
	case attribute.FLOAT64:
		kv.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: attr.Value.AsFloat64()}}
	default:
		kv.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: attr.Value.AsString()}}
	}

	return kv
}

func attributeValue(v interface{}) attribute.Value {
	switch val := v.(type) {
	case string:
		return attribute.StringValue(val)
	case bool:
		return attribute.BoolValue(val)
	case int, int8, int16, int32, int64:
		return attribute.Int64Value(toInt64(val))
	case uint, uint8, uint16, uint32, uint64:
		return attribute.Int64Value(int64(toUint64(val)))
	case float32, float64:
		return attribute.Float64Value(toFloat64(val))
	default:
		// Convert complex types to JSON string
		if data, err := json.Marshal(val); err == nil {
			return attribute.StringValue(string(data))
		}
		return attribute.StringValue(fmt.Sprintf("%v", val))
	}
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	default:
		return 0
	}
}

func toUint64(v interface{}) uint64 {
	switch n := v.(type) {
	case uint:
		return uint64(n)
	case uint8:
		return uint64(n)
	case uint16:
		return uint64(n)
	case uint32:
		return uint64(n)
	case uint64:
		return n
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float32:
		return float64(n)
	case float64:
		return n
	default:
		return 0
	}
}

func parseTraceID(s string) ([]byte, error) {
	// Remove any dashes
	s = strings.ReplaceAll(s, "-", "")
	
	// Trace ID should be 32 hex characters (16 bytes)
	if len(s) != 32 {
		return nil, fmt.Errorf("invalid trace ID length: %d", len(s))
	}
	
	// Validate all characters are hex
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return nil, fmt.Errorf("invalid hex character in trace ID: %c", c)
		}
	}
	
	data := make([]byte, 16)
	for i := 0; i < 16; i++ {
		var b byte
		_, err := fmt.Sscanf(s[i*2:i*2+2], "%02x", &b)
		if err != nil {
			return nil, err
		}
		data[i] = b
	}
	return data, nil
}

func parseSpanID(s string) ([]byte, error) {
	// Remove any dashes
	s = strings.ReplaceAll(s, "-", "")
	
	// Span ID should be 16 hex characters (8 bytes)
	if len(s) != 16 {
		return nil, fmt.Errorf("invalid span ID length: %d", len(s))
	}
	
	// Validate all characters are hex
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return nil, fmt.Errorf("invalid hex character in span ID: %c", c)
		}
	}
	
	data := make([]byte, 8)
	for i := 0; i < 8; i++ {
		var b byte
		_, err := fmt.Sscanf(s[i*2:i*2+2], "%02x", &b)
		if err != nil {
			return nil, err
		}
		data[i] = b
	}
	return data, nil
}

// UnmarshalCaddyfile implements caddyfile.Unmarshaler.
// Syntax:
//
//	otlp {
//	    endpoint <url>
//	    protocol <grpc|http>
//	    insecure [true|false]
//	    timeout <duration>
//	    headers {
//	        <name> <value>
//	    }
//	    service_name <name>
//	    resource_attributes {
//	        <name> <value>
//	    }
//	    batch_size <size>
//	    batch_timeout <duration>
//	}
func (w *OTLPWriter) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		// No arguments on the same line  
		if d.NextArg() {
			return d.ArgErr()
		}

		for d.NextBlock(0) {
			switch d.Val() {
			case "endpoint":
				if !d.NextArg() {
					return d.ArgErr()
				}
				w.Endpoint = d.Val()

			case "protocol":
				if !d.NextArg() {
					return d.ArgErr()
				}
				w.Protocol = d.Val()

			case "insecure":
				if d.NextArg() {
					val := d.Val()
					if val == "true" {
						w.Insecure = true
					} else if val == "false" {
						w.Insecure = false
					} else {
						return d.Errf("insecure must be 'true' or 'false', got '%s'", val)
					}
				} else {
					w.Insecure = true
				}

			case "timeout":
				if !d.NextArg() {
					return d.ArgErr()
				}
				dur, err := caddy.ParseDuration(d.Val())
				if err != nil {
					return d.Errf("invalid timeout duration: %v", err)
				}
				w.Timeout = caddy.Duration(dur)

			case "headers":
				if w.Headers == nil {
					w.Headers = make(map[string]string)
				}
				for d.NextBlock(1) {
					name := d.Val()
					if !d.NextArg() {
						return d.ArgErr()
					}
					w.Headers[name] = d.Val()
				}

			case "service_name":
				if !d.NextArg() {
					return d.ArgErr()
				}
				w.ServiceName = d.Val()

			case "resource_attributes":
				if w.ResourceAttributes == nil {
					w.ResourceAttributes = make(map[string]string)
				}
				for d.NextBlock(1) {
					name := d.Val()
					if !d.NextArg() {
						return d.ArgErr()
					}
					w.ResourceAttributes[name] = d.Val()
				}

			case "batch_size":
				if !d.NextArg() {
					return d.ArgErr()
				}
				var size int
				_, err := fmt.Sscanf(d.Val(), "%d", &size)
				if err != nil {
					return d.Errf("invalid batch_size: %v", err)
				}
				w.BatchSize = size

			case "batch_timeout":
				if !d.NextArg() {
					return d.ArgErr()
				}
				dur, err := caddy.ParseDuration(d.Val())
				if err != nil {
					return d.Errf("invalid batch_timeout duration: %v", err)
				}
				w.BatchTimeout = caddy.Duration(dur)

			default:
				return d.Errf("unrecognized subdirective: %s", d.Val())
			}
		}
	}

	return nil
}

// retryWorker processes failed batches from the retry queue
func (w *OTLPWriter) retryWorker() {
	if w.Debug {
		w.logger.Debug("retry worker started")
	}
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("retry worker panic recovered", zap.Any("panic", r))
		}
		if w.Debug {
			w.logger.Debug("retry worker shutting down")
		}
		close(w.retryWorkerDone)
	}()
	
	// Create a timer for periodic health checks
	healthCheckTicker := time.NewTicker(30 * time.Second)
	defer healthCheckTicker.Stop()

	for {
		select {
		case item, ok := <-w.retryQueue:
			if !ok {
				return // Channel closed
			}

			// Wait until retry time with timeout protection
			waitTime := time.Until(item.nextRetry)
			if waitTime > 0 {
				// Cap wait time to prevent excessive blocking
				maxWait := 5 * time.Minute
				if waitTime > maxWait {
					waitTime = maxWait
				}
				
				waitTimer := time.NewTimer(waitTime)
				select {
				case <-waitTimer.C:
				case <-w.closeChan:
					waitTimer.Stop()
					return
				}
			}

			// Check if we should skip retry due to circuit breaker
			if !w.circuitBreaker.canSend() {
				// Requeue for later
				item.nextRetry = time.Now().Add(10 * time.Second)
				select {
				case w.retryQueue <- item:
					w.logger.Debug("requeued item due to circuit breaker",
						zap.Int("logs", len(item.logs)))
				case <-w.closeChan:
					// Channel is being closed, exit
					return
				default:
					w.logger.Warn("retry queue full while circuit breaker open",
						zap.Int("logs", len(item.logs)))
				}
				continue
			}

			// Try to send the batch with timeout
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(w.Timeout))
			err := w.sendBatchWithRetryContext(ctx, item.logs)
			cancel()
			
			if err != nil {
				item.attempts++
				if item.attempts < w.MaxRetries {
					// Calculate next retry with exponential backoff
					delay := time.Duration(w.RetryDelay) * time.Duration(1<<uint(item.attempts))
					// Cap maximum delay
					maxDelay := 5 * time.Minute
					if delay > maxDelay {
						delay = maxDelay
					}
					item.nextRetry = time.Now().Add(delay)
					
					// Try to requeue - check if queue is still open
					select {
					case w.retryQueue <- item:
						w.logger.Warn("requeued failed batch for retry",
							zap.Int("attempt", item.attempts),
							zap.Duration("next_delay", delay),
							zap.Int("logs", len(item.logs)),
							zap.Error(err))
					case <-w.closeChan:
						// Channel is being closed, exit
						return
					default:
						w.logger.Error("retry queue full, dropping batch",
							zap.Int("logs", len(item.logs)))
						w.reportBatchError(fmt.Errorf("retry queue full"), len(item.logs), "retryWorker")
					}
				} else {
					w.logger.Error("max retries exceeded, dropping batch",
						zap.Int("logs", len(item.logs)),
						zap.Int("attempts", item.attempts),
						zap.Error(err))
					w.reportBatchError(fmt.Errorf("max retries exceeded: %w", err), len(item.logs), "retryWorker")
				}
			} else {
				w.logger.Info("successfully sent retried batch",
					zap.Int("logs", len(item.logs)),
					zap.Int("attempts", item.attempts))
			}
			
		case <-healthCheckTicker.C:
			// Periodic health check
			w.checkConnectionHealth()

		case <-w.closeChan:
			return
		}
	}
}

// sendBatchWithRetry sends a batch with circuit breaker and connection recovery
func (w *OTLPWriter) sendBatchWithRetry(logs []*logspb.LogRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(w.Timeout))
	defer cancel()
	return w.sendBatchWithRetryContext(ctx, logs)
}

// sendBatchWithRetryUnsafe sends a batch with circuit breaker and connection recovery without acquiring mutex
// This should only be called when the mutex is already held
func (w *OTLPWriter) sendBatchWithRetryUnsafe(logs []*logspb.LogRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(w.Timeout))
	defer cancel()
	return w.sendBatchWithRetryContextUnsafe(ctx, logs)
}

// sendBatchWithRetryContext sends a batch with circuit breaker, connection recovery, and context support
func (w *OTLPWriter) sendBatchWithRetryContext(ctx context.Context, logs []*logspb.LogRecord) error {
	// Check circuit breaker
	if !w.circuitBreaker.canSend() {
		return fmt.Errorf("circuit breaker is open")
	}

	// Create request
	req := &collectorlogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				Resource: w.resource,
				ScopeLogs: []*logspb.ScopeLogs{
					{
						Scope: &commonpb.InstrumentationScope{
							Name:    "caddy",
							Version: "2.0.0",
						},
						LogRecords: logs,
					},
				},
			},
		},
	}

	var err error
	// Send based on protocol
	if w.grpcClient != nil {
		err = w.sendViaGRPCContext(ctx, req)
	} else if w.httpClient != nil {
		err = w.sendViaHTTPContext(ctx, req)
	}

	// Update circuit breaker and connection health
	if err != nil {
		// Increment failed batch counter for any send failure
		w.failedBatches++
		oldState := w.circuitBreaker.state
		w.circuitBreaker.recordFailure()
		if w.Debug && oldState != w.circuitBreaker.state {
			w.logger.Debug("circuit breaker state changed due to failure", 
				zap.String("old_state", w.getCircuitBreakerStateName(oldState)),
				zap.String("new_state", w.getCircuitBreakerStateName(w.circuitBreaker.state)),
				zap.Int("failures", w.circuitBreaker.failures))
		}
		w.updateConnectionHealth(false)
		
		// Check if we need to reconnect
		if w.isConnectionError(err) {
			w.logger.Warn("detected connection error, attempting to reconnect", zap.Error(err))
			if reconnectErr := w.reconnectSafe(); reconnectErr != nil {
				w.logger.Error("failed to reconnect", zap.Error(reconnectErr))
			}
		}
	} else {
		oldState := w.circuitBreaker.state
		w.circuitBreaker.recordSuccess()
		if w.Debug && oldState != w.circuitBreaker.state {
			w.logger.Debug("circuit breaker state changed due to success", 
				zap.String("old_state", w.getCircuitBreakerStateName(oldState)),
				zap.String("new_state", w.getCircuitBreakerStateName(w.circuitBreaker.state)),
				zap.Int("success_count", w.circuitBreaker.successCount))
		}
		w.updateConnectionHealth(true)
	}

	return err
}

// sendBatchWithRetryContextUnsafe sends a batch with circuit breaker, connection recovery, and context support without acquiring mutex
// This should only be called when the mutex is already held
func (w *OTLPWriter) sendBatchWithRetryContextUnsafe(ctx context.Context, logs []*logspb.LogRecord) error {
	// Check circuit breaker
	if !w.circuitBreaker.canSend() {
		return fmt.Errorf("circuit breaker is open")
	}

	// Create request
	req := &collectorlogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				Resource: w.resource,
				ScopeLogs: []*logspb.ScopeLogs{
					{
						Scope: &commonpb.InstrumentationScope{
							Name:    "caddy",
							Version: "2.0.0",
						},
						LogRecords: logs,
					},
				},
			},
		},
	}

	var err error
	// Send based on protocol
	if w.grpcClient != nil {
		err = w.sendViaGRPCContext(ctx, req)
	} else if w.httpClient != nil {
		err = w.sendViaHTTPContext(ctx, req)
	}

	// Update circuit breaker and connection health (using unsafe version since mutex is held)
	if err != nil {
		// Increment failed batch counter for any send failure
		w.failedBatches++
		oldState := w.circuitBreaker.state
		w.circuitBreaker.recordFailure()
		if w.Debug && oldState != w.circuitBreaker.state {
			w.logger.Debug("circuit breaker state changed due to failure (unsafe)", 
				zap.String("old_state", w.getCircuitBreakerStateName(oldState)),
				zap.String("new_state", w.getCircuitBreakerStateName(w.circuitBreaker.state)),
				zap.Int("failures", w.circuitBreaker.failures))
		}
		w.updateConnectionHealthUnsafe(false)
		
		// Check if we need to reconnect
		if w.isConnectionError(err) {
			w.logger.Warn("detected connection error, attempting to reconnect", zap.Error(err))
			if reconnectErr := w.reconnectSafe(); reconnectErr != nil {
				w.logger.Error("failed to reconnect", zap.Error(reconnectErr))
			}
		}
	} else {
		oldState := w.circuitBreaker.state
		w.circuitBreaker.recordSuccess()
		if w.Debug && oldState != w.circuitBreaker.state {
			w.logger.Debug("circuit breaker state changed due to success (unsafe)", 
				zap.String("old_state", w.getCircuitBreakerStateName(oldState)),
				zap.String("new_state", w.getCircuitBreakerStateName(w.circuitBreaker.state)),
				zap.Int("success_count", w.circuitBreaker.successCount))
		}
		w.updateConnectionHealthUnsafe(true)
	}

	return err
}

// sendViaGRPC sends logs via gRPC with proper error handling
func (w *OTLPWriter) sendViaGRPC(req *collectorlogspb.ExportLogsServiceRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(w.Timeout))
	defer cancel()
	return w.sendViaGRPCContext(ctx, req)
}

// sendViaGRPCContext sends logs via gRPC with context
func (w *OTLPWriter) sendViaGRPCContext(ctx context.Context, req *collectorlogspb.ExportLogsServiceRequest) error {
	_, err := w.grpcClient.Export(ctx, req)
	return err
}

// sendViaHTTP sends logs via HTTP with proper error handling
func (w *OTLPWriter) sendViaHTTP(req *collectorlogspb.ExportLogsServiceRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(w.Timeout))
	defer cancel()
	return w.sendViaHTTPContext(ctx, req)
}

// sendViaHTTPContext sends logs via HTTP with context
func (w *OTLPWriter) sendViaHTTPContext(ctx context.Context, req *collectorlogspb.ExportLogsServiceRequest) error {
	data, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal logs: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", w.httpEndpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	for k, v := range w.Headers {
		httpReq.Header.Set(k, v)
	}

	resp, err := w.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("OTLP export failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// isConnectionError checks if an error indicates a connection problem
func (w *OTLPWriter) isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	
	errStr := err.Error()
	connectionErrors := []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"no such host",
		"network is unreachable",
		"transport is closing",
		"connection closed",
		"EOF",
		"context deadline exceeded",
		"i/o timeout",
		"connection timed out",
		"no route to host",
	}
	
	for _, connErr := range connectionErrors {
		if strings.Contains(strings.ToLower(errStr), connErr) {
			return true
		}
	}
	
	return false
}

// updateConnectionHealth safely updates connection health status
func (w *OTLPWriter) updateConnectionHealth(healthy bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	w.updateConnectionHealthUnsafe(healthy)
}

// updateConnectionHealthUnsafe updates connection health status without acquiring mutex
// This should only be called when the mutex is already held
func (w *OTLPWriter) updateConnectionHealthUnsafe(healthy bool) {
	w.connectionHealthy = healthy
	if healthy {
		w.lastSuccessfulSend = time.Now()
	}
}

// reconnect attempts to reestablish the connection
func (w *OTLPWriter) reconnect() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.Debug {
		w.logger.Info("attempting connection reconnect", 
			zap.String("protocol", w.Protocol),
			zap.String("endpoint", w.Endpoint))
	}

	// Close existing connections
	if w.grpcConn != nil {
		w.grpcConn.Close()
		w.grpcConn = nil
		w.grpcClient = nil
	}

	// Reinitialize based on protocol
	var err error
	switch strings.ToLower(w.Protocol) {
	case "grpc":
		err = w.initGRPCClient()
	case "http/protobuf", "http":
		err = w.initHTTPClient()
	default:
		err = fmt.Errorf("unsupported protocol for reconnection: %s", w.Protocol)
	}

	if err != nil && w.Debug {
		w.logger.Error("connection reconnect failed", zap.Error(err))
	} else if w.Debug {
		w.logger.Info("connection reconnect successful")
	}

	return err
}

// reconnectSafe attempts to reestablish the connection without acquiring mutex
func (w *OTLPWriter) reconnectSafe() error {
	// Close existing connections
	if w.grpcConn != nil {
		w.grpcConn.Close()
		w.grpcConn = nil
		w.grpcClient = nil
	}

	// Reinitialize based on protocol
	switch strings.ToLower(w.Protocol) {
	case "grpc":
		return w.initGRPCClient()
	case "http/protobuf", "http":
		return w.initHTTPClient()
	default:
		return fmt.Errorf("unsupported protocol for reconnection: %s", w.Protocol)
	}
}

// connectionMonitor monitors connection health and triggers reconnection when needed
func (w *OTLPWriter) connectionMonitor() {
	if w.Debug {
		w.logger.Debug("connection monitor worker started")
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			w.checkConnectionHealth()
		case <-w.closeChan:
			if w.Debug {
				w.logger.Debug("connection monitor worker shutting down")
			}
			return
		}
	}
}

// Circuit breaker methods
func (cb *circuitBreaker) canSend() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case circuitOpen:
		if now.After(cb.cooldownUntil) {
			cb.state = circuitHalfOpen
			cb.successCount = 0
			// Note: Debug logging would require access to logger, which is not available in this context
			// This will be logged at the caller level
			return true
		}
		return false
	case circuitHalfOpen:
		return true
	default: // circuitClosed
		return true
	}
}

func (cb *circuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0
	
	if cb.state == circuitHalfOpen {
		cb.successCount++
		if cb.successCount >= 2 { // Need 2 consecutive successes to close
			oldState := cb.state
			cb.state = circuitClosed
			cb.failures = 0
			// Note: Debug logging would require access to logger, which is not available in this context
			// This will be logged at the caller level
			_ = oldState // Avoid unused variable warning
		}
	}
}

func (cb *circuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailureTime = time.Now()

	if cb.state == circuitHalfOpen {
		// Immediately open on failure in half-open state
		cb.state = circuitOpen
		cb.cooldownUntil = time.Now().Add(15 * time.Second) // Reduced cooldown
		cb.successCount = 0
		// Note: Debug logging would require access to logger, which is not available in this context
		// This will be logged at the caller level
	} else if cb.failures >= 3 { // Open after 3 consecutive failures (reduced from 5)
		cb.state = circuitOpen
		cb.cooldownUntil = time.Now().Add(15 * time.Second) // Reduced cooldown
		// Note: Debug logging would require access to logger, which is not available in this context
		// This will be logged at the caller level
	}
}

// Interface guards
var (
	_ caddy.Module          = (*OTLPWriter)(nil)
	_ caddy.Provisioner     = (*OTLPWriter)(nil)
	_ caddy.WriterOpener    = (*OTLPWriter)(nil)
	_ caddy.CleanerUpper    = (*OTLPWriter)(nil)
	_ caddyfile.Unmarshaler = (*OTLPWriter)(nil)
)
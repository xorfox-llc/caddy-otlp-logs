// Package otlplogs provides a Caddy log writer that exports logs via OpenTelemetry Protocol (OTLP).
package otlplogs

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip" // Register gzip compressor
	"google.golang.org/grpc/metadata"
	collectorlogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

var (
	// globalWriters holds singleton instances of OTLP writers
	globalWriters   = make(map[string]*globalOTLPWriter)
	globalWritersMu sync.RWMutex
)

// globalOTLPWriter wraps the actual writer with reference counting
type globalOTLPWriter struct {
	*actualOTLPWriter
	refCount int
}

func init() {
	caddy.RegisterModule(OTLPWriter{})
	
	// Configure global text map propagator with W3C Trace Context and Baggage
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
}

// OTLPWriter implements a Caddy log writer configuration that sends logs via OTLP.
type OTLPWriter struct {
	// Endpoint is the OTLP endpoint URL.
	Endpoint string `json:"endpoint,omitempty"`

	// Protocol specifies the OTLP transport protocol (grpc or http/protobuf).
	Protocol string `json:"protocol,omitempty"`

	// Headers are additional headers to send with each request.
	Headers map[string]string `json:"headers,omitempty"`

	// Timeout for export operations.
	Timeout caddy.Duration `json:"timeout,omitempty"`

	// Insecure disables TLS verification when true.
	Insecure bool `json:"insecure,omitempty"`

	// ServiceName sets the service.name resource attribute.
	ServiceName string `json:"service_name,omitempty"`

	// ResourceAttributes are additional resource attributes.
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
	Debug bool `json:"debug,omitempty"`

	// Compression specifies the compression type (gzip or none).
	Compression string `json:"compression,omitempty"`

	// CACert is the path to CA certificate for TLS verification.
	CACert string `json:"ca_cert,omitempty"`

	// ClientCert is the path to client certificate for mTLS.
	ClientCert string `json:"client_cert,omitempty"`

	// ClientKey is the path to client key for mTLS.
	ClientKey string `json:"client_key,omitempty"`
	
	// SwarmMode configures behavior when running in Docker Swarm mode.
	// Options: "disabled" (default), "replica" (only first replica writes), 
	// "hash" (consistent hash routing), "active" (all replicas write)
	SwarmMode string `json:"swarm_mode,omitempty"`

	key string // computed key for singleton lookup
	
	// swarmInfo holds detected swarm environment information
	swarmInfo *swarmEnvironment
}

// swarmEnvironment contains Docker Swarm environment details
type swarmEnvironment struct {
	NodeID      string
	ServiceName string
	TaskSlot    string
	TaskID      string
	IsSwarmMode bool
}

// actualOTLPWriter is the actual implementation that handles OTLP export
type actualOTLPWriter struct {
	logger       *zap.Logger
	resource     *resourcepb.Resource
	grpcConn     *grpc.ClientConn
	grpcClient   collectorlogspb.LogsServiceClient
	httpClient   *http.Client
	httpEndpoint string
	
	// Configuration (copied from OTLPWriter during provisioning)
	config OTLPWriter
	
	// Simple batching
	mu           sync.Mutex
	logsBatch    []*logspb.LogRecord
	sendCh       chan []*logspb.LogRecord
	stopCh       chan struct{}
	workerDone   chan struct{}
	closed       bool
	
	// Stats
	failedBatches int64
	sentBatches   int64
	
	// Telemetry
	tracer         trace.Tracer
	meter          metric.Meter
	logsExported   metric.Int64Counter
	exportFailures metric.Int64Counter
	exportDuration metric.Float64Histogram
	batchSize      metric.Float64Histogram
	droppedLogs    metric.Int64Counter
	retryAttempts  metric.Int64Counter
}

// Interface guards
var (
	_ caddy.Provisioner     = (*OTLPWriter)(nil)
	_ caddy.WriterOpener    = (*OTLPWriter)(nil)
	_ caddyfile.Unmarshaler = (*OTLPWriter)(nil)
	_ caddy.CleanerUpper    = (*OTLPWriter)(nil)
)

// CaddyModule returns the Caddy module information.
func (OTLPWriter) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.logging.writers.otlp",
		New: func() caddy.Module { return new(OTLPWriter) },
	}
}

// Provision sets up the OTLP writer.
func (w *OTLPWriter) Provision(ctx caddy.Context) error {
	// Apply environment variables if not set
	if w.Endpoint == "" {
		if ep := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"); ep != "" {
			w.Endpoint = ep
		} else if ep := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); ep != "" {
			w.Endpoint = ep
		}
	}

	if w.Protocol == "" {
		if proto := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL"); proto != "" {
			w.Protocol = proto
		} else if proto := os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"); proto != "" {
			w.Protocol = proto
		} else {
			w.Protocol = "grpc"
		}
	}

	// Apply timeout from environment if not set
	if w.Timeout == 0 {
		if timeout := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_TIMEOUT"); timeout != "" {
			if d, err := time.ParseDuration(timeout); err == nil {
				w.Timeout = caddy.Duration(d)
			}
		} else if timeout := os.Getenv("OTEL_EXPORTER_OTLP_TIMEOUT"); timeout != "" {
			if d, err := time.ParseDuration(timeout); err == nil {
				w.Timeout = caddy.Duration(d)
			}
		}
	}
	
	// Set defaults
	if w.Timeout == 0 {
		w.Timeout = caddy.Duration(10 * time.Second)
	}
	if w.ServiceName == "" {
		if svc := os.Getenv("OTEL_SERVICE_NAME"); svc != "" {
			w.ServiceName = svc
		} else {
			w.ServiceName = "caddy"
		}
	}
	if w.BatchSize == 0 {
		w.BatchSize = 100
	}
	if w.BatchTimeout == 0 {
		w.BatchTimeout = caddy.Duration(5 * time.Second)
	}
	if w.MaxRetries == 0 {
		w.MaxRetries = 3
	}
	if w.RetryDelay == 0 {
		w.RetryDelay = caddy.Duration(1 * time.Second)
	}

	// Apply insecure setting from environment if not explicitly set
	// Note: In Caddy config, false is the zero value, so we need to check if it was explicitly set
	if !w.Insecure {
		if insecure := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_INSECURE"); insecure == "true" {
			w.Insecure = true
		} else if insecure := os.Getenv("OTEL_EXPORTER_OTLP_INSECURE"); insecure == "true" {
			w.Insecure = true
		}
	}

	// Apply compression from environment if not set
	if w.Compression == "" {
		if comp := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_COMPRESSION"); comp != "" {
			w.Compression = comp
		} else if comp := os.Getenv("OTEL_EXPORTER_OTLP_COMPRESSION"); comp != "" {
			w.Compression = comp
		}
	}

	// Apply certificate paths from environment if not set
	if w.CACert == "" {
		if cert := os.Getenv("OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE"); cert != "" {
			w.CACert = cert
		} else if cert := os.Getenv("OTEL_EXPORTER_OTLP_CERTIFICATE"); cert != "" {
			w.CACert = cert
		}
	}

	if w.ClientCert == "" {
		if cert := os.Getenv("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE"); cert != "" {
			w.ClientCert = cert
		}
	}

	if w.ClientKey == "" {
		if key := os.Getenv("OTEL_EXPORTER_OTLP_CLIENT_KEY"); key != "" {
			w.ClientKey = key
		}
	}

	// Check for debug environment variable
	if os.Getenv("CADDY_OTLP_DEBUG") == "true" {
		w.Debug = true
	}

	// Parse headers from environment
	if w.Headers == nil {
		w.Headers = make(map[string]string)
	}
	w.parseHeadersFromEnv("OTEL_EXPORTER_OTLP_LOGS_HEADERS")
	w.parseHeadersFromEnv("OTEL_EXPORTER_OTLP_HEADERS")

	// Parse resource attributes from environment
	if w.ResourceAttributes == nil {
		w.ResourceAttributes = make(map[string]string)
	}
	if attrs := os.Getenv("OTEL_RESOURCE_ATTRIBUTES"); attrs != "" {
		pairs := strings.Split(attrs, ",")
		for _, pair := range pairs {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) == 2 {
				w.ResourceAttributes[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
			}
		}
	}

	// Validate protocol
	switch strings.ToLower(w.Protocol) {
	case "grpc", "http/protobuf", "http":
		// Valid protocols
	default:
		return fmt.Errorf("unsupported protocol: %s (supported: grpc, http/protobuf)", w.Protocol)
	}
	
	// Detect Docker Swarm environment
	w.detectSwarmEnvironment()
	
	// Set default swarm mode if not specified
	if w.SwarmMode == "" {
		w.SwarmMode = "disabled"
	}
	
	// Validate swarm mode
	switch strings.ToLower(w.SwarmMode) {
	case "disabled", "replica", "hash", "active":
		// Valid modes
	default:
		return fmt.Errorf("unsupported swarm_mode: %s (supported: disabled, replica, hash, active)", w.SwarmMode)
	}

	// Compute the key for singleton lookup
	w.key = w.WriterKey()

	return nil
}

// OpenWriter returns a write closer that writes to the OTLP endpoint.
func (w *OTLPWriter) OpenWriter() (io.WriteCloser, error) {
	// In replica mode, only slot 1 writes; others get a no-op writer
	if w.swarmInfo != nil && w.swarmInfo.IsSwarmMode && 
		strings.ToLower(w.SwarmMode) == "replica" && w.swarmInfo.TaskSlot != "1" {
		// Important to log when using no-op writer
		fmt.Fprintf(os.Stderr, "[INFO OTLP] Swarm replica mode: slot %s using no-op writer (only slot 1 sends logs)\n", w.swarmInfo.TaskSlot)
		return &noOpWriteCloser{}, nil
	}
	
	globalWritersMu.Lock()
	defer globalWritersMu.Unlock()

	// Check if we already have a writer for this key
	gw, exists := globalWriters[w.key]
	if exists {
		gw.refCount++
		// Log singleton reuse at info level for visibility
		gw.infof("reusing existing OTLP writer: endpoint=%s, refcount=%d", w.Endpoint, gw.refCount)
		return &otlpWriteCloser{actualWriter: gw.actualOTLPWriter, key: w.key}, nil
	}

	// Create a new actual writer
	aw := &actualOTLPWriter{
		config: *w,
	}

	// Initialize the actual writer
	if err := aw.init(); err != nil {
		return nil, fmt.Errorf("failed to initialize OTLP writer: %w", err)
	}

	// Store in global map
	gw = &globalOTLPWriter{
		actualOTLPWriter: aw,
		refCount:         1,
	}
	globalWriters[w.key] = gw

	// Log creation at info level for visibility
	aw.infof("created new OTLP writer: endpoint=%s, protocol=%s, service=%s", w.Endpoint, w.Protocol, w.ServiceName)
	if w.swarmInfo != nil && w.swarmInfo.IsSwarmMode {
		aw.infof("swarm mode enabled: mode=%s, node=%s, slot=%s", w.SwarmMode, w.swarmInfo.NodeID, w.swarmInfo.TaskSlot)
	}

	return &otlpWriteCloser{actualWriter: aw, key: w.key}, nil
}

// Cleanup cleans up resources but doesn't affect the singleton writers.
func (w *OTLPWriter) Cleanup() error {
	// The actual cleanup happens when the last WriteCloser is closed
	return nil
}

// WriterKey returns a unique key for this writer configuration.
func (w *OTLPWriter) WriterKey() string {
	baseKey := fmt.Sprintf("otlp:%s:%s", w.Protocol, w.Endpoint)
	
	// If not in swarm mode or swarm detection not enabled, return base key
	if w.swarmInfo == nil || !w.swarmInfo.IsSwarmMode || w.SwarmMode == "disabled" {
		return baseKey
	}
	
	// Adjust key based on swarm mode strategy
	switch strings.ToLower(w.SwarmMode) {
	case "replica":
		// Only slot 1 gets the base key, others get a readonly key
		if w.swarmInfo.TaskSlot == "1" {
			return baseKey
		}
		return fmt.Sprintf("%s:readonly:slot:%s", baseKey, w.swarmInfo.TaskSlot)
		
	case "hash":
		// All instances use the same key for consistent hashing
		return baseKey
		
	case "active":
		// Each instance gets its own key to maintain separate singletons
		return fmt.Sprintf("%s:node:%s:slot:%s", baseKey, w.swarmInfo.NodeID, w.swarmInfo.TaskSlot)
		
	default:
		return baseKey
	}
}

// detectSwarmEnvironment checks if running in Docker Swarm and populates swarm info
func (w *OTLPWriter) detectSwarmEnvironment() {
	// Check for Docker Swarm environment variables
	nodeID := os.Getenv("DOCKER_NODE_ID")
	serviceName := os.Getenv("DOCKER_SERVICE_NAME")
	taskSlot := os.Getenv("DOCKER_TASK_SLOT")
	taskID := os.Getenv("DOCKER_TASK_ID")
	
	// Only consider it swarm mode if we have the essential variables
	if nodeID != "" && serviceName != "" {
		w.swarmInfo = &swarmEnvironment{
			NodeID:      nodeID,
			ServiceName: serviceName,
			TaskSlot:    taskSlot,
			TaskID:      taskID,
			IsSwarmMode: true,
		}
		
		// Always log swarm detection as it affects behavior
		fmt.Fprintf(os.Stderr, "[INFO OTLP] Detected Docker Swarm environment: node=%s, service=%s, slot=%s, mode=%s\n",
			nodeID, serviceName, taskSlot, w.SwarmMode)
	}
}

// String returns a string representation of the writer.
func (w *OTLPWriter) String() string {
	return fmt.Sprintf("otlp writer to %s via %s", w.Endpoint, w.Protocol)
}

// otlpWriteCloser implements io.WriteCloser for the OTLP writer
type otlpWriteCloser struct {
	actualWriter *actualOTLPWriter
	key          string
}

// Write implements io.Writer
func (owc *otlpWriteCloser) Write(p []byte) (int, error) {
	ctx := context.Background()
	ctx, span := owc.actualWriter.tracer.Start(ctx, "otlp.Write",
		trace.WithAttributes(
			attribute.Int("log.size", len(p)),
		),
	)
	defer span.End()

	// Parse the log entry
	var entry map[string]interface{}
	if err := json.Unmarshal(p, &entry); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to parse log entry")
		owc.actualWriter.warnf("failed to parse log entry: %v", err)
		return len(p), nil // Don't fail, just skip
	}

	// Convert to OTLP log record
	record := &logspb.LogRecord{
		TimeUnixNano: uint64(time.Now().UnixNano()),
		Body:         toAnyValue(entry),
	}

	// Extract known fields
	if level, ok := entry["level"].(string); ok {
		record.SeverityNumber = toSeverityNumber(level)
		record.SeverityText = level
		span.SetAttributes(attribute.String("log.level", level))
	}
	if ts, ok := entry["ts"].(float64); ok {
		record.TimeUnixNano = uint64(ts * 1e9)
	}
	if msg, ok := entry["msg"].(string); ok {
		record.Body = toAnyValue(msg)
		// Store full entry as attributes
		record.Attributes = toAttributes(entry)
	}
	
	// Extract trace context if present
	var hasTraceContext bool
	if traceID, ok := entry["trace_id"].(string); ok {
		if tid := parseTraceID(traceID); tid != nil {
			record.TraceId = tid
			hasTraceContext = true
			span.SetAttributes(attribute.String("log.trace_id", traceID))
		}
	}
	if spanID, ok := entry["span_id"].(string); ok {
		if sid := parseSpanID(spanID); sid != nil {
			record.SpanId = sid
			hasTraceContext = true
			span.SetAttributes(attribute.String("log.span_id", spanID))
		}
	}
	span.SetAttributes(attribute.Bool("log.has_trace_context", hasTraceContext))

	owc.actualWriter.addLog(record)
	span.SetStatus(codes.Ok, "log added to batch")
	return len(p), nil
}

// Close implements io.Closer
func (owc *otlpWriteCloser) Close() error {
	globalWritersMu.Lock()
	defer globalWritersMu.Unlock()

	gw, exists := globalWriters[owc.key]
	if !exists {
		return nil // Already cleaned up
	}

	gw.refCount--
	// Log reference counting at info level for visibility
	gw.infof("closing OTLP writer reference: endpoint=%s, refcount=%d", gw.config.Endpoint, gw.refCount)

	if gw.refCount <= 0 {
		// Last reference, clean up
		delete(globalWriters, owc.key)
		return gw.close()
	}

	return nil
}

// init initializes the actual OTLP writer
func (w *actualOTLPWriter) init() error {
	w.logger = zap.NewNop() // Use a no-op logger for now

	// Initialize telemetry
	w.tracer = otel.Tracer("caddy-otlp-logs", trace.WithInstrumentationVersion("1.0.0"))
	w.meter = otel.Meter("caddy-otlp-logs", metric.WithInstrumentationVersion("1.0.0"))
	
	// Initialize metrics
	var err error
	w.logsExported, err = w.meter.Int64Counter(
		"otlp_logs_exported_total",
		metric.WithDescription("Total number of logs successfully exported"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return fmt.Errorf("failed to create logs_exported metric: %w", err)
	}
	
	w.exportFailures, err = w.meter.Int64Counter(
		"otlp_export_failures_total",
		metric.WithDescription("Total number of failed export attempts"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return fmt.Errorf("failed to create export_failures metric: %w", err)
	}
	
	w.exportDuration, err = w.meter.Float64Histogram(
		"otlp_export_duration_seconds",
		metric.WithDescription("Duration of export operations in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return fmt.Errorf("failed to create export_duration metric: %w", err)
	}
	
	w.batchSize, err = w.meter.Float64Histogram(
		"otlp_batch_size",
		metric.WithDescription("Size of log batches sent"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return fmt.Errorf("failed to create batch_size metric: %w", err)
	}
	
	w.droppedLogs, err = w.meter.Int64Counter(
		"otlp_logs_dropped_total",
		metric.WithDescription("Total number of logs dropped due to full channel"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return fmt.Errorf("failed to create dropped_logs metric: %w", err)
	}
	
	w.retryAttempts, err = w.meter.Int64Counter(
		"otlp_retry_attempts_total",
		metric.WithDescription("Total number of retry attempts"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return fmt.Errorf("failed to create retry_attempts metric: %w", err)
	}

	// Create resource
	attrs := []attribute.KeyValue{
		attribute.String("service.name", w.config.ServiceName),
	}
	for k, v := range w.config.ResourceAttributes {
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

	// Initialize channels
	w.sendCh = make(chan []*logspb.LogRecord, 100)
	w.stopCh = make(chan struct{})
	w.workerDone = make(chan struct{})

	// Initialize client based on protocol
	switch strings.ToLower(w.config.Protocol) {
	case "grpc":
		if err := w.initGRPCClient(); err != nil {
			return fmt.Errorf("failed to initialize gRPC client: %w", err)
		}
	case "http/protobuf", "http":
		if err := w.initHTTPClient(); err != nil {
			return fmt.Errorf("failed to initialize HTTP client: %w", err)
		}
	default:
		return fmt.Errorf("unsupported protocol: %s (supported: grpc, http/protobuf)", w.config.Protocol)
	}

	// Start worker
	go w.worker()

	// Log configuration at info level
	w.infof("OTLP log writer initialized: endpoint=%s, protocol=%s, service=%s, batch_size=%d, batch_timeout=%s",
		w.config.Endpoint, w.config.Protocol, w.config.ServiceName, w.config.BatchSize, w.config.BatchTimeout)

	return nil
}

// close shuts down the actual OTLP writer
func (w *actualOTLPWriter) close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.mu.Unlock()

	// Signal worker to stop
	if w.stopCh != nil {
		close(w.stopCh)
	}
	
	// Wait for worker with timeout
	if w.workerDone != nil {
		select {
		case <-w.workerDone:
		case <-time.After(10 * time.Second):
			w.warnf("worker did not finish in time")
		}
	}

	// Close gRPC connection
	if w.grpcConn != nil {
		if err := w.grpcConn.Close(); err != nil {
			w.warnf("failed to close gRPC connection: %v", err)
		}
	}

	w.infof("OTLP writer closed: sent_batches=%d, failed_batches=%d", 
		atomic.LoadInt64(&w.sentBatches), atomic.LoadInt64(&w.failedBatches))

	return nil
}

// worker processes batches in a single goroutine
func (w *actualOTLPWriter) worker() {
	ticker := time.NewTicker(time.Duration(w.config.BatchTimeout))
	defer ticker.Stop()
	defer close(w.workerDone)

	// Status reporting ticker - report every 5 minutes
	statusTicker := time.NewTicker(5 * time.Minute)
	defer statusTicker.Stop()

	w.infof("batch processor worker started: batch_size=%d, batch_timeout=%s", w.config.BatchSize, w.config.BatchTimeout)

	for {
		select {
		case batch := <-w.sendCh:
			w.sendBatchWithRetry(batch)
			
		case <-ticker.C:
			w.mu.Lock()
			if len(w.logsBatch) > 0 {
				batch := w.logsBatch
				w.logsBatch = nil
				w.mu.Unlock()
				
				select {
				case w.sendCh <- batch:
				default:
					if w.droppedLogs != nil {
						ctx := context.Background()
						w.droppedLogs.Add(ctx, int64(len(batch)),
							metric.WithAttributes(
								attribute.String("reason", "channel_full"),
							),
						)
					}
					w.warnf("send channel full, dropping batch of %d logs", len(batch))
				}
			} else {
				w.mu.Unlock()
			}
			
		case <-statusTicker.C:
			// Report periodic status
			sent := atomic.LoadInt64(&w.sentBatches)
			failed := atomic.LoadInt64(&w.failedBatches)
			w.infof("OTLP writer status: sent_batches=%d, failed_batches=%d, endpoint=%s", 
				sent, failed, w.config.Endpoint)
			
		case <-w.stopCh:
			w.infof("worker received stop signal, performing final flush")
			// Final flush
			w.mu.Lock()
			if len(w.logsBatch) > 0 {
				batch := w.logsBatch
				w.logsBatch = nil
				w.mu.Unlock()
				w.sendBatchWithRetry(batch)
			} else {
				w.mu.Unlock()
			}
			return
		}
	}
}

// addLog adds a log record to the batch
func (w *actualOTLPWriter) addLog(record *logspb.LogRecord) {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.closed {
		return
	}
	
	w.logsBatch = append(w.logsBatch, record)
	
	if w.config.Debug {
		w.debugf("added log to batch: current_batch_size=%d, max_batch_size=%d", 
			len(w.logsBatch), w.config.BatchSize)
	}
	
	if len(w.logsBatch) >= w.config.BatchSize {
		batch := w.logsBatch
		w.logsBatch = nil
		
		select {
		case w.sendCh <- batch:
			if w.config.Debug {
				w.debugf("batch size threshold reached, queued for sending: batch_size=%d", len(batch))
			}
		default:
			w.warnf("send channel full, dropping batch of %d logs", len(batch))
		}
	}
}

// sendBatchWithRetry sends a batch with retry logic
func (w *actualOTLPWriter) sendBatchWithRetry(logs []*logspb.LogRecord) {
	ctx := context.Background()
	ctx, span := w.tracer.Start(ctx, "otlp.sendBatchWithRetry",
		trace.WithAttributes(
			attribute.Int("batch.size", len(logs)),
			attribute.Int("max_retries", w.config.MaxRetries),
		),
	)
	defer span.End()

	var lastErr error
	
	for attempt := 0; attempt <= w.config.MaxRetries; attempt++ {
		if attempt > 0 {
			// Record retry attempt metric
			if w.retryAttempts != nil {
				w.retryAttempts.Add(ctx, 1,
					metric.WithAttributes(
						attribute.Int("attempt", attempt),
					),
				)
			}

			delay := time.Duration(w.config.RetryDelay) * time.Duration(attempt)
			if w.config.Debug {
				w.debugf("retrying batch send after %v (attempt %d/%d)", delay, attempt, w.config.MaxRetries)
			}
			span.AddEvent("retry_delay", trace.WithAttributes(
				attribute.Int("attempt", attempt),
				attribute.String("delay", delay.String()),
			))
			time.Sleep(delay)
		}
		
		ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(w.config.Timeout))
		err := w.sendBatch(ctxTimeout, logs)
		cancel()
		
		if err == nil {
			atomic.AddInt64(&w.sentBatches, 1)
			if w.config.Debug {
				w.debugf("successfully sent batch of %d logs", len(logs))
			}
			span.SetStatus(codes.Ok, "batch sent successfully")
			span.SetAttributes(attribute.Int("attempts", attempt+1))
			return
		}
		
		lastErr = err
		span.RecordError(err, trace.WithAttributes(
			attribute.Int("attempt", attempt+1),
		))
		w.warnf("failed to send batch (attempt %d/%d): %v", attempt+1, w.config.MaxRetries, err)
	}
	
	// All retries failed
	atomic.AddInt64(&w.failedBatches, 1)
	
	// Record export failure metric
	if w.exportFailures != nil {
		w.exportFailures.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("reason", "max_retries_exceeded"),
			),
		)
	}
	
	span.SetStatus(codes.Error, "max retries exceeded")
	span.SetAttributes(attribute.Int("attempts", w.config.MaxRetries+1))
	w.errorf("failed to send batch after %d attempts: %v", w.config.MaxRetries+1, lastErr)
}

// sendBatch sends a batch of logs
func (w *actualOTLPWriter) sendBatch(ctx context.Context, logs []*logspb.LogRecord) error {
	ctx, span := w.tracer.Start(ctx, "otlp.sendBatch",
		trace.WithAttributes(
			attribute.Int("batch.size", len(logs)),
			attribute.String("protocol", w.config.Protocol),
			attribute.String("endpoint", w.config.Endpoint),
		),
	)
	defer span.End()

	// Record batch size metric
	if w.batchSize != nil {
		w.batchSize.Record(ctx, float64(len(logs)))
	}

	// Extract trace context from the first log that has it
	var traceID trace.TraceID
	var spanID trace.SpanID
	var hasTraceContext bool
	for _, log := range logs {
		if len(log.TraceId) == 16 && len(log.SpanId) == 8 {
			copy(traceID[:], log.TraceId)
			copy(spanID[:], log.SpanId)
			hasTraceContext = true
			break
		}
	}
	
	// If we found trace context, create a span context and propagate it
	if hasTraceContext {
		// Create a span context from the trace and span IDs
		spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  spanID,
			Remote:  true,
		})
		
		// Inject the span context into the outgoing context
		ctx = trace.ContextWithSpanContext(ctx, spanCtx)
		span.SetAttributes(attribute.Bool("propagated_context", true))
	}
	
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

	// Record start time for duration metric
	startTime := time.Now()
	
	var err error
	switch strings.ToLower(w.config.Protocol) {
	case "grpc":
		_, err = w.grpcClient.Export(ctx, req)
	case "http/protobuf", "http":
		err = w.sendHTTPRequest(ctx, req)
	default:
		err = fmt.Errorf("unsupported protocol: %s", w.config.Protocol)
	}

	// Record export duration
	duration := time.Since(startTime).Seconds()
	if w.exportDuration != nil {
		w.exportDuration.Record(ctx, duration,
			metric.WithAttributes(
				attribute.String("protocol", w.config.Protocol),
				attribute.Bool("success", err == nil),
			),
		)
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "export failed")
		return err
	}

	// Record successful export
	if w.logsExported != nil {
		w.logsExported.Add(ctx, int64(len(logs)),
			metric.WithAttributes(
				attribute.String("protocol", w.config.Protocol),
			),
		)
	}

	span.SetStatus(codes.Ok, "export successful")
	return nil
}

// Helper methods for logging to stderr
func (w *actualOTLPWriter) debugf(format string, args ...interface{}) {
	if w.config.Debug {
		fmt.Fprintf(os.Stderr, "[DEBUG OTLP] "+format+"\n", args...)
	}
}

func (w *actualOTLPWriter) infof(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[INFO OTLP] "+format+"\n", args...)
}

func (w *actualOTLPWriter) warnf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[WARN OTLP] "+format+"\n", args...)
}

func (w *actualOTLPWriter) errorf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[ERROR OTLP] "+format+"\n", args...)
}

// initGRPCClient initializes the gRPC client
func (w *actualOTLPWriter) initGRPCClient() error {
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(10 * 1024 * 1024)), // 10MB
	}

	if w.config.Insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		// Set up TLS configuration
		tlsConfig := &tls.Config{}
		
		// Load CA certificate if specified
		if w.config.CACert != "" {
			caCert, err := os.ReadFile(w.config.CACert)
			if err != nil {
				return fmt.Errorf("failed to read CA certificate: %w", err)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return fmt.Errorf("failed to parse CA certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}
		
		// Load client certificate and key for mTLS if specified
		if w.config.ClientCert != "" && w.config.ClientKey != "" {
			clientCert, err := tls.LoadX509KeyPair(w.config.ClientCert, w.config.ClientKey)
			if err != nil {
				return fmt.Errorf("failed to load client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{clientCert}
		}
		
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	// Add compression if specified
	if w.config.Compression == "gzip" {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor("gzip")))
	}

	// Add OpenTelemetry instrumentation for trace propagation
	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	
	// Add headers via interceptor if specified
	if len(w.config.Headers) > 0 {
		opts = append(opts, grpc.WithUnaryInterceptor(w.grpcHeaderInterceptor()))
	}

	// Strip scheme from endpoint if present
	endpoint := w.config.Endpoint
	if u, err := url.Parse(endpoint); err == nil && u.Scheme != "" {
		endpoint = u.Host
		if endpoint == "" {
			// Fallback to original endpoint if Host is empty (e.g., for "localhost:4317")
			endpoint = w.config.Endpoint
		}
	}

	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		return fmt.Errorf("failed to dial gRPC endpoint: %w", err)
	}

	w.grpcConn = conn
	w.grpcClient = collectorlogspb.NewLogsServiceClient(conn)
	return nil
}

// initHTTPClient initializes the HTTP client
func (w *actualOTLPWriter) initHTTPClient() error {
	w.httpClient = &http.Client{
		Timeout: time.Duration(w.config.Timeout),
	}

	// Parse endpoint
	u, err := url.Parse(w.config.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to parse endpoint URL: %w", err)
	}

	// Ensure path ends with /v1/logs
	if !strings.HasSuffix(u.Path, "/v1/logs") {
		u.Path = strings.TrimSuffix(u.Path, "/") + "/v1/logs"
	}

	w.httpEndpoint = u.String()
	return nil
}

// grpcHeaderInterceptor creates a gRPC interceptor for headers
func (w *actualOTLPWriter) grpcHeaderInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Add headers to context metadata
		md := metadata.New(w.config.Headers)
		ctx = metadata.NewOutgoingContext(ctx, md)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// sendHTTPRequest sends logs via HTTP/protobuf
func (w *actualOTLPWriter) sendHTTPRequest(ctx context.Context, req *collectorlogspb.ExportLogsServiceRequest) error {
	// Implementation would marshal to protobuf and send via HTTP
	return fmt.Errorf("HTTP/protobuf protocol not yet implemented")
}

// parseHeadersFromEnv parses headers from environment variable
func (w *OTLPWriter) parseHeadersFromEnv(envVar string) {
	if headers := os.Getenv(envVar); headers != "" {
		pairs := strings.Split(headers, ",")
		for _, pair := range pairs {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])
				// Only set if not already set
				if _, exists := w.Headers[key]; !exists {
					w.Headers[key] = value
				}
			}
		}
	}
}

// UnmarshalCaddyfile sets up the OTLP writer from Caddyfile tokens.
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

			case "debug":
				if d.NextArg() {
					val := d.Val()
					if val == "true" {
						w.Debug = true
					} else if val == "false" {
						w.Debug = false
					} else {
						return d.Errf("debug must be 'true' or 'false', got '%s'", val)
					}
				} else {
					w.Debug = true
				}

			case "compression":
				if !d.NextArg() {
					return d.ArgErr()
				}
				w.Compression = d.Val()

			case "ca_cert":
				if !d.NextArg() {
					return d.ArgErr()
				}
				w.CACert = d.Val()

			case "client_cert":
				if !d.NextArg() {
					return d.ArgErr()
				}
				w.ClientCert = d.Val()

			case "client_key":
				if !d.NextArg() {
					return d.ArgErr()
				}
				w.ClientKey = d.Val()

			case "swarm_mode":
				if !d.NextArg() {
					return d.ArgErr()
				}
				w.SwarmMode = d.Val()
				// Validate the value
				switch strings.ToLower(w.SwarmMode) {
				case "disabled", "replica", "hash", "active":
					// Valid modes
				default:
					return d.Errf("invalid swarm_mode: %s (valid options: disabled, replica, hash, active)", w.SwarmMode)
				}

			default:
				return d.Errf("unrecognized subdirective: %s", d.Val())
			}
		}
	}

	return nil
}

// Helper functions for OTLP conversion

func toKeyValue(attr attribute.KeyValue) *commonpb.KeyValue {
	kv := &commonpb.KeyValue{
		Key: string(attr.Key),
	}
	
	switch attr.Value.Type() {
	case attribute.STRING:
		kv.Value = &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{
				StringValue: attr.Value.AsString(),
			},
		}
	case attribute.BOOL:
		kv.Value = &commonpb.AnyValue{
			Value: &commonpb.AnyValue_BoolValue{
				BoolValue: attr.Value.AsBool(),
			},
		}
	case attribute.INT64:
		kv.Value = &commonpb.AnyValue{
			Value: &commonpb.AnyValue_IntValue{
				IntValue: attr.Value.AsInt64(),
			},
		}
	case attribute.FLOAT64:
		kv.Value = &commonpb.AnyValue{
			Value: &commonpb.AnyValue_DoubleValue{
				DoubleValue: attr.Value.AsFloat64(),
			},
		}
	default:
		kv.Value = &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{
				StringValue: attr.Value.AsString(),
			},
		}
	}
	
	return kv
}

func toAnyValue(v interface{}) *commonpb.AnyValue {
	switch val := v.(type) {
	case string:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: val},
		}
	case bool:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_BoolValue{BoolValue: val},
		}
	case int, int32, int64:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_IntValue{IntValue: toInt64(val)},
		}
	case float32, float64:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_DoubleValue{DoubleValue: toFloat64(val)},
		}
	case map[string]interface{}:
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_KvlistValue{
				KvlistValue: toKeyValueList(val),
			},
		}
	default:
		// Fallback to string representation
		return &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: fmt.Sprintf("%v", val)},
		}
	}
}

func toAttributes(m map[string]interface{}) []*commonpb.KeyValue {
	attrs := make([]*commonpb.KeyValue, 0, len(m))
	for k, v := range m {
		attrs = append(attrs, &commonpb.KeyValue{
			Key:   k,
			Value: toAnyValue(v),
		})
	}
	return attrs
}

func toKeyValueList(m map[string]interface{}) *commonpb.KeyValueList {
	return &commonpb.KeyValueList{
		Values: toAttributes(m),
	}
}

func toSeverityNumber(level string) logspb.SeverityNumber {
	switch strings.ToLower(level) {
	case "trace":
		return logspb.SeverityNumber_SEVERITY_NUMBER_TRACE
	case "debug":
		return logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG
	case "info":
		return logspb.SeverityNumber_SEVERITY_NUMBER_INFO
	case "warn", "warning":
		return logspb.SeverityNumber_SEVERITY_NUMBER_WARN
	case "error":
		return logspb.SeverityNumber_SEVERITY_NUMBER_ERROR
	case "fatal":
		return logspb.SeverityNumber_SEVERITY_NUMBER_FATAL
	default:
		return logspb.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED
	}
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float32:
		return float64(val)
	case float64:
		return val
	default:
		return 0
	}
}

// parseTraceID parses a trace ID from string to 16-byte array
// Supports both 32-character hex string and 16-byte binary formats
func parseTraceID(traceID string) []byte {
	// Remove any dashes or spaces
	traceID = strings.ReplaceAll(traceID, "-", "")
	traceID = strings.ReplaceAll(traceID, " ", "")
	
	// Check if it's a valid 32-character hex string (16 bytes)
	if len(traceID) == 32 {
		if b, err := hex.DecodeString(traceID); err == nil && len(b) == 16 {
			return b
		}
	}
	
	return nil
}

// parseSpanID parses a span ID from string to 8-byte array
// Supports both 16-character hex string and 8-byte binary formats
func parseSpanID(spanID string) []byte {
	// Remove any dashes or spaces
	spanID = strings.ReplaceAll(spanID, "-", "")
	spanID = strings.ReplaceAll(spanID, " ", "")
	
	// Check if it's a valid 16-character hex string (8 bytes)
	if len(spanID) == 16 {
		if b, err := hex.DecodeString(spanID); err == nil && len(b) == 8 {
			return b
		}
	}
	
	return nil
}

// noOpWriteCloser is a writer that discards all input (used for non-primary replicas)
type noOpWriteCloser struct{}

func (n *noOpWriteCloser) Write(p []byte) (int, error) {
	// Discard the data but return success
	return len(p), nil
}

func (n *noOpWriteCloser) Close() error {
	// Nothing to close
	return nil
}

// Test helpers - only exposed for testing
type testHelper interface {
	setGRPCClient(client collectorlogspb.LogsServiceClient)
	getActualWriter() *actualOTLPWriter
}

// testWriteCloser wraps otlpWriteCloser to expose test methods
type testWriteCloser struct {
	*otlpWriteCloser
}

func (twc *testWriteCloser) setGRPCClient(client collectorlogspb.LogsServiceClient) {
	if twc.otlpWriteCloser != nil && twc.otlpWriteCloser.actualWriter != nil {
		twc.otlpWriteCloser.actualWriter.grpcClient = client
	}
}

func (twc *testWriteCloser) getActualWriter() *actualOTLPWriter {
	if twc.otlpWriteCloser != nil {
		return twc.otlpWriteCloser.actualWriter
	}
	return nil
}

// openWriterForTest returns a write closer with test capabilities
func (w *OTLPWriter) openWriterForTest() (*testWriteCloser, error) {
	wc, err := w.OpenWriter()
	if err != nil {
		return nil, err
	}
	owc, ok := wc.(*otlpWriteCloser)
	if !ok {
		return nil, fmt.Errorf("unexpected writer type")
	}
	return &testWriteCloser{otlpWriteCloser: owc}, nil
}
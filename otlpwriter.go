// Package otlplogs provides a Caddy log writer that exports logs via OpenTelemetry Protocol (OTLP).
package otlplogs

import (
	"context"
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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	collectorlogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

func init() {
	caddy.RegisterModule(OTLPWriter{})
}

// OTLPWriter implements a Caddy log writer that sends logs via OTLP.
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

	logger       *zap.Logger
	resource     *resourcepb.Resource
	grpcConn     *grpc.ClientConn
	grpcClient   collectorlogspb.LogsServiceClient
	httpClient   *http.Client
	httpEndpoint string
	
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
	w.logger = ctx.Logger(w)

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

	// Set defaults
	if w.Timeout == 0 {
		w.Timeout = caddy.Duration(10 * time.Second)
	}
	if w.ServiceName == "" {
		w.ServiceName = "caddy"
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

	// Initialize channels
	w.sendCh = make(chan []*logspb.LogRecord, 100)
	w.stopCh = make(chan struct{})
	w.workerDone = make(chan struct{})

	// Initialize client based on protocol
	switch strings.ToLower(w.Protocol) {
	case "grpc":
		if err := w.initGRPCClient(); err != nil {
			return fmt.Errorf("failed to initialize gRPC client: %w", err)
		}
	case "http/protobuf", "http":
		if err := w.initHTTPClient(); err != nil {
			return fmt.Errorf("failed to initialize HTTP client: %w", err)
		}
	default:
		return fmt.Errorf("unsupported protocol: %s (supported: grpc, http/protobuf)", w.Protocol)
	}

	// Start worker
	go w.worker()

	if w.Debug {
		w.debugf("OTLP log writer configured: endpoint=%s, protocol=%s, service_name=%s",
			w.Endpoint, w.Protocol, w.ServiceName)
	}

	return nil
}

// worker processes batches in a single goroutine
func (w *OTLPWriter) worker() {
	ticker := time.NewTicker(time.Duration(w.BatchTimeout))
	defer ticker.Stop()
	defer close(w.workerDone)

	if w.Debug {
		w.debugf("batch processor worker started: batch_timeout=%s", w.BatchTimeout)
	}

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
					w.warnf("send channel full, dropping batch of %d logs", len(batch))
				}
			} else {
				w.mu.Unlock()
			}
			
		case <-w.stopCh:
			if w.Debug {
				w.debugf("worker received stop signal")
			}
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
func (w *OTLPWriter) addLog(record *logspb.LogRecord) {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.closed {
		return
	}
	
	w.logsBatch = append(w.logsBatch, record)
	
	if w.Debug {
		w.debugf("added log to batch: current_batch_size=%d, max_batch_size=%d", 
			len(w.logsBatch), w.BatchSize)
	}
	
	if len(w.logsBatch) >= w.BatchSize {
		batch := w.logsBatch
		w.logsBatch = nil
		
		select {
		case w.sendCh <- batch:
			if w.Debug {
				w.debugf("batch size threshold reached, queued for sending: batch_size=%d", len(batch))
			}
		default:
			w.warnf("send channel full, dropping batch of %d logs", len(batch))
		}
	}
}

// sendBatchWithRetry sends a batch with retry logic
func (w *OTLPWriter) sendBatchWithRetry(logs []*logspb.LogRecord) {
	var lastErr error
	
	for attempt := 0; attempt <= w.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(w.RetryDelay) * time.Duration(attempt)
			if w.Debug {
				w.debugf("retrying batch send after %v (attempt %d/%d)", delay, attempt, w.MaxRetries)
			}
			time.Sleep(delay)
		}
		
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(w.Timeout))
		err := w.sendBatch(ctx, logs)
		cancel()
		
		if err == nil {
			atomic.AddInt64(&w.sentBatches, 1)
			if w.Debug {
				w.debugf("successfully sent batch of %d logs", len(logs))
			}
			return
		}
		
		lastErr = err
		w.warnf("failed to send batch (attempt %d/%d): %v", attempt+1, w.MaxRetries, err)
	}
	
	atomic.AddInt64(&w.failedBatches, 1)
	w.errorf("failed to send batch after %d attempts: %v", w.MaxRetries+1, lastErr)
}

// sendBatch sends a batch of logs
func (w *OTLPWriter) sendBatch(ctx context.Context, logs []*logspb.LogRecord) error {
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

	switch strings.ToLower(w.Protocol) {
	case "grpc":
		_, err := w.grpcClient.Export(ctx, req)
		return err
	case "http/protobuf", "http":
		return w.sendHTTPRequest(ctx, req)
	default:
		return fmt.Errorf("unsupported protocol: %s", w.Protocol)
	}
}

// Cleanup shuts down the OTLP exporter
func (w *OTLPWriter) Cleanup() error {
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

// WriterKey returns a unique key for this writer
func (w *OTLPWriter) WriterKey() string {
	return fmt.Sprintf("otlp:%s:%s", w.Protocol, w.Endpoint)
}

// String returns a string representation
func (w *OTLPWriter) String() string {
	return fmt.Sprintf("otlp writer to %s via %s", w.Endpoint, w.Protocol)
}

// OpenWriter returns a new write closer
func (w *OTLPWriter) OpenWriter() (io.WriteCloser, error) {
	return &otlpWriteCloser{writer: w}, nil
}

// otlpWriteCloser implements io.WriteCloser for OTLPWriter
type otlpWriteCloser struct {
	writer *OTLPWriter
}

// Write implements io.Writer
func (owc *otlpWriteCloser) Write(p []byte) (int, error) {
	// Parse the log entry
	var entry map[string]interface{}
	if err := json.Unmarshal(p, &entry); err != nil {
		owc.writer.warnf("failed to parse log entry: %v", err)
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
	}
	if ts, ok := entry["ts"].(float64); ok {
		record.TimeUnixNano = uint64(ts * 1e9)
	}
	if msg, ok := entry["msg"].(string); ok {
		record.Body = toAnyValue(msg)
		// Store full entry as attributes
		record.Attributes = toAttributes(entry)
	}

	owc.writer.addLog(record)
	return len(p), nil
}

// Close implements io.Closer
func (owc *otlpWriteCloser) Close() error {
	// Nothing to do - cleanup happens at module level
	return nil
}

// Helper methods for logging to stderr
func (w *OTLPWriter) debugf(format string, args ...interface{}) {
	if w.Debug {
		fmt.Fprintf(os.Stderr, "[DEBUG OTLP] "+format+"\n", args...)
	}
}

func (w *OTLPWriter) infof(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[INFO OTLP] "+format+"\n", args...)
}

func (w *OTLPWriter) warnf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[WARN OTLP] "+format+"\n", args...)
}

func (w *OTLPWriter) errorf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[ERROR OTLP] "+format+"\n", args...)
}

// initGRPCClient initializes the gRPC client
func (w *OTLPWriter) initGRPCClient() error {
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(10 * 1024 * 1024)), // 10MB
	}

	if w.Insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(nil)))
	}

	// Add headers via interceptor if specified
	if len(w.Headers) > 0 {
		opts = append(opts, grpc.WithUnaryInterceptor(w.grpcHeaderInterceptor()))
	}

	// Strip scheme from endpoint if present
	endpoint := w.Endpoint
	if u, err := url.Parse(endpoint); err == nil && u.Scheme != "" {
		endpoint = u.Host
		if endpoint == "" {
			// Fallback to original endpoint if Host is empty (e.g., for "localhost:4317")
			endpoint = w.Endpoint
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
func (w *OTLPWriter) initHTTPClient() error {
	w.httpClient = &http.Client{
		Timeout: time.Duration(w.Timeout),
	}

	// Parse endpoint
	u, err := url.Parse(w.Endpoint)
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
func (w *OTLPWriter) grpcHeaderInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Add headers to context metadata
		md := metadata.New(w.Headers)
		ctx = metadata.NewOutgoingContext(ctx, md)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// sendHTTPRequest sends logs via HTTP/protobuf
func (w *OTLPWriter) sendHTTPRequest(ctx context.Context, req *collectorlogspb.ExportLogsServiceRequest) error {
	// Implementation would marshal to protobuf and send via HTTP
	return fmt.Errorf("HTTP/protobuf protocol not yet implemented")
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

			default:
				return d.Errf("unrecognized subdirective: %s", d.Val())
			}
		}
	}

	return nil
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
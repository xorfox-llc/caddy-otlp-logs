// Package caddyotlplogs implements a Caddy log writer that exports logs via OTLP.
package caddyotlplogs

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
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
)

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
	w.closeChan = make(chan struct{})

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
				w.ResourceAttributes[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
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

	// Initialize client based on protocol
	switch strings.ToLower(w.Protocol) {
	case "grpc":
		if err := w.initGRPCClient(); err != nil {
			return err
		}
	case "http/protobuf", "http":
		if err := w.initHTTPClient(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported protocol: %s (supported: grpc, http/protobuf)", w.Protocol)
	}

	w.logger.Info("OTLP log writer configured",
		zap.String("endpoint", w.Endpoint),
		zap.String("protocol", w.Protocol),
		zap.String("service_name", w.ServiceName),
	)

	// Start batch processor
	go w.batchProcessor()

	return nil
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
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true
	close(w.closeChan)

	// Send any remaining logs
	if len(w.logsBatch) > 0 {
		if err := w.sendBatch(); err != nil {
			w.reportBatchError(err, len(w.logsBatch), "Cleanup")
		}
	}

	// Clean up connections
	if w.grpcConn != nil {
		return w.grpcConn.Close()
	}

	return nil
}

// initGRPCClient initializes the gRPC client.
func (w *OTLPWriter) initGRPCClient() error {
	endpoint := w.normalizeEndpoint(w.Endpoint, false)
	
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64 * 1024 * 1024)),
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

	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		return fmt.Errorf("failed to create gRPC connection: %w", err)
	}

	w.grpcConn = conn
	w.grpcClient = collectorlogspb.NewLogsServiceClient(conn)
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
	
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: w.Insecure,
		},
	}

	w.httpClient = &http.Client{
		Transport: transport,
		Timeout:   time.Duration(w.Timeout),
	}

	return nil
}

// parseHeadersFromEnv parses headers from an environment variable.
func (w *OTLPWriter) parseHeadersFromEnv(envVar string) {
	if headers := os.Getenv(envVar); headers != "" {
		for _, header := range strings.Split(headers, ",") {
			if kv := strings.SplitN(header, "=", 2); len(kv) == 2 {
				w.Headers[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
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

	if isHTTP {
		// Ensure proper URL format
		if !strings.Contains(endpoint, "://") {
			if w.Insecure {
				endpoint = "http://" + endpoint
			} else {
				endpoint = "https://" + endpoint
			}
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
	ticker := time.NewTicker(time.Duration(w.BatchTimeout))
	defer ticker.Stop()

	for {
		select {
		case <-w.closeChan:
			return
		case <-ticker.C:
			w.mu.Lock()
			if len(w.logsBatch) > 0 {
				logCount := len(w.logsBatch)
				if err := w.sendBatch(); err != nil {
					w.reportBatchError(err, logCount, "batchProcessor timeout")
				}
			}
			w.mu.Unlock()
		}
	}
}

// addLog adds a log record to the batch.
func (w *OTLPWriter) addLog(record *logspb.LogRecord) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.logsBatch = append(w.logsBatch, record)

	if len(w.logsBatch) >= w.BatchSize {
		logCount := len(w.logsBatch)
		if err := w.sendBatch(); err != nil {
			w.reportBatchError(err, logCount, "addLog batch size threshold")
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
		return nil
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
						LogRecords: w.logsBatch,
					},
				},
			},
		},
	}

	// Clear batch
	w.logsBatch = w.logsBatch[:0]

	// Send based on protocol
	if w.grpcClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(w.Timeout))
		defer cancel()
		
		_, err := w.grpcClient.Export(ctx, req)
		if err != nil {
			w.logger.Error("failed to export logs via gRPC", zap.Error(err))
			return err
		}
	} else if w.httpClient != nil {
		data, err := proto.Marshal(req)
		if err != nil {
			w.logger.Error("failed to marshal logs", zap.Error(err))
			return err
		}

		httpReq, err := http.NewRequest("POST", w.httpEndpoint, bytes.NewReader(data))
		if err != nil {
			w.logger.Error("failed to create HTTP request", zap.Error(err))
			return err
		}

		httpReq.Header.Set("Content-Type", "application/x-protobuf")
		for k, v := range w.Headers {
			httpReq.Header.Set(k, v)
		}

		resp, err := w.httpClient.Do(httpReq)
		if err != nil {
			w.logger.Error("failed to export logs via HTTP", zap.Error(err))
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			w.logger.Error("OTLP export failed", 
				zap.Int("status", resp.StatusCode),
				zap.String("body", string(body)))
			return fmt.Errorf("OTLP export failed with status %d", resp.StatusCode)
		}
	}

	return nil
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

// Interface guards
var (
	_ caddy.Module          = (*OTLPWriter)(nil)
	_ caddy.Provisioner     = (*OTLPWriter)(nil)
	_ caddy.WriterOpener    = (*OTLPWriter)(nil)
	_ caddy.CleanerUpper    = (*OTLPWriter)(nil)
	_ caddyfile.Unmarshaler = (*OTLPWriter)(nil)
)
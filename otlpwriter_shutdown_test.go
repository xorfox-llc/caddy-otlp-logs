package otlplogs

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collectorlogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

// TestOTLPWriter_NoPanicOnShutdown verifies that the writer doesn't panic during shutdown
// This test specifically addresses the "send on closed channel" panic issue
func TestOTLPWriter_NoPanicOnShutdown(t *testing.T) {
	tests := []struct {
		name        string
		scenario    func(t *testing.T, w *OTLPWriter, wc *otlpWriteCloser)
		description string
	}{
		{
			name: "shutdown during writes",
			scenario: func(t *testing.T, w *OTLPWriter, wc *otlpWriteCloser) {
				// Start writing logs concurrently
				var wg sync.WaitGroup
				stopWriting := make(chan struct{})
				
				// Writer goroutines
				for i := 0; i < 5; i++ {
					wg.Add(1)
					go func(id int) {
						defer wg.Done()
						ticker := time.NewTicker(1 * time.Millisecond)
						defer ticker.Stop()
						
						for {
							select {
							case <-stopWriting:
								return
							case <-ticker.C:
								log := map[string]interface{}{
									"level": "info",
									"msg":   "concurrent write during shutdown",
									"id":    id,
								}
								data, _ := json.Marshal(log)
								wc.Write(data) // Ignore errors during shutdown
							}
						}
					}(i)
				}
				
				// Let writers run for a bit
				time.Sleep(50 * time.Millisecond)
				
				// Shutdown while writes are happening
				err := w.Cleanup()
				assert.NoError(t, err)
				
				// Stop writers
				close(stopWriting)
				wg.Wait()
			},
			description: "should handle shutdown gracefully while writes are in progress",
		},
		{
			name: "shutdown with full batch",
			scenario: func(t *testing.T, w *OTLPWriter, wc *otlpWriteCloser) {
				// Fill up to just below batch size
				for i := 0; i < w.BatchSize-1; i++ {
					log := map[string]interface{}{
						"level": "info",
						"msg":   "filling batch",
						"seq":   i,
					}
					data, _ := json.Marshal(log)
					_, err := wc.Write(data)
					assert.NoError(t, err)
				}
				
				// Shutdown with pending logs
				err := w.Cleanup()
				assert.NoError(t, err)
			},
			description: "should flush pending logs on shutdown",
		},
		{
			name: "rapid shutdown after provision",
			scenario: func(t *testing.T, w *OTLPWriter, wc *otlpWriteCloser) {
				// Immediately shutdown after getting writer
				err := w.Cleanup()
				assert.NoError(t, err)
			},
			description: "should handle immediate shutdown",
		},
		{
			name: "write after shutdown",
			scenario: func(t *testing.T, w *OTLPWriter, wc *otlpWriteCloser) {
				// Shutdown first
				err := w.Cleanup()
				assert.NoError(t, err)
				
				// Try to write after shutdown
				log := map[string]interface{}{
					"level": "info",
					"msg":   "write after close",
				}
				data, _ := json.Marshal(log)
				_, err = wc.Write(data)
				// Should not panic, just silently drop
				assert.NoError(t, err)
			},
			description: "should not panic when writing after shutdown",
		},
		{
			name: "multiple cleanup calls",
			scenario: func(t *testing.T, w *OTLPWriter, wc *otlpWriteCloser) {
				// First cleanup
				err := w.Cleanup()
				assert.NoError(t, err)
				
				// Second cleanup - should be idempotent
				err = w.Cleanup()
				assert.NoError(t, err)
				
				// Third cleanup for good measure
				err = w.Cleanup()
				assert.NoError(t, err)
			},
			description: "cleanup should be idempotent",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create writer with small batch for faster testing
			w := &OTLPWriter{
				Endpoint:     "localhost:4317",
				Protocol:     "grpc",
				BatchSize:    10,
				BatchTimeout: caddy.Duration(50 * time.Millisecond),
				Insecure:     true,
			}
			
			// Mock client
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
			owc := wc.(*otlpWriteCloser)
			
			// Run the test scenario
			tt.scenario(t, w, owc)
			
			// Verify no panic occurred (test will fail if panic happens)
			t.Logf("✓ %s: %s", tt.name, tt.description)
		})
	}
}

// TestOTLPWriter_StressShutdown performs stress testing of shutdown scenarios
func TestOTLPWriter_StressShutdown(t *testing.T) {
	// Run multiple iterations to catch race conditions
	for i := 0; i < 10; i++ {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			w := &OTLPWriter{
				Endpoint:     "localhost:4317",
				Protocol:     "grpc",
				BatchSize:    5,
				BatchTimeout: caddy.Duration(10 * time.Millisecond),
				Insecure:     true,
				Debug:        false, // Disable debug to reduce output
			}
			
			mockClient := &mockLogsServiceClient{
				responses: make(chan *collectorlogspb.ExportLogsServiceResponse, 1000),
			}
			
			ctx := caddy.Context{
				Context: context.Background(),
			}
			
			require.NoError(t, w.Provision(ctx))
			w.grpcClient = mockClient
			
			wc, err := w.OpenWriter()
			require.NoError(t, err)
			
			// Stress test: many goroutines writing
			var wg sync.WaitGroup
			stopCh := make(chan struct{})
			
			// Start 20 writer goroutines
			for j := 0; j < 20; j++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for k := 0; k < 100; k++ {
						select {
						case <-stopCh:
							return
						default:
							log := map[string]interface{}{
								"level": "info",
								"msg":   "stress test",
								"id":    id,
								"seq":   k,
							}
							data, _ := json.Marshal(log)
							wc.Write(data)
						}
					}
				}(j)
			}
			
			// Let it run briefly
			time.Sleep(5 * time.Millisecond)
			
			// Shutdown in the middle of writes
			close(stopCh)
			err = w.Cleanup()
			assert.NoError(t, err)
			
			// Wait for writers to finish
			wg.Wait()
		})
	}
}
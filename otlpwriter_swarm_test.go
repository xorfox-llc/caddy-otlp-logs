package otlplogs

import (
	"fmt"
	"os"
	"testing"

	"github.com/caddyserver/caddy/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOTLPWriter_SwarmMode(t *testing.T) {
	tests := []struct {
		name            string
		swarmMode       string
		envVars         map[string]string
		expectedKeyFunc func(baseKey string, envVars map[string]string) string
		shouldWrite     bool
	}{
		{
			name:      "disabled mode - no swarm environment",
			swarmMode: "disabled",
			envVars:   map[string]string{},
			expectedKeyFunc: func(baseKey string, _ map[string]string) string {
				return baseKey
			},
			shouldWrite: true,
		},
		{
			name:      "disabled mode - with swarm environment",
			swarmMode: "disabled",
			envVars: map[string]string{
				"DOCKER_NODE_ID":      "node1",
				"DOCKER_SERVICE_NAME": "caddy",
				"DOCKER_TASK_SLOT":    "2",
			},
			expectedKeyFunc: func(baseKey string, _ map[string]string) string {
				return baseKey
			},
			shouldWrite: true,
		},
		{
			name:      "replica mode - primary replica",
			swarmMode: "replica",
			envVars: map[string]string{
				"DOCKER_NODE_ID":      "node1",
				"DOCKER_SERVICE_NAME": "caddy",
				"DOCKER_TASK_SLOT":    "1",
			},
			expectedKeyFunc: func(baseKey string, _ map[string]string) string {
				return baseKey
			},
			shouldWrite: true,
		},
		{
			name:      "replica mode - secondary replica",
			swarmMode: "replica",
			envVars: map[string]string{
				"DOCKER_NODE_ID":      "node1",
				"DOCKER_SERVICE_NAME": "caddy",
				"DOCKER_TASK_SLOT":    "2",
			},
			expectedKeyFunc: func(baseKey string, _ map[string]string) string {
				return fmt.Sprintf("%s:readonly:slot:2", baseKey)
			},
			shouldWrite: false, // Secondary replicas use no-op writer
		},
		{
			name:      "active mode - each instance gets unique key",
			swarmMode: "active",
			envVars: map[string]string{
				"DOCKER_NODE_ID":      "node1",
				"DOCKER_SERVICE_NAME": "caddy",
				"DOCKER_TASK_SLOT":    "2",
			},
			expectedKeyFunc: func(baseKey string, envVars map[string]string) string {
				return fmt.Sprintf("%s:node:%s:slot:%s", baseKey, envVars["DOCKER_NODE_ID"], envVars["DOCKER_TASK_SLOT"])
			},
			shouldWrite: true,
		},
		{
			name:      "hash mode - same key for all instances",
			swarmMode: "hash",
			envVars: map[string]string{
				"DOCKER_NODE_ID":      "node1",
				"DOCKER_SERVICE_NAME": "caddy",
				"DOCKER_TASK_SLOT":    "3",
			},
			expectedKeyFunc: func(baseKey string, _ map[string]string) string {
				return baseKey
			},
			shouldWrite: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up environment variables
			for k, v := range tt.envVars {
				os.Setenv(k, v)
			}
			defer func() {
				for k := range tt.envVars {
					os.Unsetenv(k)
				}
			}()

			// Create and provision writer
			w := &OTLPWriter{
				Endpoint:   "localhost:4317",
				Protocol:   "grpc",
				SwarmMode:  tt.swarmMode,
				BatchSize:  5,
				Debug:      false,
			}

			ctx := caddy.Context{}

			err := w.Provision(ctx)
			require.NoError(t, err)

			// Check the generated key
			baseKey := fmt.Sprintf("otlp:%s:%s", w.Protocol, w.Endpoint)
			expectedKey := tt.expectedKeyFunc(baseKey, tt.envVars)
			assert.Equal(t, expectedKey, w.key, "WriterKey should match expected pattern")

			// Test OpenWriter behavior
			writer, err := w.OpenWriter()
			require.NoError(t, err)
			defer writer.Close()

			// For replica mode with non-primary replicas, verify we get a no-op writer
			if tt.swarmMode == "replica" && tt.envVars["DOCKER_TASK_SLOT"] != "1" {
				_, isNoOp := writer.(*noOpWriteCloser)
				assert.True(t, isNoOp, "Non-primary replicas in replica mode should get no-op writer")
			} else {
				_, isOTLP := writer.(*otlpWriteCloser)
				assert.True(t, isOTLP, "Should get OTLP writer for writing instances")
			}
		})
	}
}

func TestOTLPWriter_SwarmEnvironmentDetection(t *testing.T) {
	tests := []struct {
		name        string
		envVars     map[string]string
		shouldDetect bool
	}{
		{
			name:        "no swarm environment",
			envVars:     map[string]string{},
			shouldDetect: false,
		},
		{
			name: "partial swarm environment - missing service name",
			envVars: map[string]string{
				"DOCKER_NODE_ID": "node1",
			},
			shouldDetect: false,
		},
		{
			name: "full swarm environment",
			envVars: map[string]string{
				"DOCKER_NODE_ID":      "node1",
				"DOCKER_SERVICE_NAME": "caddy",
				"DOCKER_TASK_SLOT":    "1",
				"DOCKER_TASK_ID":      "task123",
			},
			shouldDetect: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up environment variables
			for k, v := range tt.envVars {
				os.Setenv(k, v)
			}
			defer func() {
				for k := range tt.envVars {
					os.Unsetenv(k)
				}
			}()

			w := &OTLPWriter{}
			w.detectSwarmEnvironment()

			if tt.shouldDetect {
				require.NotNil(t, w.swarmInfo)
				assert.True(t, w.swarmInfo.IsSwarmMode)
				assert.Equal(t, tt.envVars["DOCKER_NODE_ID"], w.swarmInfo.NodeID)
				assert.Equal(t, tt.envVars["DOCKER_SERVICE_NAME"], w.swarmInfo.ServiceName)
				assert.Equal(t, tt.envVars["DOCKER_TASK_SLOT"], w.swarmInfo.TaskSlot)
				assert.Equal(t, tt.envVars["DOCKER_TASK_ID"], w.swarmInfo.TaskID)
			} else {
				assert.Nil(t, w.swarmInfo)
			}
		})
	}
}

func TestOTLPWriter_SwarmModeValidation(t *testing.T) {
	tests := []struct {
		name      string
		swarmMode string
		expectErr bool
	}{
		{"valid disabled", "disabled", false},
		{"valid replica", "replica", false},
		{"valid hash", "hash", false},
		{"valid active", "active", false},
		{"invalid mode", "invalid", true},
		{"empty defaults to disabled", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &OTLPWriter{
				Endpoint:  "localhost:4317",
				Protocol:  "grpc",
				SwarmMode: tt.swarmMode,
			}

			ctx := caddy.Context{}

			err := w.Provision(ctx)
			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "unsupported swarm_mode")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
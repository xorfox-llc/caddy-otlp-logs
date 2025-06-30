package otlplogs

import (
	"context"
	"testing"

	"github.com/caddyserver/caddy/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOTLPWriter_Singleton(t *testing.T) {
	// Clear any existing writers
	globalWritersMu.Lock()
	globalWriters = make(map[string]*globalOTLPWriter)
	globalWritersMu.Unlock()

	// Create first writer
	w1 := &OTLPWriter{
		Endpoint: "localhost:4317",
		Protocol: "grpc",
		Debug:    true,
	}

	ctx := caddy.Context{
		Context: context.Background(),
	}

	// Provision the first writer
	require.NoError(t, w1.Provision(ctx))

	// Open first writer
	wc1, err := w1.OpenWriter()
	require.NoError(t, err)
	assert.NotNil(t, wc1)

	// Create second writer with same config
	w2 := &OTLPWriter{
		Endpoint: "localhost:4317",
		Protocol: "grpc",
		Debug:    true,
	}

	// Provision the second writer
	require.NoError(t, w2.Provision(ctx))

	// Open second writer - should reuse the same underlying writer
	wc2, err := w2.OpenWriter()
	require.NoError(t, err)
	assert.NotNil(t, wc2)

	// Check that we have only one global writer
	globalWritersMu.RLock()
	assert.Len(t, globalWriters, 1)
	gw := globalWriters[w1.key]
	assert.Equal(t, 2, gw.refCount)
	globalWritersMu.RUnlock()

	// Close first writer
	err = wc1.Close()
	assert.NoError(t, err)

	// Check refcount decreased
	globalWritersMu.RLock()
	assert.Len(t, globalWriters, 1)
	gw = globalWriters[w1.key]
	assert.Equal(t, 1, gw.refCount)
	globalWritersMu.RUnlock()

	// Close second writer
	err = wc2.Close()
	assert.NoError(t, err)

	// Check writer was removed
	globalWritersMu.RLock()
	assert.Len(t, globalWriters, 0)
	globalWritersMu.RUnlock()
}

func TestOTLPWriter_DifferentConfigs(t *testing.T) {
	// Clear any existing writers
	globalWritersMu.Lock()
	globalWriters = make(map[string]*globalOTLPWriter)
	globalWritersMu.Unlock()

	// Create first writer
	w1 := &OTLPWriter{
		Endpoint: "localhost:4317",
		Protocol: "grpc",
	}

	// Create second writer with different endpoint
	w2 := &OTLPWriter{
		Endpoint: "localhost:4318",
		Protocol: "grpc",
	}

	ctx := caddy.Context{
		Context: context.Background(),
	}

	// Provision both writers
	require.NoError(t, w1.Provision(ctx))
	require.NoError(t, w2.Provision(ctx))

	// Open both writers
	wc1, err := w1.OpenWriter()
	require.NoError(t, err)
	
	wc2, err := w2.OpenWriter()
	require.NoError(t, err)

	// Check that we have two different global writers
	globalWritersMu.RLock()
	assert.Len(t, globalWriters, 2)
	globalWritersMu.RUnlock()

	// Close both
	wc1.Close()
	wc2.Close()

	// Check all cleaned up
	globalWritersMu.RLock()
	assert.Len(t, globalWriters, 0)
	globalWritersMu.RUnlock()
}
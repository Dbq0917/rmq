package rmq

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startBlockedConsumer starts consuming on queue and adds a consumer which
// pulls deliveries but never acks or finishes, so they stay in the unacked list.
func startBlockedConsumer(t *testing.T, queue Queue, prefetch int64) *TestConsumer {
	t.Helper()
	require.NoError(t, queue.StartConsuming(prefetch, time.Millisecond))
	consumer := NewTestConsumer("blocked")
	consumer.AutoAck = false
	consumer.AutoFinish = false
	_, err := queue.AddConsumer("consumer", consumer)
	require.NoError(t, err)
	return consumer
}

// TestCleanerReturnsUnackedAfterHardKill covers the crash fallback. A consumer
// process prefetches deliveries from the ready list into its unacked list and
// then gets killed hard (SIGKILL, OOM, power loss, ...) without ever running the
// graceful shutdown path. The only things left behind in Redis are an expiring
// heartbeat key and the stuck unacked deliveries.
//
// Once the abandoned heartbeat expires the cleaner must move those deliveries
// back to the ready list. At the same time deliveries of a still-live
// connection, whose heartbeat stays fresh, must remain completely untouched.
func TestCleanerReturnsUnackedAfterHardKill(t *testing.T) {
	mr := miniredis.RunT(t)
	options := &redis.Options{Addr: mr.Addr()}
	raw := redis.NewClient(options)

	// --- connection which is going to be hard killed, on its own queue ---
	victimConn, err := OpenConnectionWithRedisOptions("victim", options, nil)
	require.NoError(t, err)
	victimQueue, err := victimConn.OpenQueue("crashed-tasks")
	require.NoError(t, err)

	for _, payload := range []string{"crashed-1", "crashed-2", "crashed-3"} {
		require.NoError(t, victimQueue.Publish(payload))
	}
	eventuallyReady(t, victimQueue, 3)

	victimConsumer := startBlockedConsumer(t, victimQueue, 10)

	// wait until the deliveries are in flight, i.e. stuck in the unacked list
	eventuallyUnacked(t, victimQueue, 3)
	eventuallyReady(t, victimQueue, 0)

	// --- still healthy connection actively processing a different queue ---
	healthyConn, err := OpenConnectionWithRedisOptions("healthy", options, nil)
	require.NoError(t, err)
	healthyQueue, err := healthyConn.OpenQueue("live-tasks")
	require.NoError(t, err)

	for _, payload := range []string{"live-1", "live-2"} {
		require.NoError(t, healthyQueue.Publish(payload))
	}

	healthyConsumer := startBlockedConsumer(t, healthyQueue, 10)

	healthyUnackedKey := healthyQueue.(*redisQueue).unackedKey
	require.Eventually(t, func() bool {
		n, lErr := raw.LLen(t.Context(), healthyUnackedKey).Result()
		return lErr == nil && n == 2
	}, time.Second, 2*time.Millisecond)

	// Simulate the hard kill. A real SIGKILL makes every goroutine of the
	// consumer process disappear at once, leaving behind in Redis only the
	// unacked deliveries and the heartbeat key. We mirror that by terminating
	// the victim's consume and heartbeat goroutines while deliberately NOT
	// touching any Redis keys: no graceful return of the unacked messages and
	// no heartbeat deletion. The abandoned heartbeat then simply reaches its
	// TTL. The healthy connection is never touched and keeps its heartbeat alive.
	victimRC := victimConn.(*redisConnection)
	victimHeartbeatKey := victimRC.heartbeatKey
	require.Equal(t, heartbeatDuration, mr.TTL(victimHeartbeatKey))

	// stop the consumer side: unblock the in-flight Consume() and wait for the
	// consume goroutines to die, without acking anything so the deliveries stay
	// in the unacked list
	victimConsumer.FinishAll()
	<-victimQueue.StopConsuming()

	// stop the heartbeat goroutine without deleting the heartbeat key (the
	// graceful stopHeartbeat() would delete it, which we must avoid)
	heartbeatStopped := make(chan struct{})
	victimRC.heartbeatStop <- heartbeatStopped
	<-heartbeatStopped

	// right after the kill the key still exists but is no longer refreshed ...
	assert.True(t, mr.Exists(victimHeartbeatKey))
	mr.SetTTL(victimHeartbeatKey, 0) // ... then it reaches its TTL

	// victim connection is stale now, healthy connection is still alive
	assert.Equal(t, ErrorNotFound,
		healthyConn.hijackConnection(victimRC.Name).checkHeartbeat())
	assert.NoError(t, healthyConn.checkHeartbeat())

	// --- the cleaner, running in a separate process, reclaims the crash ---
	cleanerConn, err := OpenConnectionWithRedisOptions("cleaner", options, nil)
	require.NoError(t, err)

	// Test-only teardown so background goroutines don't leak beyond the test.
	// Not part of the code path under test, which is the hard kill above.
	t.Cleanup(func() {
		healthyConsumer.FinishAll()
		<-healthyConn.StopAllConsuming()
		_ = cleanerConn.stopHeartbeat()
	})

	returned, err := NewCleaner(cleanerConn).Clean()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), returned)

	// the crashed deliveries are back in the ready list ...
	readyMembers, err := raw.LRange(t.Context(), victimQueue.(*redisQueue).readyKey, 0, -1).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"crashed-1", "crashed-2", "crashed-3"}, readyMembers)

	// ... the victim's unacked list is gone ...
	victimUnacked, err := raw.Exists(t.Context(), victimQueue.(*redisQueue).unackedKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), victimUnacked)

	// ... while the healthy connection's in-flight deliveries stay untouched
	healthyUnacked, err := raw.LLen(t.Context(), healthyUnackedKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), healthyUnacked)

	// the stale connection is unregistered, the healthy one remains registered
	connections, err := cleanerConn.getConnections()
	require.NoError(t, err)
	assert.NotContains(t, connections, victimRC.Name)
	assert.Contains(t, connections, healthyConn.(*redisConnection).Name)
}

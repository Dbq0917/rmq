package rmq

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// publishExpired fills a queue with expired deliveries followed by fresh
// ones. Items are pushed one by one so the expired ones settle at the tail,
// which is the head (oldest end) consumers and the cleaner read from. The
// resulting list head (LPUSH side) -> tail (RPop side) is:
// fresh..., expired...
func publishExpired(t *testing.T, queue Queue, expiredCount, freshCount int) {
	t.Helper()

	frozen := time.Now().Add(-time.Hour)
	previous := nowFunc
	nowFunc = func() time.Time { return frozen }

	for i := 0; i < expiredCount; i++ {
		require.NoError(t, queue.PublishWithTTL(time.Minute, "expired"))
	}
	for i := 0; i < freshCount; i++ {
		require.NoError(t, queue.PublishWithTTL(2*time.Hour, "fresh"))
	}

	nowFunc = previous
}

func TestPublishWithTTLAndConsume(t *testing.T) {
	redisOptions, closer := testRedis(t)
	defer closer()

	flushConn, err := OpenConnectionWithRedisOptions("ttl-flush", redisOptions, nil)
	require.NoError(t, err)
	require.NoError(t, flushConn.stopHeartbeat())
	require.NoError(t, flushConn.flushDb())

	conn, err := OpenConnectionWithRedisOptions("ttl-conn", redisOptions, nil)
	require.NoError(t, err)
	defer conn.stopHeartbeat()

	queue, err := conn.OpenQueue("ttl-q")
	require.NoError(t, err)

	require.NoError(t, queue.PublishWithTTL(time.Minute, "a", "b"))
	// non-positive TTL behaves like the plain Publish, no envelope
	require.NoError(t, queue.PublishWithTTL(0, "plain"))

	require.NoError(t, queue.StartConsuming(3, time.Millisecond))
	consumer := NewTestConsumer("ttl-c")
	_, err = queue.AddConsumer("consumer", consumer)
	require.NoError(t, err)

	// consumers see the original payloads, never the expiration envelope
	assert.Eventually(t, func() bool {
		return len(consumer.Deliveries()) == 3
	}, time.Second, time.Millisecond)

	deliveries := consumer.Deliveries()
	payloads := make([]string, len(deliveries))
	for i, delivery := range deliveries {
		payloads[i] = delivery.Payload()
	}
	assert.ElementsMatch(t, []string{"a", "b", "plain"}, payloads)
	for _, payload := range payloads {
		_, hasExpiration := parseExpiration(payload)
		assert.False(t, hasExpiration)
	}

	queue.StopConsuming()
}

func TestPublishWithTTLPreservesHeader(t *testing.T) {
	redisOptions, closer := testRedis(t)
	defer closer()

	flushConn, err := OpenConnectionWithRedisOptions("ttlh-flush", redisOptions, nil)
	require.NoError(t, err)
	require.NoError(t, flushConn.stopHeartbeat())
	require.NoError(t, flushConn.flushDb())

	conn, err := OpenConnectionWithRedisOptions("ttlh-conn", redisOptions, nil)
	require.NoError(t, err)
	defer conn.stopHeartbeat()

	queue, err := conn.OpenQueue("ttlh-q")
	require.NoError(t, err)

	header := http.Header{}
	header.Set("x-trace-id", "trace-123")
	payload := PayloadWithHeader("body", header)
	require.NoError(t, queue.PublishWithTTL(time.Minute, payload))

	require.NoError(t, queue.StartConsuming(1, time.Millisecond))
	consumer := NewTestConsumer("ttlh-c")
	consumer.AutoAck = false
	consumer.AutoFinish = false
	_, err = queue.AddConsumer("consumer", consumer)
	require.NoError(t, err)

	var delivery Delivery
	assert.Eventually(t, func() bool {
		deliveries := consumer.Deliveries()
		if len(deliveries) == 0 {
			return false
		}
		delivery = deliveries[0]
		return true
	}, time.Second, time.Millisecond)

	assert.Equal(t, "body", delivery.Payload())
	withHeader, ok := delivery.(WithHeader)
	require.True(t, ok)
	assert.Equal(t, "trace-123", withHeader.Header().Get("x-trace-id"))

	// acking still works because the raw envelope is retained internally
	require.NoError(t, delivery.Ack())
	consumer.Finish()
	eventuallyUnacked(t, queue, 0)

	queue.StopConsuming()
}

func TestCleanerCleanExpiredThresholdAndHeadScan(t *testing.T) {
	redisOptions, closer := testRedis(t)
	defer closer()

	flushConn, err := OpenConnectionWithRedisOptions("ce-flush", redisOptions, nil)
	require.NoError(t, err)
	require.NoError(t, flushConn.stopHeartbeat())
	require.NoError(t, flushConn.flushDb())

	conn, err := OpenConnectionWithRedisOptions("ce-conn", redisOptions, nil)
	require.NoError(t, err)
	defer conn.stopHeartbeat()

	cleanerConn, err := OpenConnectionWithRedisOptions("ce-cleaner", redisOptions, nil)
	require.NoError(t, err)
	defer cleanerConn.stopHeartbeat()
	cleaner := NewCleaner(cleanerConn)

	const threshold, max = int64(10), int64(4)

	flush := func(t *testing.T) {
		t.Helper()
		require.NoError(t, flushConn.flushDb())
	}

	t.Run("skips queues below threshold", func(t *testing.T) {
		flush(t)
		q, err := conn.OpenQueue("ce-below")
		require.NoError(t, err)

		publishExpired(t, q, int(threshold)-1, 0)
		eventuallyReady(t, q, threshold-1)

		removed, err := cleaner.CleanExpired(threshold, max)
		require.NoError(t, err)
		assert.Equal(t, int64(0), removed)
		eventuallyReady(t, q, threshold-1)
	})

	t.Run("bounds each pass to max head deliveries", func(t *testing.T) {
		flush(t)
		q, err := conn.OpenQueue("ce-bounded")
		require.NoError(t, err)

		publishExpired(t, q, int(threshold)+int(max), 0)
		eventuallyReady(t, q, threshold+max)

		removed, err := cleaner.CleanExpired(threshold, max)
		require.NoError(t, err)
		assert.Equal(t, max, removed)
		eventuallyReady(t, q, threshold)

		// refill to the threshold; a second pass again removes at most max
		// head deliveries instead of walking the whole list
		publishExpired(t, q, int(max), 0)
		eventuallyReady(t, q, threshold+max)

		removed, err = cleaner.CleanExpired(threshold, max)
		require.NoError(t, err)
		assert.Equal(t, max, removed)
		eventuallyReady(t, q, threshold)
	})

	t.Run("stops at first still valid delivery", func(t *testing.T) {
		flush(t)
		q, err := conn.OpenQueue("ce-stop")
		require.NoError(t, err)

		// list head -> tail: fresh..., expired...
		publishExpired(t, q, 6, 6)
		eventuallyReady(t, q, 12)

		removed, err := cleaner.CleanExpired(threshold, 100)
		require.NoError(t, err)
		assert.Equal(t, int64(6), removed)
		eventuallyReady(t, q, 6)

		// only the fresh deliveries remain and stay consumable
		require.NoError(t, q.StartConsuming(6, time.Millisecond))
		freshConsumer := NewTestConsumer("ce-fresh")
		_, err = q.AddConsumer("fresh-consumer", freshConsumer)
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return len(freshConsumer.Deliveries()) == 6
		}, time.Second, time.Millisecond)

		for _, delivery := range freshConsumer.Deliveries() {
			assert.Equal(t, "fresh", delivery.Payload())
		}

		q.StopConsuming()
	})

	t.Run("falls back to default parameters", func(t *testing.T) {
		flush(t)
		q, err := conn.OpenQueue("ce-defaults")
		require.NoError(t, err)

		publishExpired(t, q, 3, 0)
		eventuallyReady(t, q, 3)

		removed, err := cleaner.CleanExpired(0, 0)
		require.NoError(t, err)
		assert.Equal(t, int64(0), removed) // below default threshold
		eventuallyReady(t, q, 3)
	})
}

func TestCleanerCleanExpiredDoesNotTouchUnacked(t *testing.T) {
	redisOptions, closer := testRedis(t)
	defer closer()

	flushConn, err := OpenConnectionWithRedisOptions("cu-flush", redisOptions, nil)
	require.NoError(t, err)
	require.NoError(t, flushConn.stopHeartbeat())
	require.NoError(t, flushConn.flushDb())

	conn, err := OpenConnectionWithRedisOptions("cu-conn", redisOptions, nil)
	require.NoError(t, err)

	queue, err := conn.OpenQueue("cu-q")
	require.NoError(t, err)

	// long TTL so the prefetched delivery is still valid at cleaning time
	require.NoError(t, queue.PublishWithTTL(2*time.Hour, "in-flight"))
	require.NoError(t, queue.StartConsuming(1, time.Millisecond))

	consumer := NewTestConsumer("cu-c")
	consumer.AutoFinish = false
	consumer.AutoAck = false
	_, err = queue.AddConsumer("consumer", consumer)
	require.NoError(t, err)

	eventuallyUnacked(t, queue, 1)

	// enough expired ready messages to cross the threshold
	publishExpired(t, queue, 12, 0)
	eventuallyReady(t, queue, 12)

	cleanerConn, err := OpenConnectionWithRedisOptions("cu-cleaner", redisOptions, nil)
	require.NoError(t, err)
	defer cleanerConn.stopHeartbeat()

	removed, err := NewCleaner(cleanerConn).CleanExpired(10, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(12), removed)

	// the prefetched delivery stays in the unacked list and can still be acked
	eventuallyReady(t, queue, 0)
	eventuallyUnacked(t, queue, 1)
	require.NotNil(t, consumer.Last())
	assert.Equal(t, "in-flight", consumer.Last().Payload())
	require.NoError(t, consumer.Last().Ack())
	eventuallyUnacked(t, queue, 0)

	queue.StopConsuming()
	require.NoError(t, conn.stopHeartbeat())
}

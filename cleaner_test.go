package rmq

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ttlCountingRedisClient struct {
	RedisClient
	ttlCalls int
}

func (client *ttlCountingRedisClient) TTL(key string) (time.Duration, error) {
	client.ttlCalls++
	return client.RedisClient.TTL(key)
}

func (client *ttlCountingRedisClient) Exists(key string) (int64, error) {
	return client.RedisClient.(heartbeatIndexClient).Exists(key)
}

func (client *ttlCountingRedisClient) ZAdd(key string, member string, score float64) error {
	return client.RedisClient.(heartbeatIndexClient).ZAdd(key, member, score)
}

func (client *ttlCountingRedisClient) ZRange(key string, start int64, stop int64) ([]string, error) {
	return client.RedisClient.(heartbeatIndexClient).ZRange(key, start, stop)
}

func (client *ttlCountingRedisClient) ZRangeByScore(key string, min float64, max float64) ([]string, error) {
	return client.RedisClient.(heartbeatIndexClient).ZRangeByScore(key, min, max)
}

func (client *ttlCountingRedisClient) ZRem(key string, member string) (int64, error) {
	return client.RedisClient.(heartbeatIndexClient).ZRem(key, member)
}

type legacyRedisClient struct {
	RedisClient
	ttlCalls int
}

func (client *legacyRedisClient) TTL(key string) (time.Duration, error) {
	client.ttlCalls++
	return client.RedisClient.TTL(key)
}

func openCleanerBenchmarkConnections(tb testing.TB, redisOptions *redis.Options, count int) []Connection {
	tb.Helper()

	connections := make([]Connection, 0, count)
	for i := 0; i < count; i++ {
		connection, err := OpenConnectionWithRmqRedisClient(
			fmt.Sprintf("cleaner-active-%d", i),
			RedisWrapper{redis.NewClient(redisOptions)},
			nil,
		)
		require.NoError(tb, err)
		connections = append(connections, connection)
	}

	return connections
}

func stopCleanerBenchmarkConnections(tb testing.TB, connections []Connection) {
	tb.Helper()

	for _, connection := range connections {
		require.NoError(tb, connection.stopHeartbeat())
	}
}

func TestCleanerOnlyChecksExpiredHeartbeats(t *testing.T) {
	redisOptions, closer := testRedis(t)
	defer closer()

	flushConn, err := OpenConnectionWithRedisOptions("cleaner-flush", redisOptions, nil)
	require.NoError(t, err)
	require.NoError(t, flushConn.stopHeartbeat())
	require.NoError(t, flushConn.flushDb())

	activeConnections := openCleanerBenchmarkConnections(t, redisOptions, 999)
	defer stopCleanerBenchmarkConnections(t, activeConnections)

	staleName := "cleaner-stale"
	staleConnection, err := OpenConnectionWithRedisOptions(staleName, redisOptions, nil)
	require.NoError(t, err)
	require.NoError(t, staleConnection.stopHeartbeat())

	redisClient := &ttlCountingRedisClient{
		RedisClient: RedisWrapper{redis.NewClient(redisOptions)},
	}
	cleanerConnection, err := OpenConnectionWithRmqRedisClient("cleaner-conn", redisClient, nil)
	require.NoError(t, err)
	defer func() { assert.NoError(t, cleanerConnection.stopHeartbeat()) }()

	returned, err := NewCleaner(cleanerConnection).Clean()
	require.NoError(t, err)
	assert.Equal(t, int64(0), returned)
	assert.Equal(t, 1, redisClient.ttlCalls)

	connections, err := cleanerConnection.getConnections()
	require.NoError(t, err)
	assert.Len(t, connections, 1000)
	assert.False(t, containsConnectionWithPrefix(connections, staleName))

	indexClient := redisClient.RedisClient.(heartbeatIndexClient)
	staleCandidates, err := indexClient.ZRangeByScore(connectionHeartbeatsKey, 0, float64(time.Now().Unix()))
	require.NoError(t, err)
	assert.False(t, containsConnectionWithPrefix(staleCandidates, staleName))
}

func containsConnectionWithPrefix(connectionNames []string, prefix string) bool {
	for _, connectionName := range connectionNames {
		if strings.HasPrefix(connectionName, prefix+"-") {
			return true
		}
	}

	return false
}

func TestCleanerFallsBackWithoutHeartbeatIndex(t *testing.T) {
	redisOptions, closer := testRedis(t)
	defer closer()

	flushConn, err := OpenConnectionWithRedisOptions("cleaner-flush", redisOptions, nil)
	require.NoError(t, err)
	require.NoError(t, flushConn.stopHeartbeat())
	require.NoError(t, flushConn.flushDb())

	legacyClient := &legacyRedisClient{
		RedisClient: RedisWrapper{redis.NewClient(redisOptions)},
	}
	activeConnection, err := OpenConnectionWithRmqRedisClient("cleaner-active", legacyClient, nil)
	require.NoError(t, err)
	defer func() { assert.NoError(t, activeConnection.stopHeartbeat()) }()

	staleConnection, err := OpenConnectionWithRmqRedisClient("cleaner-stale", legacyClient, nil)
	require.NoError(t, err)
	require.NoError(t, staleConnection.stopHeartbeat())

	cleanerConnection, err := OpenConnectionWithRmqRedisClient("cleaner-conn", legacyClient, nil)
	require.NoError(t, err)
	defer func() { assert.NoError(t, cleanerConnection.stopHeartbeat()) }()

	returned, err := NewCleaner(cleanerConnection).Clean()
	require.NoError(t, err)
	assert.Equal(t, int64(0), returned)
	assert.Equal(t, 3, legacyClient.ttlCalls)

	connections, err := cleanerConnection.getConnections()
	require.NoError(t, err)
	assert.Len(t, connections, 2)
}

func TestCleanerFallsBackWhenHeartbeatIndexIsIncomplete(t *testing.T) {
	redisOptions, closer := testRedis(t)
	defer closer()

	flushConn, err := OpenConnectionWithRedisOptions("cleaner-flush", redisOptions, nil)
	require.NoError(t, err)
	require.NoError(t, flushConn.stopHeartbeat())
	require.NoError(t, flushConn.flushDb())

	activeConnections := openCleanerBenchmarkConnections(t, redisOptions, 2)
	defer stopCleanerBenchmarkConnections(t, activeConnections)

	legacyName := "cleaner-legacy"
	legacyConnection, err := OpenConnectionWithRedisOptions(legacyName, redisOptions, nil)
	require.NoError(t, err)
	require.NoError(t, legacyConnection.stopHeartbeat())

	rawClient := redis.NewClient(redisOptions)
	connectionNames, err := rawClient.SMembers(context.TODO(), connectionsKey).Result()
	require.NoError(t, err)

	var actualLegacyName string
	for _, connectionName := range connectionNames {
		if strings.HasPrefix(connectionName, legacyName+"-") {
			actualLegacyName = connectionName
			break
		}
	}
	require.NotEmpty(t, actualLegacyName)
	require.NoError(t, rawClient.ZRem(context.TODO(), connectionHeartbeatsKey, actualLegacyName).Err())

	redisClient := &ttlCountingRedisClient{
		RedisClient: RedisWrapper{rawClient},
	}
	cleanerConnection, err := OpenConnectionWithRmqRedisClient("cleaner-conn", redisClient, nil)
	require.NoError(t, err)
	defer func() { assert.NoError(t, cleanerConnection.stopHeartbeat()) }()

	returned, err := NewCleaner(cleanerConnection).Clean()
	require.NoError(t, err)
	assert.Equal(t, int64(0), returned)
	assert.Equal(t, 4, redisClient.ttlCalls)
}

func BenchmarkCleaner1000Connections(b *testing.B) {
	redisOptions, closer := testRedis(b)
	defer closer()

	flushConn, err := OpenConnectionWithRedisOptions("cleaner-flush", redisOptions, nil)
	require.NoError(b, err)
	require.NoError(b, flushConn.stopHeartbeat())
	require.NoError(b, flushConn.flushDb())

	activeConnections := openCleanerBenchmarkConnections(b, redisOptions, 999)
	defer stopCleanerBenchmarkConnections(b, activeConnections)

	cleanerConn, err := OpenConnectionWithRedisOptions("cleaner-conn", redisOptions, nil)
	require.NoError(b, err)
	defer func() { require.NoError(b, cleanerConn.stopHeartbeat()) }()

	staleConn, err := OpenConnectionWithRedisOptions("cleaner-stale", redisOptions, nil)
	require.NoError(b, err)
	require.NoError(b, staleConn.stopHeartbeat())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = NewCleaner(cleanerConn).Clean()
		require.NoError(b, err)
	}
}

func TestCleaner(t *testing.T) {
	redisOptions, closer := testRedis(t)
	defer closer()

	flushConn, err := OpenConnectionWithRedisOptions("cleaner-flush", redisOptions, nil)
	assert.NoError(t, err)
	assert.NoError(t, flushConn.stopHeartbeat())
	assert.NoError(t, flushConn.flushDb())

	conn, err := OpenConnectionWithRedisOptions("cleaner-conn1", redisOptions, nil)
	assert.NoError(t, err)
	queues, err := conn.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 0)
	queue, err := conn.OpenQueue("q1")
	assert.NoError(t, err)
	queues, err = conn.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 1)
	_, err = conn.OpenQueue("q2")
	assert.NoError(t, err)
	queues, err = conn.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 2)

	eventuallyReady(t, queue, 0)
	assert.NoError(t, queue.Publish("del1"))
	eventuallyReady(t, queue, 1)
	assert.NoError(t, queue.Publish("del2"))
	eventuallyReady(t, queue, 2)
	assert.NoError(t, queue.Publish("del3"))
	eventuallyReady(t, queue, 3)
	assert.NoError(t, queue.Publish("del4"))
	eventuallyReady(t, queue, 4)
	assert.NoError(t, queue.Publish("del5"))
	eventuallyReady(t, queue, 5)
	assert.NoError(t, queue.Publish("del6"))
	eventuallyReady(t, queue, 6)

	eventuallyUnacked(t, queue, 0)
	assert.NoError(t, queue.StartConsuming(2, time.Millisecond))
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 4)

	consumer := NewTestConsumer("c-A")
	consumer.AutoFinish = false
	consumer.AutoAck = false

	_, err = queue.AddConsumer("consumer1", consumer)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 4)

	require.NotNil(t, consumer.Last())
	assert.Equal(t, "del1", consumer.Last().Payload())
	assert.NoError(t, consumer.Last().Ack())
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 3)

	consumer.Finish()
	time.Sleep(10 * time.Millisecond)
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 3)
	assert.Equal(t, "del2", consumer.Last().Payload())

	queue.StopConsuming()
	assert.NoError(t, conn.stopHeartbeat())
	time.Sleep(time.Millisecond)

	conn, err = OpenConnectionWithRedisOptions("cleaner-conn1", redisOptions, nil)
	assert.NoError(t, err)
	queue, err = conn.OpenQueue("q1")
	assert.NoError(t, err)

	assert.NoError(t, queue.Publish("del7"))
	eventuallyReady(t, queue, 4)
	assert.NoError(t, queue.Publish("del8"))
	eventuallyReady(t, queue, 5)
	assert.NoError(t, queue.Publish("del9"))
	eventuallyReady(t, queue, 6)
	assert.NoError(t, queue.Publish("del10"))
	eventuallyReady(t, queue, 7)
	assert.NoError(t, queue.Publish("del11"))
	eventuallyReady(t, queue, 8)

	eventuallyUnacked(t, queue, 0)
	assert.NoError(t, queue.StartConsuming(2, time.Millisecond))
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 6)

	consumer = NewTestConsumer("c-B")
	consumer.AutoFinish = false
	consumer.AutoAck = false

	_, err = queue.AddConsumer("consumer2", consumer)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 6)
	assert.Equal(t, "del4", consumer.Last().Payload())

	consumer.Finish() // unacked
	time.Sleep(10 * time.Millisecond)
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 6)

	assert.Equal(t, "del5", consumer.Last().Payload())
	assert.NoError(t, consumer.Last().Ack())
	time.Sleep(10 * time.Millisecond)
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 5)

	queue.StopConsuming()
	assert.NoError(t, conn.stopHeartbeat())
	time.Sleep(time.Millisecond)

	cleanerConn, err := OpenConnectionWithRedisOptions("cleaner-conn", redisOptions, nil)
	assert.NoError(t, err)
	cleaner := NewCleaner(cleanerConn)
	returned, err := cleaner.Clean()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), returned)
	eventuallyReady(t, queue, 9) // 2 of 11 were acked above
	queues, err = conn.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 2)

	conn, err = OpenConnectionWithRedisOptions("cleaner-conn1", redisOptions, nil)
	assert.NoError(t, err)
	queue, err = conn.OpenQueue("q1")
	assert.NoError(t, err)
	assert.NoError(t, queue.StartConsuming(10, time.Millisecond))
	consumer = NewTestConsumer("c-C")

	_, err = queue.AddConsumer("consumer3", consumer)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	assert.Eventually(t, func() bool {
		return len(consumer.Deliveries()) == 9
	}, 10*time.Second, 2*time.Millisecond)

	queue.StopConsuming()
	assert.NoError(t, conn.stopHeartbeat())
	time.Sleep(time.Millisecond)

	returned, err = cleaner.Clean()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), returned)
	assert.NoError(t, cleanerConn.stopHeartbeat())
}

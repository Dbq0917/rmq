package rmq

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCleanExpiredHeadCluster ensures the head scan works with Redis hash
// tags, where ready list keys use the {queue} notation required for clusters.
func TestCleanExpiredHeadCluster(t *testing.T) {
	redisOptions, closer := testClusterRedis(t)
	defer closer()

	flushConn, err := OpenClusterConnection("cec-flush", redis.NewClusterClient(redisOptions), nil)
	require.NoError(t, err)
	require.NoError(t, flushConn.stopHeartbeat())
	require.NoError(t, flushConn.flushDb())

	conn, err := OpenClusterConnection("cec-conn", redis.NewClusterClient(redisOptions), nil)
	require.NoError(t, err)
	defer conn.stopHeartbeat()

	queue, err := conn.OpenQueue("cec-q")
	require.NoError(t, err)

	publishExpired(t, queue, 8, 4)

	// Verify the queue-level head scan directly: the miniredis cluster double
	// does not replicate the global queue set across nodes, so the cleaner's
	// queue enumeration cannot be exercised here. The single-key Lua script
	// is routed via the {queue} hash tag and behaves like in the non-cluster
	// tests.
	removed, err := queue.(*redisQueue).cleanExpiredHead(time.Now(), 100)
	require.NoError(t, err)
	assert.Equal(t, int64(8), removed)

	count, err := queue.readyCount()
	require.NoError(t, err)
	assert.Equal(t, int64(4), count)
}

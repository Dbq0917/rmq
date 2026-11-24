package rmq

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpirationEnvelope(t *testing.T) {
	expiresAt := time.Unix(1700000000, 123)
	envelope := withExpiration("payload", expiresAt)

	parsed, ok := parseExpiration(envelope)
	assert.True(t, ok)
	assert.True(t, parsed.Equal(expiresAt))
	assert.Equal(t, "payload", stripExpiration(envelope))

	// plain payloads stay untouched and report no expiration
	_, ok = parseExpiration("plain")
	assert.False(t, ok)
	assert.Equal(t, "plain", stripExpiration("plain"))

	// the expiration envelope must not be confused with the header envelope
	headerEnvelope := jsonHeaderSignature + "{}\npayload"
	_, ok = parseExpiration(headerEnvelope)
	assert.False(t, ok)
	assert.Equal(t, headerEnvelope, stripExpiration(headerEnvelope))
}

func newHeadScriptTestQueue(t *testing.T, client RedisClient) *redisQueue {
	t.Helper()
	return &redisQueue{
		readyKey:    "q::ready",
		redisClient: client,
	}
}

func TestCleanExpiredHeadScript(t *testing.T) {
	redisOptions, closer := testRedis(t)
	defer closer()

	clients := map[string]RedisClient{
		"mock":  NewTestRedisClient(),
		"redis": RedisWrapper{redis.NewClient(redisOptions)},
	}

	for name, client := range clients {
		t.Run(name, func(t *testing.T) {
			queue := newHeadScriptTestQueue(t, client)

			now := time.Now()
			old := withExpiration("old", now.Add(-time.Hour))
			alsoOld := withExpiration("also-old", now.Add(-time.Minute))
			fresh := withExpiration("fresh", now.Add(time.Hour))
			plain := "plain-payload"

			// push oldest first so the list head (LPUSH side) -> tail
			// (RPop side) ends up as: plain, fresh, alsoOld, old
			for _, envelope := range []string{old, alsoOld, fresh, plain} {
				_, err := client.LPush(queue.readyKey, envelope)
				require.NoError(t, err)
			}

			count, err := queue.cleanExpiredHead(now, 10)
			require.NoError(t, err)
			assert.Equal(t, int64(2), count)

			remaining, err := queue.readyCount()
			require.NoError(t, err)
			assert.Equal(t, int64(2), remaining)

			// scan stops at the first non-expired delivery; fresh stays at
			// the tail and plain stays at the head, both are kept
			next, err := client.RPop(queue.readyKey)
			require.NoError(t, err)
			assert.Equal(t, fresh, next)

			next, err = client.RPop(queue.readyKey)
			require.NoError(t, err)
			assert.Equal(t, plain, next)
		})
	}
}

func TestCleanExpiredHeadScriptStopsAtLimit(t *testing.T) {
	redisOptions, closer := testRedis(t)
	defer closer()

	clients := map[string]RedisClient{
		"mock":  NewTestRedisClient(),
		"redis": RedisWrapper{redis.NewClient(redisOptions)},
	}

	for name, client := range clients {
		t.Run(name, func(t *testing.T) {
			queue := newHeadScriptTestQueue(t, client)

			now := time.Now()
			envelopes := make([]string, 5)
			for i := range envelopes {
				envelopes[i] = withExpiration("expired", now.Add(-time.Hour))
			}

			for _, envelope := range envelopes {
				_, err := client.LPush(queue.readyKey, envelope)
				require.NoError(t, err)
			}

			count, err := queue.cleanExpiredHead(now, 2)
			require.NoError(t, err)
			assert.Equal(t, int64(2), count)

			remaining, err := queue.readyCount()
			require.NoError(t, err)
			assert.Equal(t, int64(3), remaining)
		})
	}
}

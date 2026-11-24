package rmq

import (
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// fillExpiredBacklog fills readyKey with count expired deliveries in batches.
// All messages are stamped one hour in the past with a one minute TTL.
func fillExpiredBacklog(tb testing.TB, client RedisClient, readyKey string, count int) {
	tb.Helper()

	frozen := time.Now().Add(-time.Hour)
	expiresAt := frozen.Add(time.Minute)

	batch := make([]string, 0, 1000)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		_, err := client.LPush(readyKey, batch...)
		require.NoError(tb, err)
		batch = batch[:0]
	}

	for i := 0; i < count; i++ {
		batch = append(batch, withExpiration("expired", expiresAt))
		if len(batch) == cap(batch) {
			flush()
		}
	}
	flush()
}

// TestExpiredCleaningTiming compares the on-demand head scan against the old
// full scan over a large backlog. The head scan cost is bounded by
// DefaultExpiryCleanMax and independent of the backlog size, while the full
// scan touches every delivery. The 300k scenario from the production case is
// covered by BenchmarkCleanExpiredHead300k below (run it explicitly).
func TestExpiredCleaningTiming(t *testing.T) {
	redisOptions, closer := testRedis(t)
	defer closer()

	const backlog = 20000

	t.Run("head scan", func(t *testing.T) {
		client := RedisWrapper{redis.NewClient(redisOptions)}
		readyKey := "rmq::queue::[timing-head]::ready"
		fillExpiredBacklog(t, client, readyKey, backlog)

		queue := &redisQueue{readyKey: readyKey, redisClient: client}

		started := time.Now()
		removed, err := queue.cleanExpiredHead(time.Now(), DefaultExpiryCleanMax)
		elapsed := time.Since(started)

		require.NoError(t, err)
		require.Equal(t, DefaultExpiryCleanMax, removed)
		t.Logf("head scan removed %d of %d deliveries in %v", removed, backlog, elapsed)
		require.Less(t, elapsed, 100*time.Millisecond)
	})

	t.Run("full scan", func(t *testing.T) {
		client := RedisWrapper{redis.NewClient(redisOptions)}
		readyKey := "rmq::queue::[timing-full]::ready"
		fillExpiredBacklog(t, client, readyKey, backlog)

		now := time.Now()
		started := time.Now()
		var removed int64
		for removed < backlog {
			envelope, err := client.RPop(readyKey)
			if err != nil {
				break
			}
			expiresAt, ok := parseExpiration(envelope)
			if ok && expiresAt.UnixNano() <= now.UnixNano() {
				removed++
			}
		}
		elapsed := time.Since(started)

		require.Equal(t, int64(backlog), removed)
		t.Logf("full scan removed %d deliveries in %v", removed, elapsed)
	})
}

// BenchmarkCleanExpiredHead300k reproduces the production scenario with a
// 300k backlog. It is skipped by default because filling the miniredis
// backlog takes minutes; run it explicitly with:
//
//	go test -run '^$' -bench BenchmarkCleanExpiredHead300k -benchtime 10x
func BenchmarkCleanExpiredHead300k(b *testing.B) {
	if os.Getenv("RMQ_300K_BENCH") == "" {
		b.Skip("set RMQ_300K_BENCH=1 to run the 300k backlog benchmark")
	}

	redisOptions, closer := testRedis(b)
	b.Cleanup(closer)

	client := RedisWrapper{redis.NewClient(redisOptions)}
	readyKey := "rmq::queue::[bench-300k]::ready"
	fillExpiredBacklog(b, client, readyKey, 300000)

	queue := &redisQueue{readyKey: readyKey, redisClient: client}
	now := time.Now()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		removed, err := queue.cleanExpiredHead(now, DefaultExpiryCleanMax)
		if err != nil {
			b.Fatal(err)
		}
		if removed != DefaultExpiryCleanMax {
			b.Fatalf("removed %d, want %d", removed, DefaultExpiryCleanMax)
		}
	}
}

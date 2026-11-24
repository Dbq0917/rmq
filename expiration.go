package rmq

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

// expirySignature marks the head of a delivery carrying an expiration time.
// It is distinct from jsonHeaderSignature so existing payloads and headers
// stay fully backward compatible. Do not change the value.
const expirySignature = "\xFF\x00EXPT"

// DefaultExpiryCleanThreshold is the ready list size above which
// CleanExpired starts scanning a queue. Queues smaller than this are skipped
// without issuing any scan command.
const DefaultExpiryCleanThreshold int64 = 10000

// DefaultExpiryCleanMax is the maximum number of head deliveries inspected
// per queue during one on-demand cleaning pass.
const DefaultExpiryCleanMax int64 = 1000

// errUnsupportedScript is returned by test doubles which don't implement Eval.
var errUnsupportedScript = errors.New("rmq: redis client does not support EVAL")

// nowFunc is the clock used to stamp and compare expiration times. It is a
// variable so tests can deterministically create already-expired deliveries.
var nowFunc = time.Now

// withExpiration wraps a payload so it carries the absolute unix nanosecond
// timestamp at which it expires.
func withExpiration(payload string, expiresAt time.Time) string {
	return expirySignature + strconv.FormatInt(expiresAt.UnixNano(), 10) + "\n" + payload
}

// parseExpiration returns the expiration time carried by an envelope. The
// second return value is false if the delivery does not carry an expiration.
func parseExpiration(envelope string) (expiresAt time.Time, ok bool) {
	if !strings.HasPrefix(envelope, expirySignature) {
		return time.Time{}, false
	}

	lineEnd := strings.Index(envelope, "\n")
	if lineEnd < 0 {
		return time.Time{}, false
	}

	nanos, err := strconv.ParseInt(envelope[len(expirySignature):lineEnd], 10, 64)
	if err != nil {
		return time.Time{}, false
	}

	return time.Unix(0, nanos), true
}

// stripExpiration removes the expiration envelope and returns the original
// payload. Deliveries without an envelope are returned unchanged.
func stripExpiration(envelope string) string {
	if !strings.HasPrefix(envelope, expirySignature) {
		return envelope
	}

	lineEnd := strings.Index(envelope, "\n")
	if lineEnd < 0 {
		return envelope
	}

	return envelope[lineEnd+1:]
}

// cleanExpiredHeadScript atomically removes expired deliveries from the head
// (oldest end) of a ready list.
//
// It inspects at most ARGV[2] deliveries starting at the tail of the list,
// which is the end consumers read from via RPOPLPUSH. The first delivery
// which is not expired stops the scan, so non-expired deliveries and the
// consumers' cursor are never touched. Deliveries without an expiration
// envelope are treated as never expiring and stop the scan as well.
//
// KEYS[1] is the ready list key, ARGV[1] is the current unix nanosecond
// timestamp and ARGV[2] is the maximum number of deliveries to inspect.
// It returns the number of removed deliveries.
const cleanExpiredHeadScript = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local removed = 0

local signature = string.char(255, 0) .. 'EXPT'

while removed < limit do
	local value = redis.call('LINDEX', key, -1)
	if not value then
		break
	end

	if string.sub(value, 1, string.len(signature)) ~= signature then
		break
	end

	local lineEnd = string.find(value, string.char(10), 1, true)
	if not lineEnd then
		break
	end

	local expiresAt = tonumber(string.sub(value, string.len(signature) + 1, lineEnd - 1))
	if not expiresAt or expiresAt > now then
		break
	end

	redis.call('RPOP', key)
	removed = removed + 1
end

return removed
`

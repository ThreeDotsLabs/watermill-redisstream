package redisstream

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tinyPoolClient returns a redis client with PoolSize=1 so the pre-check and
// pool-exhaustion paths can be exercised deterministically. PoolTimeout is set
// large to mimic the misconfigured "hang forever" setup used in production
// where ReadTimeout=-1 / PoolTimeout is effectively unbounded; the test then
// verifies that our new timeouts cap the wait regardless.
func tinyPoolClient(t *testing.T, poolSize int) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:6379",
		PoolSize:    poolSize,
		PoolTimeout: 10 * time.Minute,
	})
	require.NoError(t, c.Ping(context.Background()).Err())
	return c
}

// holdOneConn parks one connection from the pool indefinitely with XRead.
// Used to make the inUse count deterministic in pool-pre-check tests.
// Returns a cancel function that releases the connection.
func holdOneConn(t *testing.T, client redis.UniversalClient) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	go func() {
		close(started)
		_, _ = client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{"hold-stream-" + watermill.NewShortUUID(), "$"},
			Block:   30 * time.Second,
		}).Result()
	}()
	<-started
	// Give XRead a moment to dial and seat itself in the pool.
	time.Sleep(150 * time.Millisecond)
	return cancel
}

func TestSubscribePoolNoFreeSlotsReturnsError(t *testing.T) {
	// PoolSize=1, no slots free for Subscribe: inUse=0, free=1, free<2 → error.
	client := tinyPoolClient(t, 1)
	defer client.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		Client:        client,
		ConsumerGroup: "test-no-free-" + watermill.NewShortUUID(),
	}, nil)
	require.NoError(t, err)
	defer sub.Close()

	_, err = sub.Subscribe(context.Background(), "topic-no-free-"+watermill.NewShortUUID())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "redis pool of size")
	assert.Contains(t, err.Error(), "PoolSize")
}

func TestSubscribePoolEmpiricallyDetectsExternalUsage(t *testing.T) {
	// PoolSize=2: with no external load, Subscribe would succeed (inUse=0, free=2).
	// Once another consumer holds 1 connection, the local Subscriber sees inUse=1,
	// free=1, free<2 → error. This verifies the empirical (cross-instance /
	// cross-consumer) detection.
	client := tinyPoolClient(t, 2)
	defer client.Close()

	releaseHold := holdOneConn(t, client)
	defer releaseHold()

	sub, err := NewSubscriber(SubscriberConfig{
		Client:        client,
		ConsumerGroup: "test-external-" + watermill.NewShortUUID(),
	}, nil)
	require.NoError(t, err)
	defer sub.Close()

	_, err = sub.Subscribe(context.Background(), "topic-external-"+watermill.NewShortUUID())
	require.Error(t, err, "Subscribe should error because external consumer is using one of two pool slots")
	assert.Contains(t, err.Error(), "redis pool of size 2")
}

func TestSubscribePoolLowHeadroomLogsWarning(t *testing.T) {
	// PoolSize=2: Subscribe sees inUse=0, free=2. free>=2 (no error) but free<3 (warn).
	client := tinyPoolClient(t, 2)
	defer client.Close()

	logger := watermill.NewCaptureLogger()
	sub, err := NewSubscriber(SubscriberConfig{
		Client:        client,
		ConsumerGroup: "test-low-headroom-" + watermill.NewShortUUID(),
	}, logger)
	require.NoError(t, err)
	defer sub.Close()

	_, err = sub.Subscribe(context.Background(), "topic-low-headroom-"+watermill.NewShortUUID())
	require.NoError(t, err)

	captured := logger.Captured()[watermill.ErrorLogLevel]
	found := false
	for _, m := range captured {
		if strings.Contains(m.Msg, "no headroom") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected warning log about pool headroom; captured=%+v", captured)
}

func TestLogPoolTimeoutEmitsRichDiagnostic(t *testing.T) {
	// Unit-tests the helper invoked from read() / claim() when a real pool
	// timeout occurs. Verifies the rich message and pool stats are emitted.
	client := tinyPoolClient(t, 10)
	defer client.Close()

	logger := watermill.NewCaptureLogger()
	sub, err := NewSubscriber(SubscriberConfig{
		Client:        client,
		ConsumerGroup: "test-helper-" + watermill.NewShortUUID(),
	}, logger)
	require.NoError(t, err)
	defer sub.Close()

	sub.logPoolTimeout(redis.ErrPoolTimeout, watermill.LogFields{"topic": "test-topic"})

	captured := logger.Captured()[watermill.ErrorLogLevel]
	matched := 0
	for _, m := range captured {
		if strings.Contains(m.Msg, "connection pool timeout") &&
			strings.Contains(m.Msg, "Client.PoolSize") {
			matched++
		}
	}
	assert.Equal(t, 1, matched, "expected exactly one rich pool-timeout log; got %d (captured=%+v)", matched, captured)
}

func TestPublishFailsFastOnPoolExhaustion(t *testing.T) {
	// PoolSize=1 + PoolTimeout=10min simulates the misconfigured setup that
	// causes Publish to hang forever today. The fail-fast check inside
	// Publish should detect the exhausted pool and return immediately,
	// well under PublishTimeout (which is the backup safety net).
	client := tinyPoolClient(t, 1)
	defer client.Close()

	// Saturate the pool by parking the only connection in a long XRead block.
	holdCtx, holdCancel := context.WithCancel(context.Background())
	defer holdCancel()
	go func() {
		_, _ = client.XRead(holdCtx, &redis.XReadArgs{
			Streams: []string{"nonexistent-stream-" + watermill.NewShortUUID(), "$"},
			Block:   5 * time.Second,
		}).Result()
	}()
	// Give the blocker a moment to acquire the only connection.
	time.Sleep(100 * time.Millisecond)

	publisher, err := NewPublisher(PublisherConfig{
		Client:         client,
		PublishTimeout: 500 * time.Millisecond,
	}, nil)
	require.NoError(t, err)
	defer publisher.Close()

	msg := message.NewMessage(watermill.NewUUID(), []byte("test"))
	start := time.Now()
	err = publisher.Publish("topic-fail-fast-"+watermill.NewShortUUID(), msg)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "pool of size 1 is exhausted")
	// Fail-fast should return far below PublishTimeout. Allow 100ms slack
	// for goroutine scheduling on a loaded CI box.
	assert.Less(t, elapsed, 100*time.Millisecond,
		"Publish should fail fast on pool exhaustion, took %s", elapsed)
}

func TestNoPublishTimeoutAllowsPublish(t *testing.T) {
	// With NoPublishTimeout and a healthy pool, Publish should succeed.
	// (NoPublishTimeout disables the per-XAdd library deadline; fail-fast
	// still runs but does not fire with an unsaturated pool.)
	client := tinyPoolClient(t, 10)
	defer client.Close()

	publisher, err := NewPublisher(PublisherConfig{
		Client:         client,
		PublishTimeout: NoPublishTimeout,
	}, nil)
	require.NoError(t, err)
	defer publisher.Close()

	msg := message.NewMessage(watermill.NewUUID(), []byte("test"))
	require.NoError(t, publisher.Publish("topic-no-timeout-"+watermill.NewShortUUID(), msg))
}

package redisstream

import (
	"github.com/redis/go-redis/v9"
)

// effectivePoolSize returns the effective concurrent-connection cap for the
// given client, and whether it could be determined.
//
// The go-redis pool uses a counting semaphore sized to PoolSize to gate
// concurrent Get() calls. When MaxActiveConns is set and is smaller than
// PoolSize, dialing new connections fails before the semaphore would block,
// so the effective cap is min(PoolSize, MaxActiveConns).
//
// For ClusterClient/Ring the returned value is the per-node cap; a single
// subscriber's blocked XRead lands on one node, so this is the correct
// denominator for the worst-case pre-check.
func effectivePoolSize(c redis.UniversalClient) (int, bool) {
	var poolSize, maxActive int
	switch v := c.(type) {
	case *redis.Client:
		opts := v.Options()
		poolSize, maxActive = opts.PoolSize, opts.MaxActiveConns
	case *redis.ClusterClient:
		opts := v.Options()
		poolSize, maxActive = opts.PoolSize, opts.MaxActiveConns
	case *redis.Ring:
		opts := v.Options()
		poolSize, maxActive = opts.PoolSize, opts.MaxActiveConns
	default:
		// Sentinel users get a *redis.Client via NewFailoverClient and are
		// covered by the *redis.Client case. Unknown UniversalClient
		// implementations (mocks, third-party) skip the pre-check; runtime
		// polling of PoolStats() still works for them.
		return 0, false
	}
	if maxActive > 0 && maxActive < poolSize {
		return maxActive, true
	}
	return poolSize, true
}

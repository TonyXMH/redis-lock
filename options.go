package redis_lock

import (
	"time"
)

type Options struct {
	RetryStrategy RetryStrategy
	Metadata      string
}

func (o *Options) getMetadata() string {
	if o != nil {
		return o.Metadata
	}
	return ""
}
func (o *Options) getRetryStrategy() RetryStrategy {
	if o != nil && o.RetryStrategy != nil {
		return o.RetryStrategy
	}
	return NoRetry()
}

type RetryStrategy interface {
	NextBackoff() time.Duration
}

type linearBackoff time.Duration

func (l linearBackoff) NextBackoff() time.Duration {
	return time.Duration(l)
}
func LinearBackoff(backoff time.Duration) RetryStrategy {
	return linearBackoff(0)
}
func NoRetry() RetryStrategy {
	return linearBackoff(0)
}

type limitedRetry struct {
	s   RetryStrategy
	cnt int
	max int
}

func LimitRetry(s RetryStrategy, max int) RetryStrategy {
	return &limitedRetry{s: s, max: max}
}

func (r *limitedRetry) NextBackoff() time.Duration {
	if r.cnt > r.max {
		return 0
	}
	r.cnt++
	return r.s.NextBackoff()
}

type exponentialBackoff struct {
	cnt uint
	min time.Duration
	max time.Duration
}

func ExponentialBackoff(min, max time.Duration) RetryStrategy {
	return &exponentialBackoff{min: min, max: max}
}

func (r *exponentialBackoff) NextBackoff() time.Duration {
	r.cnt++
	ms := 2 << 25
	if r.cnt < 25 {
		ms = 2 << r.cnt
	}
	if d := time.Duration(ms) * time.Millisecond; d < r.min {
		return r.min
	} else if r.max != 0 && d > r.max {
		return r.max
	} else {
		return d
	}
}

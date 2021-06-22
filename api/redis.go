package api

import (
	"errors"
	"net/url"

	"github.com/go-redis/redis/v8"
	"github.com/gopherd/doge/query"
)

// Options represents redis options
type Options struct {
	redis.Options

	Prefix string
}

// ParseSource parses options source string. Formats of source:
//
//	[network://]host:port?k1=v1&k2=v2&...&kn=vn
//
// network should be tcp(default) or unix.
//
// e.g.
//	127.0.0.1:26379
//	tcp://127.0.0.1:26379
//	tcp://127.0.0.1:26379?db=1&username=foo&password=123456
func ParseSource(options *Options, source string) error {
	u, err := query.ParseURL(source, "tcp")
	if err != nil {
		return err
	}
	switch u.Scheme {
	case "tcp", "unix":
	default:
		return errors.New("unknown redis network " + u.Scheme)
	}
	options.Network = u.Scheme
	options.Addr = u.Host
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return err
	}
	return query.New(query.Query(q)).
		String(&options.Username, "username", "").
		String(&options.Password, "password", "").
		Int(&options.DB, "db", 0).
		Int(&options.MaxRetries, "max_retries", 0).
		Duration(&options.MaxRetryBackoff, "max_retry_backoff", 0).
		Duration(&options.DialTimeout, "dial_timeout", 0).
		Duration(&options.ReadTimeout, "read_timeout", 0).
		Duration(&options.WriteTimeout, "write_timeout", 0).
		Int(&options.PoolSize, "pool_size", 0).
		Int(&options.MinIdleConns, "min_idle_conns", 0).
		Duration(&options.MaxConnAge, "max_conn_age", 0).
		Duration(&options.PoolTimeout, "pool_timeout", 0).
		Duration(&options.IdleTimeout, "idle_timeout", 0).
		Duration(&options.IdleCheckFrequency, "idle_check_frequency", 0).
		String(&options.Prefix, "prefix", "").
		Err()
}

// NewClient returns a redis client by options source string
func NewClient(source string) (*redis.Client, *Options, error) {
	var options Options
	if err := ParseSource(&options, source); err != nil {
		return nil, nil, err
	}
	return redis.NewClient(&options.Options), &options, nil
}

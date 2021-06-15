package api

import (
	"errors"
	"net/url"

	"github.com/go-redis/redis/v8"
	"github.com/gopherd/doge/text/query"
)

// Maybe rawurl is of the form scheme:path.
// (Scheme must be [a-zA-Z][a-zA-Z0-9+-.]*)
// If so, return scheme, path; else return "", rawurl.
func getscheme(rawurl string) (scheme, path string, err error) {
	for i := 0; i < len(rawurl); i++ {
		c := rawurl[i]
		switch {
		case 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z':
		// do nothing
		case '0' <= c && c <= '9' || c == '+' || c == '-' || c == '.':
			if i == 0 {
				return "", rawurl, nil
			}
		case c == ':':
			if i == 0 {
				return "", "", errors.New("missing protocol scheme")
			}
			return rawurl[:i], rawurl[i+1:], nil
		default:
			// we have encountered an invalid character,
			// so there is no valid scheme
			return "", rawurl, nil
		}
	}
	return "", rawurl, nil
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
func ParseSource(options *redis.Options, source string) error {
	scheme, rest, err := getscheme(source)
	if err != nil || len(scheme) == 0 || len(rest) < 2 || rest[0] != '/' || rest[1] != '/' {
		source = "tcp://" + source
	}
	u, err := url.Parse(source)
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
	return query.NewParser(query.Query(q)).
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
		Err()
}

// NewClient returns a redis client by options source string
func NewClient(source string) (*redis.Client, error) {
	var options redis.Options
	if err := ParseSource(&options, source); err != nil {
		return nil, err
	}
	return redis.NewClient(&options), nil
}

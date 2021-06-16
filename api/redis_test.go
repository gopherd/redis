package api_test

import (
	"testing"

	. "github.com/gopherd/redis/api"
)

func TestParseOptions(t *testing.T) {
	for i, tc := range []struct {
		source   string
		network  string
		address  string
		db       int
		username string
		password string
		err      bool
	}{
		{"127.0.0.1:26379", "tcp", "127.0.0.1:26379", 0, "", "", false},
		{"tcp://127.0.0.1:26379", "tcp", "127.0.0.1:26379", 0, "", "", false},
		{"unix://redis.sock?db=1&username=foo&password=123456", "unix", "redis.sock", 1, "foo", "123456", false},
		{"127.0.0.1:26379?pool_size=NaN", "tcp", "127.0.0.1:26379", 0, "", "", true},
		{"invalid://xxx", "", "", 0, "", "", true},
	} {
		var options Options
		if err := ParseSource(&options, tc.source); err != nil {
			if !tc.err {
				t.Errorf("%dth: ParseSource %s error: %v", i, tc.source, err)
			} else {
				t.Logf("expected parsing error: %v", err)
			}
		} else if tc.err {
			t.Errorf("%dth: ParseSource %s want error", i, tc.source)
		} else {
			if tc.network != options.Network {
				t.Errorf("%dth: network expect %q, but got %q", i, tc.network, options.Network)
			} else if tc.address != options.Addr {
				t.Errorf("%dth: address expect %q, but got %q", i, tc.address, options.Addr)
			} else if tc.db != options.DB {
				t.Errorf("%d: db expect %d, but got %d", i, tc.db, options.DB)
			} else if tc.username != options.Username {
				t.Errorf("%d: db username %q, but got %q", i, tc.username, options.Username)
			} else if tc.password != options.Password {
				t.Errorf("%d: db password %q, but got %q", i, tc.password, options.Password)
			}
		}
	}
}

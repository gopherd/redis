package discovery

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gopherd/doge/service/discovery"

	"github.com/gopherd/redis/api"
)

func init() {
	discovery.Register("redis", new(driver))
}

type driver struct {
}

// Open implements discovery.Driver Open method
func (d driver) Open(source string) (discovery.Discovery, error) {
	client, options, err := api.NewClient(source)
	if err != nil {
		return nil, err
	}
	options.Prefix += "discovery.registry."
	return &discoveryImpl{
		options: options,
		client:  client,
	}, nil
}

type discoveryImpl struct {
	options *api.Options
	client  *redis.Client
}

func (d *discoveryImpl) key(name string) string {
	return d.options.Prefix + name
}

// Register registers a service. Argument ttl only be used name is empty
func (d *discoveryImpl) Register(ctx context.Context, name, id string, content string, nx bool, ttl time.Duration) error {
	if nx {
		var (
			ok  bool
			err error
		)
		if name == "" {
			ok, err = d.client.SetNX(ctx, d.key(name), content, ttl).Result()
		} else {
			ok, err = d.client.HSetNX(ctx, d.key(name), id, content).Result()
		}
		if err != nil {
			return err
		}
		if !ok {
			return discovery.ErrExist
		}
		return nil
	}
	return d.client.HSet(ctx, d.key(name), id, content).Err()
}

// Unregister unregisters a service
func (d *discoveryImpl) Unregister(ctx context.Context, name, id string) error {
	if name == "" {
		return d.client.Del(ctx, d.key(id)).Err()
	}
	return d.client.HDel(ctx, d.key(name), id).Err()
}

// Finds finds service content
func (d *discoveryImpl) Find(ctx context.Context, name, id string) (string, error) {
	if name == "" {
		return d.client.Get(ctx, d.key(id)).Result()
	}
	return d.client.HGet(ctx, d.key(name), id).Result()
}

// Resolve resolves any one service by name
func (d *discoveryImpl) Resolve(ctx context.Context, name string) (id, content string, err error) {
	if name == "" {
		return "", "", errors.New("empty resolved name")
	}
	result, err := d.client.HGetAll(ctx, d.key(name)).Result()
	if err != nil {
		return "", "", err
	}
	for k, v := range result {
		return k, v, nil
	}
	return "", "", nil
}

// Resolve resolves all services by name
func (d *discoveryImpl) ResolveAll(ctx context.Context, name string) (map[string]string, error) {
	if name == "" {
		return nil, errors.New("empty resolved name")
	}
	return d.client.HGetAll(ctx, d.key(name)).Result()
}

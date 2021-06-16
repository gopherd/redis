package discovery

import (
	"context"

	"github.com/go-redis/redis/v8"

	"github.com/gopherd/doge/service/discovery"
	redisapi "github.com/gopherd/redis/api"
)

func init() {
	discovery.Register("redis", new(driver))
}

type driver struct {
}

// Open implements discovery.Driver Open method
func (d driver) Open(source string) (discovery.Discovery, error) {
	client, options, err := redisapi.NewClient(source)
	if err != nil {
		return nil, err
	}
	options.Prefix += "service.discovery."
	return &discoveryImpl{
		options: options,
		client:  client,
	}, nil
}

type discoveryImpl struct {
	options *redisapi.Options
	client  *redis.Client
}

func (d *discoveryImpl) key(serviceName string) string {
	return d.options.Prefix + serviceName
}

// Register registers a service
func (d *discoveryImpl) Register(ctx context.Context, serviceName, serviceId string, content string) error {
	return d.client.HSet(ctx, d.key(serviceName), serviceId, content).Err()
}

// Unregister unregisters a service
func (d *discoveryImpl) Unregister(ctx context.Context, serviceName, serviceId string) error {
	return d.client.HDel(ctx, d.key(serviceName), serviceId).Err()
}

// Resolve resolves any one service by name
func (d *discoveryImpl) Resolve(ctx context.Context, serviceName string) (serviceId, content string, err error) {
	result, err := d.client.HGetAll(ctx, d.key(serviceName)).Result()
	if err != nil {
		return "", "", err
	}
	for k, v := range result {
		return k, v, nil
	}
	return "", "", nil
}

// Resolve resolves all services by name
func (d *discoveryImpl) ResolveAll(ctx context.Context, serviceName string) (map[string]string, error) {
	return d.client.HGetAll(ctx, d.key(serviceName)).Result()
}

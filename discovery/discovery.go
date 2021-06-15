package discovery

import (
	"github.com/go-redis/redis/v8"

	"github.com/gopherd/doge/service/discovery"
	redisapi "github.com/gopherd/redis/api"
)

func init() {
	discovery.Register("redis", new(driver))
}

type driver struct {
}

func (d driver) Open(source string) (discovery.Discovery, error) {
	client, err := redisapi.NewClient(source)
	if err != nil {
		return nil, err
	}
	return &discoveryImpl{
		client: client,
	}, nil
}

type discoveryImpl struct {
	client *redis.Client
}

// Register registers a service
func (d *discoveryImpl) Register(serviceName, serviceId string, content []byte) error {
	return nil
}

// Unregister unregisters a service
func (d *discoveryImpl) Unregister(serviceName, serviceId string) error {
	return nil
}

// Resolve resolves any one service by name
func (d *discoveryImpl) Resolve(serviceName string) ([]byte, error) {
	return nil, nil
}

// Resolve resolves all services by name
func (d *discoveryImpl) ResolveAll(serviceName string) (map[string][]byte, error) {
	return nil, nil
}

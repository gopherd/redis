package zmq

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/gopherd/doge/mq"
	"github.com/gopherd/doge/service/discovery"
	"github.com/gopherd/redis/api"
)

func init() {
	mq.Register("redismq", new(driver))
}

type driver struct {
}

// source format:
//
// [tcp://]host:port
//
func (d driver) Open(source string, discovery discovery.Discovery) (mq.Conn, error) {
	client, options, err := api.NewClient(source)
	if err != nil {
		return nil, err
	}
	return newConn(client, discovery, options), nil
}

func getChannel(prefix, topic string) string {
	if prefix == "" {
		return topic
	}
	return path.Join(prefix, topic)
}

// conn is the top-level zmq connection
type conn struct {
	options   *api.Options
	redisc    *redis.Client
	discovery discovery.Discovery

	pushersMu sync.RWMutex
	pushers   map[string]*pusher

	pullersMu sync.Mutex
	pullers   map[string]*puller
}

func newConn(client *redis.Client, discovery discovery.Discovery, options *api.Options) *conn {
	return &conn{
		options:   options,
		redisc:    client,
		discovery: discovery,
		pullers:   make(map[string]*puller),
		pushers:   make(map[string]*pusher),
	}
}

// Close closes the conn
func (c *conn) Close() error {
	c.pullersMu.Lock()
	for topic, puller := range c.pullers {
		puller.shutdown()
		delete(c.pullers, topic)
	}
	c.pullersMu.Unlock()

	c.pushersMu.Lock()
	for topic, pusher := range c.pushers {
		pusher.shutdown()
		delete(c.pushers, topic)
	}
	c.pushersMu.Unlock()

	return nil
}

// Ping implements mq.Conn Ping method
func (c *conn) Ping(topic string) error {
	_, err := c.getPusher(topic)
	return err
}

// Subscribe implements mq.Conn Subscribe method
func (c *conn) Subscribe(topic string, consumer mq.Consumer) error {
	c.pullersMu.Lock()
	defer c.pullersMu.Unlock()

	if _, ok := c.pullers[topic]; ok {
		return nil
	}
	channel := getChannel(c.options.Prefix, topic)
	p, err := newPuller(c.redisc.Subscribe(context.TODO(), channel), c.options, topic, consumer)
	if err != nil {
		return err
	}
	if err := c.discovery.Register(context.TODO(), "redismq/"+topic, "0", c.options.Addr, false); err != nil {
		return err
	}
	go p.start()
	c.pullers[topic] = p
	return nil
}

// Publish implements mq.Conn Publish method
func (c *conn) Publish(topic string, content []byte) error {
	p, err := c.getPusher(topic)
	if err != nil {
		return err
	}
	return p.publish(content)
}

func (c *conn) getPusher(topic string) (*pusher, error) {
	c.pushersMu.RLock()
	p, ok := c.pushers[topic]
	if ok {
		c.pushersMu.RUnlock()
		return p, nil
	}
	content, err := c.discovery.Find(context.TODO(), "redismq/"+topic, "0")
	if err != nil {
		return nil, err
	}
	var (
		client  = c.redisc
		options = c.options
		clone   = true
	)
	if content != c.options.Addr {
		clone = false
		client, options, err = api.NewClient(content)
		if err != nil {
			return nil, err
		}
	}
	p = newPusher(getChannel(options.Prefix, topic), client, clone)
	if err == nil {
		c.pushersMu.Lock()
		defer c.pushersMu.Unlock()
		if p2, ok := c.pushers[topic]; ok {
			p.shutdown()
			return p2, nil
		}
		c.pushers[topic] = p
	}
	return p, err
}

// pusher used publish messages
type pusher struct {
	channel string
	redisc  *redis.Client
	clone   bool
}

func newPusher(channel string, client *redis.Client, clone bool) *pusher {
	return &pusher{
		channel: channel,
		redisc:  client,
		clone:   clone,
	}
}

func (p *pusher) publish(data []byte) error {
	return p.redisc.Publish(context.TODO(), p.channel, data).Err()
}

func (p *pusher) shutdown() {
	if !p.clone {
		p.redisc.Close()
	}
}

// puller used to receive messages from specified topic
type puller struct {
	quit     chan struct{}
	topic    string
	options  *api.Options
	sub      *redis.PubSub
	consumer mq.Consumer
	claim    *claim
}

func newPuller(sub *redis.PubSub, options *api.Options, topic string, consumer mq.Consumer) (*puller, error) {
	p := &puller{
		quit:     make(chan struct{}),
		topic:    topic,
		sub:      sub,
		options:  options,
		consumer: consumer,
		claim:    newClaim(),
	}
	res, err := sub.Receive(context.TODO())
	if err != nil {
		return nil, err
	}
	switch res.(type) {
	case *redis.Subscription:
		// subscribe succeeded
	default:
		return nil, fmt.Errorf("unexpected subscribe response: %v", res)
	}
	if err := p.consumer.Setup(); err != nil {
		return nil, err
	}
	p.sub = sub
	return p, nil
}

func (p *puller) start() error {
	ch := p.sub.Channel()
FOR:
	for {
		select {
		case msg := <-ch:
			p.claim.msg <- []byte(msg.Payload)
		case <-p.quit:
			break FOR
		}
	}
	p.claim.err <- nil
	p.sub.Close()
	return p.consumer.Cleanup()
}

func (p *puller) shutdown() {
	close(p.quit)
}

// claim implements mq.Claim
type claim struct {
	err chan error
	msg chan []byte
}

func newClaim() *claim {
	return &claim{
		err: make(chan error),
		msg: make(chan []byte, 64),
	}
}

func (claim *claim) Err() chan<- error      { return claim.err }
func (claim *claim) Message() chan<- []byte { return claim.msg }

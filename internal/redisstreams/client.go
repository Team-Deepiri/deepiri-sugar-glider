package redisstreams

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	inner *redis.Client
}

func New(redisURL string) (*Client, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)
	return &Client{inner: client}, nil
}

func (c *Client) Ping(ctx context.Context) error {
	return c.inner.Ping(ctx).Err()
}

func (c *Client) Close() error {
	return c.inner.Close()
}

func (c *Client) Raw() *redis.Client {
	return c.inner
}

package proxy

import (
	"context"
)

type Options struct {
	// Ctx is the base context. (Default to context.Background)
	Ctx context.Context

	// ListenAddr is the addr to listen to. (Default to "127.0.0.1:6378")
	ListenAddr string

	// UpstreamAddr is the addr of upstream redis server. (Default to ":6379")
	UpstreamAddr string

	// KeyName is the key of stream. (Default to "maxwell")
	KeyName string

	// MaxLenApprox is used to trim stream. (Default to 0, no trim at all)
	MaxLenApprox int64
}

// NewProxy creates a new proxy.
func (opts Options) NewProxy() *Proxy {
	if opts.Ctx == nil {
		opts.Ctx = context.Background()
	}
	if opts.ListenAddr == "" {
		opts.ListenAddr = "127.0.0.1:6378"
	}
	if opts.UpstreamAddr == "" {
		opts.UpstreamAddr = ":6379"
	}
	if opts.KeyName == "" {
		opts.KeyName = "maxwell"
	}
	return &Proxy{
		opts: opts,
	}
}

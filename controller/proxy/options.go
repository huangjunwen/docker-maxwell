package proxy

type Options struct {
	// ListenPort is the port to listen to. (Default to "6378")
	ListenPort string

	// UpstreamPort is the port of upstream redis server. (Default to "6379")
	UpstreamPort string

	// KeyName is the key of stream. (Default to "maxwell")
	KeyName string

	// MaxLenApprox is used to trim stream. (Default to 0, no trim at all)
	MaxLenApprox int64
}

// NewProxy creates a new proxy.
func (opts Options) NewProxy() *Proxy {
	if opts.ListenPort == "" {
		opts.ListenPort = "6378"
	}
	if opts.UpstreamPort == "" {
		opts.UpstreamPort = "6379"
	}
	if opts.KeyName == "" {
		opts.KeyName = "maxwell"
	}
	return &Proxy{
		opts: opts,
	}
}

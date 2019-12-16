package proxy

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	"github.com/tidwall/redcon"
)

type Proxy struct {
	// options.
	opts Options

	// the proxy server.
	server *redcon.Server

	// the real redis server.
	upstream *redis.Client

	// used to wait handler.
	wg sync.WaitGroup

	// proxy should skip to this id.
	skipToId StreamId

	// the last id.
	lastId StreamId

	// decoded json maxwell message.
	lastMsg message
}

type message struct {
	Position string `json:"position"`
}

// Init the proxy.
func (proxy *Proxy) Init() error {
	// Create upstream client and wait.
	proxy.upstream = redis.NewClient(&redis.Options{
		Addr: proxy.opts.UpstreamAddr,
	})

	for {
		_, err := proxy.upstream.Ping().Result()
		if err == nil {
			break
		}
		log.Printf("[ERR] Ping returns error: %s, wait a while...\n", err)
		select {
		case <-proxy.opts.Ctx.Done():
			return errors.WithMessagef(proxy.opts.Ctx.Err(), "Context done during ping wait")
		case <-time.After(time.Second):
		}
	}

	// Get the last entry in stream.
	result, err := proxy.upstream.XRevRangeN(proxy.opts.KeyName, "+", "-", 1).Result()
	if err != nil {
		return errors.WithMessagef(err, "Get last entry in stream(%+q) error", proxy.opts.KeyName)
	}

	// Set skipToId.
	if len(result) != 0 {
		// Should skip to the last id in redis.
		id := ParseStreamId(result[0].ID)
		if !id.Valid() {
			return fmt.Errorf("Last id(%+q) in stream(%+q) is not valid", result[0].ID, proxy.opts.KeyName)
		}
		proxy.skipToId = id
		log.Printf("[INF] Skip to id found %s\n", proxy.skipToId.String())
	}

	// Create proxy server.
	proxy.server = redcon.NewServer(proxy.opts.ListenAddr, proxy.handler, proxy.accept, proxy.closed)
	return nil
}

// Run the proxy.
func (proxy *Proxy) Run() {
	proxy.wg.Add(1)
	go func() {
		defer proxy.wg.Done()
		if err := proxy.server.ListenAndServe(); err != nil {
			log.Printf("[ERR] Proxy.Run returns %s\n", err)
		}
	}()
	<-proxy.opts.Ctx.Done()
	log.Printf("[INF] Proxy ready to close\n")
	proxy.server.Close()
	proxy.wg.Wait()
	log.Printf("[INF] Proxy closed\n")
}

func (proxy *Proxy) handler(conn redcon.Conn, cmd redcon.Command) {
	// Wait group.
	proxy.wg.Add(1)
	defer proxy.wg.Done()

	// Expect xadd only.
	cmdName := strings.ToLower(string(cmd.Args[0]))
	if cmdName != "xadd" {
		panic(fmt.Errorf("Got unexpected command %q", cmdName))
	}

	// Extract args.
	// key := string(cmd.Args[1]) // 'maxwell'
	// id := string(cmd.Args[2]) // '*'
	field := string(cmd.Args[3]) // 'message'
	value := cmd.Args[4]         // json encoded
	proxy.lastMsg = message{}
	if err := json.Unmarshal(value, &proxy.lastMsg); err != nil {
		panic(fmt.Errorf("Decode json error: %s", err.Error()))
	}

	// NOTE: Ignore it silently if the message has no position.
	if proxy.lastMsg.Position == "" {
		// Returns an invalid id.
		// XXX: the minimal valid id is '0-1'. See https://redis.io/commands/xadd
		conn.WriteString("0-0")
		return
	}

	// Parse it.
	pos := ParseMaxwellBinlogPos(proxy.lastMsg.Position)
	if !pos.Valid() {
		panic(fmt.Errorf("Invalid binlog position %+q", proxy.lastMsg.Position))
	}

	// Update lastId.
	if pos == proxy.lastId.BinlogPos {
		proxy.lastId.Counter++
	} else {
		proxy.lastId = StreamId{
			BinlogPos: pos,
			Counter:   1,
		}
	}

	// If lastId is not after skipToId. Ignore it.
	if !proxy.lastId.After(proxy.skipToId) {
		conn.WriteString(proxy.lastId.Format())
		return
	}

	// Now send to upstream.
	result, err := proxy.upstream.XAdd(&redis.XAddArgs{
		Stream:       proxy.opts.KeyName,
		MaxLenApprox: proxy.opts.MaxLenApprox,
		ID:           proxy.lastId.Format(),
		Values: map[string]interface{}{
			field: value,
		},
	}).Result()
	if err != nil {
		panic(fmt.Errorf("XAdd %s error: %s", proxy.lastId.String(), err.Error()))
	}
	conn.WriteString(result)
}

func (proxy *Proxy) accept(conn redcon.Conn) bool {
	log.Printf("[INF] Accept conn[%s]\n", conn.RemoteAddr())
	return true
}

func (proxy *Proxy) closed(conn redcon.Conn, err error) {
	log.Printf("[INF] Conn closed[%s]: %v\n", conn.RemoteAddr(), err)
}

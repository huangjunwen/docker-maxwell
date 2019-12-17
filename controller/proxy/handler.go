package proxy

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
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

	// 1 if closed.
	closed int32

	// used to log the first entry.
	first bool

	// proxy should skip to this id.
	skipToId StreamId

	// last id sent from maxwell.
	lastId StreamId

	// last decoded json message sent from maxwell.
	lastMsg message
}

type message struct {
	Position string `json:"position"`
}

// Init the proxy.
func (proxy *Proxy) Init() error {
	// Create upstream client and wait.
	proxy.upstream = redis.NewClient(&redis.Options{
		Addr: ":" + proxy.opts.UpstreamPort,
	})

	for {
		_, err := proxy.upstream.Ping().Result()
		if err == nil {
			break
		}
		log.Printf("[ERR] Ping redis returns error: %s, wait a while...\n", err)
		time.Sleep(time.Second)
	}
	log.Printf("[INF] Ping redis ok.\n")

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
		log.Printf("[INF] Skip to id found %v.\n", proxy.skipToId)
	} else {
		log.Printf("[INF] No skip to id.\n")
	}

	// Create proxy server.
	proxy.server = redcon.NewServer("127.0.0.1:"+proxy.opts.ListenPort, proxy.handler, proxy.acceptHandler, proxy.closedHandler)
	return nil
}

// Run the proxy.
func (proxy *Proxy) Run() error {
	return proxy.server.ListenAndServe()
}

// Close and wait everything cleanup.
func (proxy *Proxy) Close() error {
	ret := proxy.server.Close()
	atomic.StoreInt32(&proxy.closed, 1)
	proxy.wg.Wait()
	return ret
}

func (proxy *Proxy) handler(conn redcon.Conn, cmd redcon.Command) {
	proxy.wg.Add(1)
	defer proxy.wg.Done()
	if atomic.LoadInt32(&proxy.closed) > 0 {
		return
	}

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
		conn.WriteString(proxy.lastId.String())
		return
	}

	// Now send to upstream.
	result, err := proxy.upstream.XAdd(&redis.XAddArgs{
		Stream:       proxy.opts.KeyName,
		MaxLenApprox: proxy.opts.MaxLenApprox,
		ID:           proxy.lastId.String(),
		Values: map[string]interface{}{
			field: value,
		},
	}).Result()
	if err != nil {
		panic(fmt.Errorf("XAdd %v error: %s", proxy.lastId, err.Error()))
	}
	conn.WriteString(result)

	if !proxy.first {
		log.Printf("[INF] First entry: %v %s\n", proxy.lastId, value)
		proxy.first = true
	}
}

func (proxy *Proxy) acceptHandler(conn redcon.Conn) bool {
	log.Printf("[INF] Accept conn[%s]\n", conn.RemoteAddr())
	return true
}

func (proxy *Proxy) closedHandler(conn redcon.Conn, err error) {
	log.Printf("[INF] Conn closed[%s]: %v\n", conn.RemoteAddr(), err)
}

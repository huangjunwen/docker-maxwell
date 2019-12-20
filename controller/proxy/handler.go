package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/tidwall/redcon"
)

var (
	ErrInvalidStreamId = errors.New("Invalid stream id")
)

type Proxy struct {
	opts     Options
	ctx      context.Context
	cancel   func()
	client   *redis.Client
	server   *redcon.Server
	wg       sync.WaitGroup
	skipToId StreamId
	lastId   StreamId
	lastMsg  message
	first    bool
}

type message struct {
	Position string `json:"position"`
}

// Run the proxy.
func (proxy *Proxy) Run(ctx context.Context, cancel func()) error {
	proxy.ctx = ctx
	proxy.cancel = cancel
	proxy.client = redis.NewClient(&redis.Options{
		Addr: ":" + proxy.opts.RedisPort,
	})
	proxy.server = redcon.NewServer("127.0.0.1:"+proxy.opts.ListenPort, proxy.handler, proxy.acceptHandler, proxy.closedHandler)

	// Ping and wait.
	for {
		_, err := proxy.client.Ping().Result()
		if err == nil {
			break
		}
		proxy.infof("Redis.Ping returns error: %s, wait a while", err.Error())

		select {
		case <-ctx.Done():
			proxy.errorf("Context done during ping wait")
			return ctx.Err()

		case <-time.After(time.Second):
		}
	}
	proxy.infof("Redis.Ping ok")

	// Get last message's id as skipToId.
	result, err := proxy.client.XRevRangeN(proxy.opts.KeyName, "+", "-", 1).Result()
	if err != nil {
		proxy.errorf("Got last entry in stream error: %s", err.Error())
		return err
	}

	if len(result) != 0 {
		id := ParseStreamId(result[0].ID)
		if !id.Valid() {
			proxy.errorf("Last id in stream is invalid: %s", result[0].ID)
			return ErrInvalidStreamId
		}
		proxy.skipToId = id
	}
	proxy.infof("Redis skip id: %v", proxy.skipToId)

	// Wait other go routines started by the proxy.
	defer proxy.wg.Wait()

	// Close the proxy when context done.
	proxy.wg.Add(1)
	go func() {
		defer proxy.wg.Done()
		<-ctx.Done()
		proxy.server.Close()
	}()

	// ListenAndServe.
	if err := proxy.server.ListenAndServe(); err != nil {
		proxy.errorf("ListenAndServe returns error: %s", err.Error())
		return err
	}

	return nil
}

func (proxy *Proxy) handler(conn redcon.Conn, cmd redcon.Command) {
	proxy.wg.Add(1)
	defer proxy.wg.Done()

	select {
	case <-proxy.ctx.Done():
		return
	default:
	}

	// Expect xadd only.
	cmdName := strings.ToLower(string(cmd.Args[0]))
	if cmdName != "xadd" {
		proxy.errorf("Proxy got unexpected command %q", cmdName)
		return
	}

	// Extract args.
	// key := string(cmd.Args[1]) // 'maxwell'
	// id := string(cmd.Args[2]) // '*'
	field := string(cmd.Args[3]) // 'message'
	value := cmd.Args[4]         // json encoded
	proxy.lastMsg = message{}
	if err := json.Unmarshal(value, &proxy.lastMsg); err != nil {
		proxy.errorf("Proxy decode json error: %s", err.Error())
		return
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
		proxy.errorf("Proxy got invalid maxwell binlog position: %q", proxy.lastMsg.Position)
		return
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

	// Now send to real redis server.
	result, err := proxy.client.XAdd(&redis.XAddArgs{
		Stream:       proxy.opts.KeyName,
		MaxLenApprox: proxy.opts.MaxLenApprox,
		ID:           proxy.lastId.String(),
		Values: map[string]interface{}{
			field: value,
		},
	}).Result()
	if err != nil {
		proxy.errorf("XAdd %v error: %s", proxy.lastId, err.Error())
		return
	}
	conn.WriteString(result)

	if !proxy.first {
		proxy.infof("First entry: %v %s", proxy.lastId, value)
		proxy.first = true
	}
}

func (proxy *Proxy) acceptHandler(conn redcon.Conn) bool {
	proxy.infof("Accept conn %s", conn.RemoteAddr())
	return true
}

func (proxy *Proxy) closedHandler(conn redcon.Conn, err error) {
	proxy.infof("Conn closed %s: %s", conn.RemoteAddr(), err.Error())
}

func (proxy *Proxy) infof(format string, v ...interface{}) {
	log.Printf("[INF][PROXY] "+format+"\n", v...)
}

// Any error will cause termination.
func (proxy *Proxy) errorf(format string, v ...interface{}) {
	log.Printf("[ERR][PROXY] "+format+"\n", v...)
	proxy.cancel()
}

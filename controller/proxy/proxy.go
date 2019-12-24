package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/tidwall/redcon"
)

type Proxy struct {
	opts           Options
	loggerFile     *os.File
	logger         *log.Logger
	ctx            context.Context
	cancel         func()
	client         *redis.Client
	server         *redcon.Server
	skipToId       StreamId
	wg             sync.WaitGroup
	lastId         StreamId
	lastMsg        message
	firstHasLogged bool
}

type message struct {
	Position string `json:"position"`
}

func (opts Options) NewProxy(ctx context.Context) (proxy *Proxy, err error) {
	if opts.ListenPort == "" {
		opts.ListenPort = "6378"
	}
	if opts.RedisPort == "" {
		opts.RedisPort = "6379"
	}
	if opts.KeyName == "" {
		opts.KeyName = "maxwell"
	}
	if opts.LogFile == "" {
		opts.LogFile = "proxy.log"
	}

	proxy = &Proxy{
		opts: opts,
	}

	loggerFile, err := os.OpenFile(opts.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			loggerFile.Close()
		}
	}()
	proxy.loggerFile = loggerFile

	proxy.logger = log.New(proxy.loggerFile, "", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)

	ctx, cancel := context.WithCancel(ctx)
	proxy.ctx = ctx
	proxy.cancel = cancel

	proxy.client = redis.NewClient(&redis.Options{
		Addr: ":" + opts.RedisPort,
	})

	proxy.server = redcon.NewServer(
		"127.0.0.1:"+proxy.opts.ListenPort,
		proxy.handler,
		proxy.acceptHandler,
		proxy.closedHandler,
	)

	// Ping and wait.
	for {
		_, err := proxy.client.Ping().Result()
		if err == nil {
			break
		}

		proxy.logger.Printf("Redis.Ping returns error: %s; Wait a while....\n", err)

		select {
		case <-ctx.Done():
			proxy.logger.Printf("Ctx done during ping wait: %s\n", err.Error())
			return nil, ctx.Err()

		case <-time.After(time.Second):
		}
	}

	// Get last message's id as skipToId.
	result, err := proxy.client.XRevRangeN(proxy.opts.KeyName, "+", "-", 1).Result()
	if err != nil {
		proxy.logger.Printf("XRevRangeN returns error: %s\n", err.Error())
		return nil, err
	}

	if len(result) != 0 {
		id := ParseStreamId(result[0].ID)
		if !id.Valid() {
			proxy.logger.Printf("ParseStreamId failed: %+q\n", result[0].ID)
			return nil, fmt.Errorf("Invalid stream id %s", result[0].ID)
		}
		proxy.skipToId = id
		proxy.logger.Printf("SkipToId is: %v\n", proxy.skipToId)
	}

	return proxy, nil

}

func (proxy *Proxy) Run() {
	defer func() {
		// Wait handler and context listener (see below)
		// end after context done.
		proxy.wg.Wait()
		proxy.logger.Printf("Bye bye~\n")
	}()

	// Context listener.
	proxy.wg.Add(1)
	go func() {
		defer proxy.wg.Done()
		defer proxy.server.Close()
		<-proxy.ctx.Done()
		proxy.logger.Printf("Ctx done.")
	}()

	// ListenAndServe.
	if err := proxy.server.ListenAndServe(); err != nil {
		proxy.logger.Printf("ListenAndServe returns error: %s\n", err.Error())
	}
	proxy.logger.Printf("ListenAndServe returns no error\n")
	return
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
		proxy.logger.Printf("Got unexpected command %q\n", cmdName)
		proxy.cancel()
		return
	}

	// Extract args.
	// key := string(cmd.Args[1]) // 'maxwell'
	// id := string(cmd.Args[2]) // '*'
	field := string(cmd.Args[3]) // 'message'
	value := cmd.Args[4]         // json encoded
	proxy.lastMsg = message{}
	if err := json.Unmarshal(value, &proxy.lastMsg); err != nil {
		proxy.logger.Printf("Decode json error: %s\n", err.Error())
		proxy.cancel()
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
		proxy.logger.Printf("Got invalid maxwell binlog position %q\n", proxy.lastMsg.Position)
		proxy.cancel()
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
		proxy.logger.Printf("XAdd %v error: %s\n", proxy.lastId, err.Error())
		proxy.cancel()
		return
	}
	conn.WriteString(result)

	if !proxy.firstHasLogged {
		proxy.logger.Printf("First entry %v: %s\n", proxy.lastId, value)
		proxy.firstHasLogged = true
	}
}

func (proxy *Proxy) acceptHandler(conn redcon.Conn) bool {
	proxy.logger.Printf("Accept conn %s\n", conn.RemoteAddr())
	return true
}

func (proxy *Proxy) closedHandler(conn redcon.Conn, err error) {
	proxy.logger.Printf("Conn closed %s: %s\n", conn.RemoteAddr(), err.Error())
}

func (proxy *Proxy) SkipToId() StreamId {
	return proxy.skipToId
}

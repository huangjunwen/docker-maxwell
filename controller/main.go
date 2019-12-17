package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"

	"github.com/huangjunwen/docker-maxwell/controller/proxy"
)

var (
	wg                = &sync.WaitGroup{}
	stopCtx, stopFunc = context.WithCancel(context.Background())
	stopOnce          = &sync.Once{}
)

func stop() {
	stopOnce.Do(stopFunc)
}

func handleSigal() {

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		log.Printf("[INF] Got signal %s\n", sig.String())
		signal.Stop(sigCh)
		stop()
	}()
}

var (
	redisServerPath  = ""
	upstreamConfPath = ""
	upstreamPort     = ""
	proxyPort        = ""
	keyName          = ""
	maxLenApprox     = int64(0)
)

func main() {
	// Parse flags.
	flag.StringVar(&redisServerPath, "redis_server", "redis-server", "Path to redis server")
	flag.StringVar(&upstreamConfPath, "upstream_conf", "/etc/redis/redis.conf", "Path to upstream redis conf")
	flag.StringVar(&upstreamPort, "upstream_port", "6379", "Upstream redis listen port")
	flag.StringVar(&proxyPort, "listen_port", "6378", "Redis proxy listen port")
	flag.StringVar(&keyName, "key_name", "maxwell", "Key of the stream")
	flag.Int64Var(&maxLenApprox, "max_len_approx", 0, "Maximum length of the stream (approx), 0 for no limit")
	flag.Parse()

	// Install signal handler.
	handleSigal()

	// Wait all exit.
	defer func() {
		wg.Wait()
		log.Printf("[INF] Controller exit now, bye bye~\n")
	}()

	// Run upstream redis.
	upstream := exec.Command(redisServerPath, upstreamConfPath, "--port", upstreamPort)
	if err := upstream.Start(); err != nil {
		log.Panicf("[ERR] Start upstream returns error: %s\n", err)
	}

	defer func() {
		log.Printf("[INF] Upstream ready to exit\n")
		upstream.Process.Signal(syscall.SIGTERM)
		if err := upstream.Wait(); err != nil {
			log.Printf("[ERR] Upstream exit with error: %s\n", err)
		} else {
			log.Printf("[INF] Upstream exit ok\n")
		}
	}()

	// Run proxy.
	proxy := proxy.Options{
		ListenPort:   proxyPort,
		UpstreamPort: upstreamPort,
		KeyName:      keyName,
		MaxLenApprox: maxLenApprox,
	}.NewProxy()
	if err := proxy.Init(); err != nil {
		log.Panicf("[ERR] Start proxy returns error: %s\n", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		proxy.Run()
	}()

	defer func() {
		log.Printf("[INF] Proxy ready to exit\n")
		if err := proxy.Close(); err != nil {
			log.Printf("[ERR] Proxy exit with error: %s\n", err)
		} else {
			log.Printf("[INF] Proxy exit ok\n")
		}
	}()

	// TODO: Run maxwell.

	// Wait signal.
	<-stopCtx.Done()

}

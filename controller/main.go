package main

import (
	"context"
	"flag"
	"log"
	"os"
	//"os/exec"
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
	proxyOpts = proxy.Options{}
)

func main() {
	// Parse flags.
	flag.StringVar(&proxyOpts.ListenPort, "listen_port", "6378", "Redis proxy listen port")
	flag.StringVar(&proxyOpts.UpstreamPort, "upstream_port", "6379", "Upstream redis listen port")
	flag.StringVar(&proxyOpts.KeyName, "key_name", "maxwell", "Key of the stream")
	flag.Int64Var(&proxyOpts.MaxLenApprox, "max_len_approx", 0, "Maximum length of the stream (approx), 0 for no limit")
	flag.Parse()

	// Install signal handler.
	handleSigal()

	// Init proxy.
	proxy := proxyOpts.NewProxy()
	if err := proxy.Init(); err != nil {
		log.Fatalf("[FATAL] %s\n", err)
	}

	// Run proxy.
	wg.Add(1)
	go func() {
		defer wg.Done()
		proxy.Run()
	}()

	// Wait signal.
	<-stopCtx.Done()

	// Stop proxy.
	proxy.Close()

	// Wait all.
	wg.Wait()
	log.Printf("[INF] controller exit now, bye bye~\n")
}

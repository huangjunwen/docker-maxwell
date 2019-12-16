package main

import (
	"context"
	"log"
	"os"
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

var (
	proxyOpts = proxy.Options{
		Ctx: stopCtx,
	}
)

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

func main() {

	handleSigal()

	proxy := proxyOpts.NewProxy()
	if err := proxy.Init(); err != nil {
		log.Fatal(err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		proxy.Run()
	}()

	wg.Wait()
	log.Printf("[INF] controller exit now, bye bye~\n")
}

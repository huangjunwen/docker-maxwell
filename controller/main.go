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
	maxwellPath      = ""
	upstreamConfPath = ""
	upstreamPort     = ""
	proxyPort        = ""
	keyName          = ""
	maxLenApprox     = int64(0)
	mysqlUser        = ""
	mysqlPassword    = ""
	mysqlHost        = ""
	mysqlPort        = ""
	mysqlSchemaDB    = ""
	maxwellClientId  = ""
)

func main() {
	// Parse flags.
	flag.StringVar(&redisServerPath, "redis_server_path", "redis-server", "Path to redis server")
	flag.StringVar(&maxwellPath, "maxwell_path", "maxwell", "Path to maxwell")
	flag.StringVar(&upstreamConfPath, "upstream_conf", "/etc/redis/redis.conf", "Path to upstream redis conf")
	flag.StringVar(&upstreamPort, "upstream_port", "6379", "Upstream redis listen port")
	flag.StringVar(&proxyPort, "listen_port", "6378", "Redis proxy listen port")
	flag.StringVar(&keyName, "key_name", "maxwell", "Key of the stream")
	flag.Int64Var(&maxLenApprox, "max_len_approx", 0, "Maximum length of the stream (approx), 0 for no limit")
	flag.StringVar(&mysqlUser, "mysql_user", "", "MySQL user")
	flag.StringVar(&mysqlPassword, "mysql_password", "", "MySQL password")
	flag.StringVar(&mysqlHost, "mysql_host", "127.0.0.1", "MySQL host")
	flag.StringVar(&mysqlPort, "mysql_port", "3306", "MySQL port")
	flag.StringVar(&mysqlSchemaDB, "mysql_schema_db", "maxwell", "MySQL database to store maxwell schema info")
	flag.StringVar(&maxwellClientId, "maxwell_client_id", "maxwell", "Maxwell client id")
	flag.Parse()

	if mysqlUser == "" {
		log.Fatal("Missing -mysql_user")
	}
	if mysqlPassword == "" {
		log.Fatal("Missing -mysql_password")
	}

	// Install signal handler.
	handleSigal()

	// Wait all exit.
	defer func() {
		wg.Wait()
		log.Printf("[INF] Controller exit now, bye bye~\n")
	}()

	// Run upstream redis.
	upstream := exec.Command(
		redisServerPath,
		upstreamConfPath,
		"--port", upstreamPort,
		"--logfile", "redis.log",
	)
	if err := upstream.Start(); err != nil {
		log.Panicf("[ERR] Upstream.Start returns error: %s\n", err)
	}
	log.Printf("[INF] Upstream.Start ok, pid: %d\n", upstream.Process.Pid)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer stop() // trigger other to stop
		if err := upstream.Wait(); err != nil {
			log.Printf("[ERR] Upstream.Wait returns error: %s\n", err)
		} else {
			log.Printf("[INF] Upstream.Wait ok\n")
		}
	}()

	defer upstream.Process.Signal(syscall.SIGTERM)

	// Run proxy.
	proxy := proxy.Options{
		ListenPort:   proxyPort,
		UpstreamPort: upstreamPort,
		KeyName:      keyName,
		MaxLenApprox: maxLenApprox,
	}.NewProxy()
	if err := proxy.Init(); err != nil {
		log.Panicf("[ERR] Proxy.Init returns error: %s\n", err)
	}
	log.Printf("[INF] Proxy.Init ok\n")

	wg.Add(1)
	go func() {
		defer wg.Done()
		proxy.Run()
	}()

	defer func() {
		if err := proxy.Close(); err != nil {
			log.Printf("[ERR] Proxy.Close returns error: %s\n", err)
		} else {
			log.Printf("[INF] Proxy.Close ok\n")
		}
	}()

	// Run maxwell.
	maxwell := exec.Command(
		maxwellPath,
		"--env_config_prefix", "MAXWELL_",
		"--user", mysqlUser,
		"--password", mysqlPassword,
		"--host", mysqlHost,
		"--port", mysqlPort,
		"--producer", "redis",
		"--redis_type", "xadd",
		"--redis_host", "localhost",
		"--redis_database", "0",
		"--redis_port", proxyPort,
		"--redis_key", keyName,
		"--output_binlog_position", "true",
		"--output_commit_info", "true",
		"--output_xoffset", "true",
		"--output_primary_keys", "true",
		"--output_primary_key_columns", "true",
		"--output_ddl", "true",
		"--bootstrap", "none", // disable bootstrap
	)
	maxwellLog, err := os.OpenFile("maxwell.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Panicf("[ERR] Open maxwell.log returns error: %s\n", err)
	}
	defer maxwellLog.Close()

	maxwell.Stdout = maxwellLog
	maxwell.Stderr = maxwellLog

	if err := maxwell.Start(); err != nil {
		log.Panicf("[ERR] Maxwell.Start returns error: %s\n", err)
	}
	log.Printf("[INF] Maxwell.Start ok, pid: %d\n", maxwell.Process.Pid)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer stop() // trigger other to stop
		if err := maxwell.Wait(); err != nil {
			log.Printf("[ERR] Maxwell.Wait returns error: %s\n", err)
		} else {
			log.Printf("[INF] Maxwell.Wait ok\n")
		}
	}()

	defer maxwell.Process.Signal(syscall.SIGINT)

	// Wait signal.
	<-stopCtx.Done()

}

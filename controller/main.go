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
	wg            = &sync.WaitGroup{}
	stopCtx, stop = context.WithCancel(context.Background())
)

func handleSigal() {

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		log.Printf("[INF][CONTROLLER] Got signal %s\n", sig.String())
		signal.Stop(sigCh)
		stop()
	}()

}

var (
	// Hard coded vars.
	redisServerPath = "redis-server"
	maxwellPath     = "maxwell"
	redisPort       = "6379"
	proxyPort       = "6378"
	// Flags.
	redisConfPath   = ""
	redisAppendOnly = false
	keyName         = ""
	maxLenApprox    = int64(0)
	mysqlUser       = ""
	mysqlPassword   = ""
	mysqlHost       = ""
	mysqlPort       = ""
	maxwellSchemaDB = ""
	maxwellClientId = ""
)

func main() {
	// Parse flags.
	flag.StringVar(&redisConfPath, "redis_conf", "/etc/redis/redis.conf", "Path to redis conf")
	flag.BoolVar(&redisAppendOnly, "redis_append_only", true, "Turn on aof")
	flag.StringVar(&keyName, "key_name", "maxwell", "Key of the stream")
	flag.Int64Var(&maxLenApprox, "max_len_approx", 0, "Maximum length of the stream (approx), 0 for no limit")
	flag.StringVar(&mysqlUser, "mysql_user", "", "MySQL user")
	flag.StringVar(&mysqlPassword, "mysql_password", "", "MySQL password")
	flag.StringVar(&mysqlHost, "mysql_host", "127.0.0.1", "MySQL host")
	flag.StringVar(&mysqlPort, "mysql_port", "3306", "MySQL port")
	flag.StringVar(&maxwellSchemaDB, "maxwell_schema_db", "maxwell", "MySQL database to store maxwell schema info")
	flag.StringVar(&maxwellClientId, "maxwell_client_id", "maxwell", "Maxwell client id")
	flag.Parse()

	if mysqlUser == "" {
		log.Fatal("Missing -mysql_user")
	}
	if mysqlPassword == "" {
		log.Fatal("Missing -mysql_password")
	}

	wd, err := os.Getwd()
	if err != nil {
		log.Panicf("[INF][CONTROLLER] Can't get curr working directory\n")
	}

	// Install signal handler.
	handleSigal()

	// Wait all exit.
	defer func() {
		if rcv := recover(); rcv != nil {
			log.Printf("[INF][CONTROLLER] Recover panic: %v\n", rcv)
		}
		wg.Wait()
		log.Printf("[INF][CONTROLLER] Exit now, bye bye~\n")
	}()

	// Run redis.
	{
		args := []string{}
		if redisConfPath != "" {
			args = append(args, redisConfPath)
		}
		args = append(args,
			"--bind", "0.0.0.0",
			"--port", redisPort,
			"--pidfile", "",
			"--daemonize", "no",
			"--dir", wd,
			"--logfile", "redis.log",
			"--dbfilename", "dump.rdb",
			"--appendfilename", "appendonly.aof",
		)
		if redisAppendOnly {
			args = append(args,
				"--appendonly", "yes",
			)
		}
		redisServer := exec.Command(redisServerPath, args...)

		if err := redisServer.Start(); err != nil {
			log.Panicf("[ERR][REDIS] Start returns error: %s\n", err)
		}
		log.Printf("[INF][REDIS] Start ok, pid: %d\n", redisServer.Process.Pid)

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer stop() // trigger other to stop
			if err := redisServer.Wait(); err != nil {
				log.Printf("[ERR][REDIS] Exit returns error: %s\n", err)
			} else {
				log.Printf("[INF][REDIS] Exit ok\n")
			}
		}()

		defer redisServer.Process.Signal(syscall.SIGTERM)
	}

	// Run proxy.
	skipToId := proxy.StreamId{}
	{
		proxy, err := proxy.Options{
			ListenPort:   proxyPort,
			RedisPort:    redisPort,
			KeyName:      keyName,
			MaxLenApprox: maxLenApprox,
		}.NewProxy(stopCtx)
		if err != nil {
			log.Panicf("[ERR][PROXY] NewProxy failed: %s\n", err.Error())
		}

		skipToId = proxy.SkipToId()
		log.Printf("[INF][PROXY] NewProxy ok, skipToId is %v\n", skipToId)

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer stop() // trigger other to stop
			log.Printf("[INF][PROXY] Ready to run\n")
			proxy.Run()
			log.Printf("[INF][PROXY] Exit\n")
		}()
	}

	// Run maxwell.
	{
		args := []string{
			"--env_config_prefix", "MAXWELL_",
			"--user", mysqlUser,
			"--password", mysqlPassword,
			"--host", mysqlHost,
			"--port", mysqlPort,
			"--schema_database", maxwellSchemaDB,
			"--client_id", maxwellClientId,
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
		}
		maxwell := exec.Command(maxwellPath, args...)

		maxwellLog, err := os.OpenFile("maxwell.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Panicf("[ERR][MAXWELL] Open log returns error: %s\n", err)
		}
		defer maxwellLog.Close()
		maxwell.Stdout = maxwellLog
		maxwell.Stderr = maxwellLog

		if err := maxwell.Start(); err != nil {
			log.Panicf("[ERR][MAXWELL] Start returns error: %s\n", err)
		}
		log.Printf("[INF][MAXWELL] Start ok, pid: %d\n", maxwell.Process.Pid)

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer stop() // trigger other to stop
			if err := maxwell.Wait(); err != nil {
				log.Printf("[ERR][MAXWELL] Exit returns error: %s\n", err)
			} else {
				log.Printf("[INF][MAXWELL] Exit ok\n")
			}
		}()

		defer maxwell.Process.Signal(syscall.SIGTERM)
	}

	// Wait signal.
	<-stopCtx.Done()

}

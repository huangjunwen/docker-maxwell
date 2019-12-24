package main

import (
	"context"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/huangjunwen/docker-maxwell/controller/proxy"
)

func getWd() string {
	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("[FATAL][CONTROLLER] Can't get curr working directory\n")
	}
	return wd
}

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

func loadConfig() {
	// Set defaults.
	viper.SetDefault("redis_conf", "/etc/redis/redis.conf")
	viper.SetDefault("redis_append_only", true)
	viper.SetDefault("key_name", "maxwell")
	viper.SetDefault("mysql_host", "127.0.0.1")
	viper.SetDefault("mysql_port", "3306")
	viper.SetDefault("maxwell_schema_db", "maxwell")
	viper.SetDefault("maxwell_client_id", "maxwell")

	// Read config file.
	viper.SetConfigName("controller")
	viper.AddConfigPath("$HOME")
	viper.AddConfigPath(".")
	viper.ReadInConfig()

	// Parse flags.
	pflag.String("redis_conf", "/etc/redis/redis.conf", "Path to redis conf")
	pflag.Bool("redis_append_only", true, "Turn on aof")
	pflag.String("key_name", "maxwell", "Key of the stream")
	pflag.Int64("max_len_approx", 0, "Maximum length of the stream (approx), 0 for no limit")
	pflag.String("mysql_user", "", "MySQL user")
	pflag.String("mysql_password", "", "MySQL password")
	pflag.String("mysql_host", "127.0.0.1", "MySQL host")
	pflag.String("mysql_port", "3306", "MySQL port")
	pflag.String("maxwell_schema_db", "maxwell", "MySQL database to store maxwell schema info")
	pflag.String("maxwell_client_id", "maxwell", "Maxwell client id")
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	// Explicitly set.
	viper.Set("redis_server_executable", "redis-server")
	viper.Set("maxwell_executable", "maxwell")
	viper.Set("redis_port", "6379")
	viper.Set("proxy_port", "6378")

	// Some checks.
	if viper.GetString("mysql_user") == "" {
		log.Fatalf("[FATAL][CONTROLLER] Missing mysql_user\n")
	}
	if viper.GetString("mysql_password") == "" {
		log.Fatalf("[FATAL][CONTROLLER] Missing mysql_password\n")
	}

	// Some logs.
	log.Printf("[INF][CONTROLLER] Conf redis_conf: %v\n", viper.Get("redis_conf"))
	log.Printf("[INF][CONTROLLER] Conf redis_append_only: %v\n", viper.Get("redis_append_only"))
	log.Printf("[INF][CONTROLLER] Conf key_name: %v\n", viper.Get("key_name"))
	log.Printf("[INF][CONTROLLER] Conf max_len_approx: %v\n", viper.Get("max_len_approx"))
	log.Printf("[INF][CONTROLLER] Conf maxwell_schema_db: %v\n", viper.Get("maxwell_schema_db"))
	log.Printf("[INF][CONTROLLER] Conf maxwell_client_id: %v\n", viper.Get("maxwell_client_id"))
}

func main() {
	// Get working directory.
	wd := getWd()

	// Install signal handler.
	handleSigal()

	// Get config.
	loadConfig()

	// Catch all and wait.
	defer func() {
		if rcv := recover(); rcv != nil {
			log.Printf("[INF][CONTROLLER] Recover from panic: %v\n", rcv)
		}
		wg.Wait()
		log.Printf("[INF][CONTROLLER] Exit now, bye bye~\n")
	}()

	// Run redis.
	{
		args := []string{}
		if redisConf := viper.GetString("redis_conf"); redisConf != "" {
			args = append(args, redisConf)
		}
		args = append(args,
			"--bind", "0.0.0.0",
			"--port", viper.GetString("redis_port"),
			"--pidfile", "",
			"--daemonize", "no",
			"--dir", wd,
			"--logfile", "redis.log",
			"--dbfilename", "dump.rdb",
			"--appendfilename", "appendonly.aof",
		)
		if viper.GetBool("redis_append_only") {
			args = append(args,
				"--appendonly", "yes",
			)
		}

		redisServer := exec.Command(viper.GetString("redis_server_executable"), args...)
		redisServer.Stdout = os.Stdout
		redisServer.Stderr = os.Stderr

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
			ListenPort:   viper.GetString("proxy_port"),
			RedisPort:    viper.GetString("redis_port"),
			KeyName:      viper.GetString("key_name"),
			MaxLenApprox: viper.GetInt64("max_len_approx"),
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
			"--user", viper.GetString("mysql_user"),
			"--password", viper.GetString("mysql_password"),
			"--host", viper.GetString("mysql_host"),
			"--port", viper.GetString("mysql_port"),
			"--schema_database", viper.GetString("maxwell_schema_db"),
			"--client_id", viper.GetString("maxwell_client_id"),
			"--producer", "redis",
			"--redis_type", "xadd",
			"--redis_host", "localhost",
			"--redis_database", "0",
			"--redis_port", viper.GetString("proxy_port"),
			"--redis_key", viper.GetString("key_name"),
			"--output_binlog_position", "true",
			"--output_commit_info", "true",
			"--output_xoffset", "true",
			"--output_primary_keys", "true",
			"--output_primary_key_columns", "true",
			"--output_ddl", "true",
			"--bootstrap", "none", // disable bootstrap
		}
		maxwell := exec.Command(viper.GetString("maxwell_executable"), args...)

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

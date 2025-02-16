package main

import (
	"context"
	"flag"
	"github.com/chengcxy/goshift/configor"
	"github.com/chengcxy/goshift/logger"
	_ "github.com/chengcxy/goshift/plugin"
	"github.com/chengcxy/goshift/scheduler"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var ConfigPath string
var Env string
var UsedEnv bool
var TaskId string
var Cmd string
var Mode string
var config *configor.Config
var globalConfig *configor.Config
var cmdLine *scheduler.Cmdline

func init() {
	flag.StringVar(&ConfigPath, "c", "../etc/", "配置文件目录")
	flag.StringVar(&Env, "e", "local", "运行的环境-json文件前缀 dev/test/prod/local")
	flag.BoolVar(&UsedEnv, "UsedEnv", true, "是否走环境变量")
	flag.StringVar(&Cmd, "cmd", "sync", "命令")
	flag.StringVar(&Mode, "mode", "init", "模式")
	flag.StringVar(&TaskId, "id", "1", "任务id")
	flag.Parse()
	config = configor.NewConfig(ConfigPath, Env, UsedEnv)
	globalConfig = configor.NewConfig(ConfigPath, "db_global", UsedEnv)
	var dev bool
	if Env != "prod" {
		dev = true
	}
	LogPath, ok := config.Get("log.log_path")
	if !ok {
		panic("log.log_path not in json config file")
	}
	logger.InitLogger(logger.Config{
		Dev:           dev,
		NeedFileWrite: true,
		LogPath:       LogPath.(string),
		FilePrefix:    "goshift-",
	})
	logger.Infof("ConfigPath: %s ,Env: %s", ConfigPath, Env)
	cmdLine = scheduler.NewCmdline(Cmd, TaskId, Mode)
	logger.Infof("cmdLine %+v", cmdLine)
}

func main() {
	StartTime := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	// 捕获kill 信号，当收到 kill 信号时取消上下文
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalChan
		cancel()
	}()
	s, err := scheduler.NewScheduler(ctx, config, globalConfig, StartTime, cmdLine)
	if err != nil {
		logger.Errorf("init NewScheduler error %+v", err)
		os.Exit(0)
	}
	s.Run()

}

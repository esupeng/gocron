// Command gocron
//go:generate statik -src=../../web/public -dest=../../internal -f

package main

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"fmt"
	"github.com/ouqiang/gocron/internal/modules/notify"
	"github.com/ouqiang/gocron/internal/models"
	"github.com/ouqiang/gocron/internal/modules/app"
	"github.com/ouqiang/gocron/internal/modules/logger"
	"github.com/ouqiang/gocron/internal/modules/setting"
	"github.com/ouqiang/gocron/internal/routers"
	"github.com/ouqiang/gocron/internal/service"
	"github.com/ouqiang/goutil"
	"github.com/urfave/cli"
	"gopkg.in/macaron.v1"
)

var (
	lockLogFD *os.File
	AppVersion           = "1.5"
	BuildDate, GitCommit string
	warnStep int = 5
	errStep int = 50
)

// web服务器默认端口
const (
	DefaultPort = 5920
	lockRecord_Log  = "/tmp/mysql-lock.log"
)

func main() {
	cliApp := cli.NewApp()
	cliApp.Name = "gocron"
	cliApp.Usage = "gocron service"
	cliApp.Version, _ = goutil.FormatAppVersion(AppVersion, GitCommit, BuildDate)
	cliApp.Commands = getCommands()
	cliApp.Flags = append(cliApp.Flags, []cli.Flag{}...)
	cliApp.Run(os.Args)
}

// getCommands
func getCommands() []cli.Command {
	command := cli.Command{
		Name:   "web",
		Usage:  "run web server",
		Action: runWeb,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "host",
				Value: "0.0.0.0",
				Usage: "bind host",
			},
			cli.IntFlag{
				Name:  "port,p",
				Value: DefaultPort,
				Usage: "bind port",
			},
			cli.StringFlag{
				Name:  "env,e",
				Value: "prod",
				Usage: "runtime environment, dev|test|prod",
			},
		},
	}

	return []cli.Command{command}
}

func runWeb(ctx *cli.Context) {
	lockLogFD,_=os.OpenFile(lockRecord_Log,os.O_RDWR|os.O_CREATE|os.O_APPEND,0644)
	// 设置运行环境
	setEnvironment(ctx)
	// 初始化应用
	app.InitEnv(AppVersion)
	// 初始化模块 DB、定时任务等
	initModule()
	// 捕捉信号,配置热更新等
	go catchSignal()
	m := macaron.Classic()
	// 注册路由
	routers.Register(m)
	// 注册中间件.

	routers.RegisterMiddleware(m)
	//锁检测
	go mysqlLockDetector()
	host := parseHost(ctx)
	port := parsePort(ctx)
	m.Run(host, port)
}

func initModule() {
	if !app.Installed {
		return
	}

	config, err := setting.Read(app.AppConfig)
	if err != nil {
		logger.Fatal("读取应用配置失败", err)
	}
	app.Setting = config

	// 初始化DB
	models.Db = models.CreateDb()

	// 版本升级
	upgradeIfNeed()

	// 初始化定时任务
	service.ServiceTask.Initialize()
}

// 解析端口
func parsePort(ctx *cli.Context) int {
	port := DefaultPort
	if ctx.IsSet("port") {
		port = ctx.Int("port")
	}
	if port <= 0 || port >= 65535 {
		port = DefaultPort
	}

	return port
}

func parseHost(ctx *cli.Context) string {
	if ctx.IsSet("host") {
		return ctx.String("host")
	}

	return "0.0.0.0"
}

func setEnvironment(ctx *cli.Context) {
	env := "prod"
	if ctx.IsSet("env") {
		env = ctx.String("env")
	}

	switch env {
	case "test":
		macaron.Env = macaron.TEST
	case "dev":
		macaron.Env = macaron.DEV
	default:
		macaron.Env = macaron.PROD
	}
}

// 捕捉信号
func catchSignal() {
	c := make(chan os.Signal)
	// todo 配置热更新, windows 不支持 syscall.SIGUSR1, syscall.SIGUSR2
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	for {
		s := <-c
		logger.Info("收到信号 -- ", s)
		switch s {
		case syscall.SIGHUP:
			logger.Info("收到终端断开信号, 忽略")
		case syscall.SIGINT, syscall.SIGTERM:
			shutdown()
		}
	}
}

// 应用退出
func shutdown() {
	defer func() {
		logger.Info("已退出")
		os.Exit(0)
	}()

	if !app.Installed {
		return
	}
	logger.Info("应用准备退出")
	// 停止所有任务调度
	logger.Info("停止定时任务调度")
	service.ServiceTask.WaitAndExit()
}

// 判断应用是否需要升级, 当存在版本号文件且版本小于app.VersionId时升级
func upgradeIfNeed() {
	currentVersionId := app.GetCurrentVersionId()
	// 没有版本号文件
	if currentVersionId == 0 {
		return
	}
	if currentVersionId >= app.VersionId {
		return
	}

	migration := new(models.Migration)
	logger.Infof("版本升级开始, 当前版本号%d", currentVersionId)

	migration.Upgrade(currentVersionId)
	app.UpdateVersionFile()

	logger.Infof("已升级到最新版本%d", app.VersionId)
}



// 数据库锁检测
func mysqlLockDetector() {
	warnSent := make(map[string]int,0)
	for {
		if ! app.Installed {
			time.Sleep(10 * time.Second)

			continue
		}
		//fmt.Println("test")
		time.Sleep(1 * time.Second)
		dbHost := new(models.MysqlHostLists)
		myLock := new(models.MysqlLocks)
		hosts, err := dbHost.GetHosts()
		if err != nil {
			fmt.Println(err)
			continue
		}
		for _, eachHost := range hosts {
			if warnSent[eachHost.Host] == 0 {
				warnSent[eachHost.Host] = 1
			}
			//fmt.Println(eachHost)
			locks, err := myLock.DetecMysqlLocks(eachHost.Host, eachHost.User, eachHost.Password, eachHost.Port)
			if err != nil {
				fmt.Println(err)
				continue
			}
			msg := notify.Message{
				"task_type":        int8(3),
				"task_receiver_id": 0,
				"name":             "MySQL 锁检测",
				"status":           "none",
				// "task_id":          taskModel.Id,
			}
			if len(locks) > warnStep && warnSent[eachHost.Host] == 1  {
					msg["output"] = fmt.Sprintf("警告： hostname: %v 锁记录数量当前为 %v 个 " ,eachHost.Host,len(locks))
					msg["name"] = fmt.Sprintf("警告： hostname: %v 锁记录数量超过%v个 " ,eachHost.Host,warnStep)
					notify.Push(msg)
				warnSent[eachHost.Host] = 2
			}
			if len(locks) > errStep && warnSent[eachHost.Host] == 2 {
				msg["output"] = fmt.Sprintf("警告： hostname: %v 锁记录数量 - %v 个 " ,eachHost.Host,len(locks))
				msg["name"] = fmt.Sprintf("警告： hostname: %v 锁记录数量超过%v个 " ,eachHost.Host,errStep)
				notify.Push(msg)
				warnSent[eachHost.Host] = 5
			}


			if len(locks) > 0 {
				outputString := ""
				for _, eachLock := range locks {
					rID, err := strconv.ParseInt(string(eachLock["request_mysql_ID"]), 10, 64)
					if err != nil {
						fmt.Println(err)
						continue
					}
					bID, err := strconv.ParseInt(string(eachLock["blocking_mysql_ID"]), 10, 64)
					if err != nil {
						fmt.Println(err)
						continue
					}
					lock, err := myLock.GetLock(rID, bID, eachHost.Host)
					if err != nil {
						fmt.Println(err)
						continue
					}
					if len(lock) == 0 {

						mysqlLock := models.MysqlLocks{
							HostName:              eachHost.Host,
							Status:                1,
							RequestMysqlThreadId:  rID,
							RequestCommand:        string(eachLock["request_command"]),
							BlockingMysqlThreadId: bID,
							BlockingCommand:       string(eachLock["blocking_command"]),
							LockIndex:             string(eachLock["lock_index"]),
							CreateTime:            time.Now(),
						}
						err = mysqlLock.Add()
						outputString = fmt.Sprintf(`{"time":"%v", "host":"%v","log":"MySQL thread ID:%v - Command: %v  blocked by  MySQL thread ID:%v - Command: %v  with index :  %v"}`,time.Now().Format("2006-01-02 15:04:05.000"), eachHost.Host, rID, string(eachLock["request_command"]), bID, string(eachLock["blocking_command"]), string(eachLock["lock_index"]))
						// send Warn
						//webHook := WebHook{}
						//go webHook.Send(msg)
						err = myLock.WriteLogToFile(outputString + "\n",lockLogFD)
						if err != nil {
							fmt.Println(err)
							continue
						}


						if err != nil {
							fmt.Println(err)
							continue
						}

					}


				}


			} else {
				result, err := myLock.GetLocksByHost(eachHost.Host)
				if err != nil {
					fmt.Print(err)
					continue
				}
				if len(result) > 0 {
					mysqlLock := models.MysqlLocks{
						Status: 0,
					}
					affectRows, err := mysqlLock.ChangeLockStatus(eachHost.Host)
					if err != nil {
						fmt.Println(err)
						continue
					}
					fmt.Println("Affect rows: ", affectRows)
					if affectRows > 5 {
						// send warn recover
						msg["output"] = fmt.Sprintf("Host: %v 锁已全部释放", eachHost.Host)
						notify.Push(msg)
						warnSent[eachHost.Host] = 1
					}

				}

			}

		}

	}
}
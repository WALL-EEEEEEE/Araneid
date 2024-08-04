package main

import (
	"bufio"
	"flag"
	"net"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

var port = flag.String("port", "8080", "port socket server listen")      //websocket port
var host = flag.String("host", "127.0.0.1", "host socket server listen") //websocket port
var count = flag.Int("count", 3000, "mssage count will generated for sending")
var cost = flag.Int("cost", 60, "seconds for sending all generated message")
var loglevel = flag.String("loglevel", "INFO", "loglevel, default: INFO")

var exit chan bool = make(chan bool)
var conns chan net.Conn = make(chan net.Conn, 100)

var msgload []string

type option struct {
	len   int
	count int
	cost  int
}

var msgLoadOpt = option{
	len:   20,
	count: 3000,
	cost:  60,
}

func getLogLevel(level string) log.Level {
	loglevel_map := map[string]log.Level{
		"DEBUG":   log.DebugLevel,
		"INFO":    log.InfoLevel,
		"WARNING": log.WarnLevel,
		"ERROR":   log.ErrorLevel,
	}
	loglevel, ok := loglevel_map[level]
	if !ok {
		return log.InfoLevel
	}
	return loglevel
}

func msgLoadGenerate(args option) {
	msgload = make([]string, 0, args.count)
	for i := 0; i < args.count; i++ {
		msgload = append(msgload, strconv.Itoa(i+1)+"\n")
	}
}

func serveConn(conn *net.Conn) {
	log.Debugf("接受连接: %s", (*conn).RemoteAddr().String())
	conns <- *conn
	start := time.Now().Unix()
	msgLoadGenerate(msgLoadOpt)
	defer func() {
		<-conns
		if len(conns) < 1 {
			close(exit)
		}
	}()
	slice := int(msgLoadOpt.count / msgLoadOpt.cost)
	writer := bufio.NewWriter(*conn)
	for i := 0; i <= len(msgload); {
		if i+slice > len(msgload) {
			break
		}
		msg_slice := msgload[i : i+slice]
		i = i + slice
		for _, msg := range msg_slice {
			_, err := writer.Write([]byte(msg))
			if err != nil {
				(*conn).Close()
				log.Debugf("关闭连接：%s", (*conn).RemoteAddr().String())
				return
			}
			writer.Flush()
		}
		time.Sleep(time.Second)
	}
	last_slice := msgload[slice*msgLoadOpt.cost:]
	for _, msg := range last_slice {
		_, err := writer.Write([]byte(msg))
		if err != nil {
			(*conn).Close()
			log.Debugf("关闭连接：%s", (*conn).RemoteAddr().String())
			return
		}
		writer.Flush()
	}
	end := time.Now().Unix()
	cost := end - start
	log.Debugf("发送消息数: %d 条, 总耗时: %d 秒", len(msgload), cost)
	for {
		_, err := bufio.NewReader(*conn).ReadBytes('\n')
		if err != nil {
			(*conn).Close()
			log.Debugf("关闭连接：%s", (*conn).RemoteAddr().String())
			return
		}
	}
}

func startServe(addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Infof("无法监听，%s", err.Error())
		close(exit)
		return
	}
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Errorf("无法接受请求, %s", err.Error())
			close(exit)
			return
		}
		go serveConn(&conn)
	}
}

func main() {
	flag.Parse()
	level := getLogLevel(*loglevel)
	log.SetLevel(level)
	msgLoadOpt.count = *count
	msgLoadOpt.cost = *cost
	addr := *host + ":" + *port
	log.Infof("开启Socket服务(监听：%s, 发送消息数: %d 条, 耗时: %d 秒)", addr, *count, *cost)
	go startServe(addr)
	<-exit
	log.Infof("退出Socket服务(监听: %s, 发送消息数: %d, 耗时: %d 秒)", addr, *count, *cost)
}

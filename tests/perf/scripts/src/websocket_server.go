package main

import (
	"flag"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

var port = flag.String("port", "8080", "port websocket server listen")      //websocket port
var host = flag.String("host", "127.0.0.1", "host websocket server listen") //websocket port
var count = flag.Int("count", 3000, "mssage count will generated for sending")
var cost = flag.Int("cost", 60, "seconds for sending all generated message")
var loglevel = flag.String("loglevel", "INFO", "loglevel, default: INFO")

var conns = make(chan *websocket.Conn, 100)
var exit chan bool = make(chan bool)
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 0,
}

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

func serveWs(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	log.Debugf("接受连接: %s", conn.RemoteAddr().String())
	if err != nil {
		log.Error(err)
		return
	}
	conns <- conn
	start := time.Now().Unix()
	msgLoadGenerate(msgLoadOpt)
	defer func() {
		<-conns
		if len(conns) < 1 {
			close(exit)
		}
	}()
	slice := int(msgLoadOpt.count / msgLoadOpt.cost)
	for i := 0; i <= len(msgload); {
		if i+slice > len(msgload) {
			break
		}
		msg_slice := msgload[i : i+slice]
		i = i + slice
		for _, msg := range msg_slice {
			err := conn.WriteMessage(websocket.TextMessage, []byte(msg))
			if err != nil {
				conn.Close()
				log.Debugf("关闭连接：%s", conn.RemoteAddr().String())
				return
			}
		}
		time.Sleep(time.Second)
	}
	last_slice := msgload[slice*msgLoadOpt.cost:]
	for _, msg := range last_slice {
		err = conn.WriteMessage(websocket.TextMessage, []byte(msg))
		if err != nil {
			conn.Close()
			log.Debugf("关闭连接：%s", conn.RemoteAddr().String())
			return
		}
	}
	end := time.Now().Unix()
	cost := end - start
	log.Debugf("发送消息数: %d 条, 总耗时: %d 秒", len(msgload), cost)
	for {
		_, _, err := conn.NextReader()
		if err != nil {
			conn.Close()
			log.Debugf("关闭连接：%s", conn.RemoteAddr().String())
			return
		}
	}

}

func main() {
	flag.Parse()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		serveWs(w, r)
	})
	level := getLogLevel(*loglevel)
	log.SetLevel(level)
	msgLoadOpt.count = *count
	msgLoadOpt.cost = *cost
	addr := *host + ":" + *port
	log.Infof("开启WebSocket服务(发送消息数: %d 条, 耗时: %d 秒)", *count, *cost)
	go http.ListenAndServe(addr, nil)
	<-exit
	log.Infof("退出WebSocket服务(发送消息数: %d, 耗时: %d 秒)", *count, *cost)
}

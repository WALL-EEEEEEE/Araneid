package main

import (
	"flag"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

var port = flag.String("port", "8080", "port websocket server listen")      //websocket port
var host = flag.String("host", "127.0.0.1", "host websocket server listen") //websocket port
var loglevel = flag.String("loglevel", "INFO", "loglevel, default: INFO")
var json = flag.String("json", "", "json response content") //websocket port
var text = flag.String("text", "", "text response content") //websocket port
var count = flag.Int("count", 3000, "mssage count will generated for sending")
var cost int = 0
var exit chan bool = make(chan bool)

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
func ServeHttp(w http.ResponseWriter, r *http.Request) {
	req_start := time.Now().Unix()
	if *json != "" {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(*json))
	} else {
		w.Write([]byte(*text))
	}
	cost += int(time.Now().Unix()) - int(req_start)
}

func main() {
	flag.Parse()
	http.HandleFunc("/", ServeHttp)
	level := getLogLevel(*loglevel)
	log.SetLevel(level)
	addr := *host + ":" + *port
	log.Infof("开启Http服务(监听： %s)", addr)
	http.ListenAndServe(addr, nil)
}

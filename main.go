package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/eugenechertikhin/mqtt/broker"
	"github.com/eugenechertikhin/mqtt/db"
)

var (
	cert      = flag.String("cert", "broker.crt", "path to broker certificate")
	key       = flag.String("key", "broker.key", "path to broker private key")
	debug     = flag.Bool("debug", false, "print debuging hex dumps")
	allowAnon = flag.Bool("allow-anonymous", false, "allow clients to connect without username/password")
)

func main() {
	flag.Parse()
	log.Println("starting broker")

	log.Println("initialize database")
	if err := db.Open("mqtt.db"); err != nil {
		panic(err)
	}

	server := broker.NewListener(*debug, *key, *cert, *allowAnon)
	if server == nil {
		log.Panic("error start listener")
	}

	server.Manage()

	finish := make(chan os.Signal, 1)
	signal.Notify(finish, syscall.SIGINT, syscall.SIGTERM)
	<-finish
	log.Println("finished")
}

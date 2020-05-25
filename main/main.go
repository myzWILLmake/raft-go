package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/myzWILLmake/raft-go"

	"github.com/spf13/viper"
)

type Server struct {
	Id      int    `json:"id"`
	Address string `json:"address"`
	Client  string `json:"client"`
}

type X struct {
	Nodes []Server
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Invalid augments")
		return
	}
	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal("Invalid id")
		return
	}
	viper.SetConfigName("config.json")
	viper.AddConfigPath(".")
	viper.SetConfigType("json")
	err = viper.ReadInConfig()
	if err != nil {
		fmt.Printf("config file error: %s\n", err)
		os.Exit(1)
	}
	var x X
	viper.Unmarshal(&x)
	addresses := make([]string, len(x.Nodes))
	clientAddr := x.Nodes[id].Client
	for _, node := range x.Nodes {
		addresses[node.Id] = node.Address
	}

	wg := &sync.WaitGroup{}
	raft.RunServer(id, addresses, clientAddr, wg)

	wg.Wait()
}

package raft

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

type peerWrapper struct {
	client  *rpc.Client
	address string
}

func (c *peerWrapper) Call(serviceMethod string, args interface{}, reply interface{}) error {
	var err error
	if c.client == nil {
		err = errors.New("")
	} else {
		err = c.client.Call(serviceMethod, args, reply)
	}
	if err != nil {
		var errdial error
		c.client, errdial = rpc.DialHTTP("tcp", c.address)
		if errdial != nil {
			return errdial
		}
		err = c.client.Call(serviceMethod, args, reply)
	}
	return err
}

func createPeers(id int, addresses []string) []peerWrapper {
	peers := make([]peerWrapper, len(addresses))
	for i := 0; i < len(addresses); i++ {
		peers[i].client = nil
		peers[i].address = addresses[i]
	}

	return peers
}

func RunServer(id int, addresses []string, clientAddr string, wg *sync.WaitGroup) {
	clientCh := make(chan ClientMsg, 1024)
	raft := RunRaft(id, addresses, clientCh)
	if raft != nil {
		RunClientServer(clientAddr, clientCh, raft, wg)
	}
}

func RunRaft(id int, addresses []string, clientCh chan ClientMsg) *Raft {
	peers := createPeers(id, addresses)
	raft := MakeRaft(peers, id, clientCh)
	rpc.Register(raft)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", addresses[id])
	if err != nil {
		log.Fatal("listen error:", err)
		return nil
	}

	go http.Serve(l, nil)
	return raft
}

func RunClientServer(clientAddr string, notifyCh chan ClientMsg, raftServer *Raft, wg *sync.WaitGroup) {
	cs := ClientServer{}
	tcpListener, err := net.Listen("tcp", clientAddr)
	if err != nil {
		log.Fatal("clientSrv listen error:", clientAddr, err)
	}
	cs.tcpl = tcpListener
	cs.clients = make(map[string]net.Conn)
	cs.addr = clientAddr
	cs.notifyCh = notifyCh
	cs.raftServer = raftServer
	wg.Add(1)
	go cs.run(wg)
}

package raft

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

type ClientServer struct {
	addr       string
	tcpl       net.Listener
	clients    map[string]net.Conn
	notifyCh   chan ClientMsg
	raftServer *Raft
}

func (cs *ClientServer) handleNotify() {
	for true {
		msg := <-cs.notifyCh
		if msg.CmdValid {
			msgtext := "Cmd { " + msg.Cmd.(string) + " } , " +
				"Term " + strconv.Itoa(msg.CmdTerm) +
				", Index " + strconv.Itoa(msg.CmdIndex)
			for _, tcpConn := range cs.clients {
				tcpConn.Write([]byte(msgtext))
			}
		}
	}
}

func (cs *ClientServer) handleEcho(conn net.Conn, args []string) {
	msg := strings.Join(args, " ")
	conn.Write([]byte(msg))
}

func (cs *ClientServer) handleCmd(conn net.Conn, args []string) {
	if len(args) < 1 {
		return
	}

	cmd := args[0]
	idx, term, ok := cs.raftServer.start(cmd)
	if !ok {
		raftInfo := cs.raftServer.getServerInfo()
		msg := fmt.Sprintf("Not Leader. The Leader is: %d", raftInfo["leaderId"].(int))
		conn.Write([]byte(msg))
		return
	}
	msg := fmt.Sprintf("Cmd sent! Index[%d], Term[%d]", idx, term)
	conn.Write([]byte(msg))
}

func (cs *ClientServer) tcpPipe(conn net.Conn) {
	buf := make([]byte, 4096)
	for {
		cnt, err := conn.Read(buf)
		if err != nil {
			return
		}
		msg := string(buf[:cnt])
		words := strings.Fields(msg)
		if len(words) > 0 {
			args := words[1:]
			switch words[0] {
			case "cmd":
				cs.handleCmd(conn, args)
			case "kill":
				conn.Write([]byte("Kill Server..."))
				conn.Close()
				cs.tcpl.Close()
			case "quit":
				conn.Write([]byte("Bye!"))
				conn.Close()
			case "echo":
				cs.handleEcho(conn, args)
			}
		}
	}
}

func (cs *ClientServer) run(wg *sync.WaitGroup) {
	go cs.handleNotify()
	for true {
		tcpConn, err := cs.tcpl.Accept()
		if err != nil {
			break
		}
		cs.clients[tcpConn.RemoteAddr().String()] = tcpConn
		go cs.tcpPipe(tcpConn)
	}
	wg.Done()
}

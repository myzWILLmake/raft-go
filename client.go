package raft

import (
	"fmt"
	"net"
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
			msgtext := fmt.Sprintf("Cmd commited! Cmd [%s], Term [%d], Index [%d]\n", msg.Cmd.(string), msg.CmdTerm, msg.CmdIndex)
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
		msg := fmt.Sprintf("Not Leader. The Leader is: %d\n", raftInfo["leaderId"].(int))
		conn.Write([]byte(msg))
		return
	}
	msg := fmt.Sprintf("Cmd sent! Index[%d], Term[%d]\n", idx, term)
	conn.Write([]byte(msg))
}

func (cs *ClientServer) handlePrint(conn net.Conn, args []string) {
	raftInfo := cs.raftServer.getServerInfo()
	var serverStateMsg string
	if len(args) < 1 {
		rfState := raftInfo["state"].(raftState)
		rfStateStr := getStateStr(rfState)
		serverStateMsg = fmt.Sprintf(`
Raft server state:
    id:          %d
    state:       %s
    leaderId:    %d
    currentTerm: %d
    logIndex:    %d
    commitIndex: %d
`, raftInfo["id"].(int), rfStateStr, raftInfo["leaderId"].(int), raftInfo["currentTerm"].(int),
			raftInfo["logIndex"].(int), raftInfo["commitIndex"].(int))
		logs := raftInfo["logs"].([]LogEntry)
		serverStateMsg += "    The last 5 logs are:\n        "
		for i := 0; i < len(logs); i++ {
			serverStateMsg += logs[i].Command.(string) + " | "
		}
		serverStateMsg += "\n"
	} else {
		key := args[0]
		data, ok := raftInfo[key]
		if !ok {
			serverStateMsg = "Invalid arguments!"
		} else {
			switch key {
			case "state":
				serverStateMsg = getStateStr(data.(raftState))
			case "logs":
				logs := data.([]LogEntry)
				serverStateMsg = "The last 5 logs are:\n    "
				for i := 0; i < len(logs); i++ {
					serverStateMsg += logs[i].Command.(string) + " | "
				}
				serverStateMsg += "\n"
			default:
				serverStateMsg = fmt.Sprintf("%d\n", data.(int))
			}
		}
	}
	conn.Write([]byte(serverStateMsg))
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
				conn.Write([]byte("Kill Server...\n"))
				conn.Close()
				cs.tcpl.Close()
			case "print":
				cs.handlePrint(conn, args)
			case "quit":
				conn.Write([]byte("Bye!\n"))
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

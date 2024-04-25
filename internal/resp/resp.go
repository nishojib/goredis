package resp

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"nishojib/goredis/internal/parser"
	"nishojib/goredis/internal/rdb"
	"nishojib/goredis/internal/store"
	"nishojib/goredis/internal/types"
)

type RESPNode struct {
	MasterReplID     string
	MasterReplOffset int
	IsSlave          bool
	Role             string
	SlaveConns       *Connections
	handshakeDone    bool
	bytesProc        bytesProcessed
	RDBFile          RDBFile
	cache            store.Store[types.Item]
	streamCache      store.Store[types.Stream]
}

type RDBFile struct {
	Dir        string
	DBFilename string
}

type bytesProcessed struct {
	all     int
	currReq int
	mutex   sync.Mutex
}

type Connections struct {
	connWait    net.Conn
	numWait     int
	numAck      int
	waitChannel chan bool
	conns       []net.Conn
	mutex       sync.Mutex
}

func New(
	masterReplID string,
	masterReplOffset int,
	role string,
	rdbFile RDBFile,
) *RESPNode {
	return &RESPNode{
		MasterReplID:     masterReplID,
		MasterReplOffset: masterReplOffset,
		IsSlave:          role == "slave",
		SlaveConns: &Connections{
			connWait:    &net.TCPConn{},
			numWait:     0,
			numAck:      0,
			waitChannel: make(chan bool, 1),
			conns:       make([]net.Conn, 0),
		},
		Role:        role,
		RDBFile:     rdbFile,
		cache:       store.New[types.Item](),
		streamCache: store.New[types.Stream](),
	}
}

func (rn *RESPNode) ConnectToMaster(masterHost string, masterPort string) {
	conn, err := net.Dial("tcp", net.JoinHostPort(masterHost, masterPort))
	if err != nil {
		fmt.Printf("failed to connect to the master node: %s", err.Error())
		return
	}

	err = sendResponse(conn, parser.EncodeArray([]string{"ping"}))
	if err != nil {
		fmt.Printf("error sending data to the master node: %s", err.Error())
		return
	}

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Printf("error receiving data from the master node: %s", err.Error())
		return
	}
	fmt.Printf("received from master: %#v\n", string(buf[:n]))

	err = sendResponse(conn, parser.EncodeArray([]string{"REPLCONF", "listening-port", "6380"}))
	if err != nil {
		fmt.Printf(
			"error sending REPLCONF command (listening-port) to the master node %s",
			err.Error(),
		)
		return
	}
	n, err = conn.Read(buf)
	if err != nil {
		fmt.Printf("error receiving data from the master node: %s", err.Error())
		return
	}
	fmt.Printf("received from master: %#v\n", string(buf[:n]))

	err = sendResponse(conn, parser.EncodeArray([]string{"REPLCONF", "capa", "psync2"}))
	if err != nil {
		fmt.Printf(
			"error sending REPLCONF command (capa) to the master node %s",
			err.Error(),
		)
		return
	}
	n, err = conn.Read(buf)
	if err != nil {
		fmt.Printf("error receiving data from the master node: %s", err.Error())
		return
	}
	fmt.Printf("received from master: %#v\n", string(buf[:n]))

	err = sendResponse(conn, parser.EncodeArray([]string{"PSYNC", "?", "-1"}))
	if err != nil {
		fmt.Printf("error sending PSYNC command to the master node %s", err.Error())
		return
	}

	go rn.HandleClient(conn)
}

func (rn *RESPNode) HandleClient(conn net.Conn) {
	defer conn.Close()

	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}
			fmt.Printf("error handling connection: %s", err.Error())
			return
		}

		fmt.Printf("query: %#v\n", string(buf[:n]))

		commands, err := rn.parseRequest(buf[:n])
		if err != nil {
			fmt.Printf("error parsing request: %s", err.Error())
			return
		}

		for _, command := range commands {
			if command.Name == "" {
				fmt.Println("Empty command name")
				continue
			}

			err = rn.processRequest(conn, command)
			if err != nil {
				fmt.Printf("error processing request: %s", err.Error())
				return
			}
		}
	}
}

func (rn *RESPNode) Restore() error {
	values, err := rdb.ParseRDBFile(rn.RDBFile.Dir, rn.RDBFile.DBFilename)
	if err != nil {
		return err
	}
	fmt.Println("len of values: ", len(values))

	for _, el := range values {
		if el.Item.Expiry != -1 {
			go rn.removeKeyAfter(el.Name, el.Item.Expiry)
		}

		rn.cache.Store(el.Name, el.Item)
		fmt.Println("key", el.Name, "val", el.Item.Value)
	}

	return nil
}

func (rn *RESPNode) propToSlaves(payload string) error {
	for _, slaveConn := range rn.SlaveConns.conns {
		err := sendResponse(slaveConn, payload)
		if err != nil {
			return err
		}
	}
	return nil
}

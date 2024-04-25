package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"nishojib/goredis/internal/resp"
)

func main() {
	var port string
	flag.StringVar(&port, "port", "6379", "Port of the TCP Server")

	var replicaOf string
	flag.StringVar(&replicaOf, "replicaof", "", "Port of the Replica")

	var rdbDir string
	flag.StringVar(&rdbDir, "dir", "", "The path to the directory where the RDB file is stored")

	var rdbFilename string
	flag.StringVar(&rdbFilename, "dbfilename", "", "The name of the RDB file")

	flag.Parse()

	var role string
	if replicaOf == "" {
		role = "master"
	} else {
		role = "slave"
	}

	rn := resp.New("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", 0, role, resp.RDBFile{
		Dir:        rdbDir,
		DBFilename: rdbFilename,
	})

	if rn.RDBFile.Dir != "" && rn.RDBFile.DBFilename != "" {
		if err := rn.Restore(); err != nil {
			fmt.Println("error: ", err)
		}
	}

	if role == "slave" {
		masterHost, masterPort := replicaOf, flag.Args()[len(flag.Args())-1]
		go rn.ConnectToMaster(masterHost, masterPort)
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer func() {
		if err := l.Close(); err != nil {
			fmt.Println("Error closing the listener: ", err.Error())
			os.Exit(1)
		}
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("error accepting connection: ", err.Error())
			continue
		}

		go rn.HandleClient(conn)
	}

}

package resp

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net"
	"strconv"
	"time"

	"nishojib/goredis/internal/parser"
	"nishojib/goredis/internal/types"
)

const (
	PING     = "ping"
	ECHO     = "echo"
	SET      = "set"
	GET      = "get"
	INFO     = "info"
	REPLCONF = "replconf"
	PSYNC    = "psync"
	WAIT     = "wait"
	CONFIG   = "config"
	KEYS     = "keys"
	TYPE     = "type"
	XADD     = "xadd"
)

func (rn *RESPNode) parseRequest(query []byte) ([]types.Command, error) {
	var commands []types.Command

	offset := 0
	qLength := len(query)

	for offset < qLength {
		firstEl := query[offset]

		switch firstEl {
		case '*':
			firstIndex := offset
			tks := bytes.SplitN(query[firstIndex:], []byte("\r\n"), 2)
			count, err := strconv.Atoi(string(tks[0][1:]))
			if err != nil {
				fmt.Printf(
					"wrong request. error converting to int in func parseRequest: %#v\n",
					string(tks[0][1:]),
				)
				continue
			}
			tokens := bytes.SplitN(tks[1], []byte("\r\n"), count*2+1)
			offset = qLength - len(tokens[count*2])

			name, args, err := parser.DecodeArray(
				bytes.Split(query[firstIndex:offset], []byte("\r\n")),
			)
			if err != nil {
				return []types.Command{}, err
			}

			n := len(query[firstIndex:offset])

			if rn.IsSlave {
				rn.bytesProc.mutex.Lock()
				rn.bytesProc.all += n
				rn.bytesProc.currReq = n
				rn.bytesProc.mutex.Unlock()
			}

			commands = append(commands, types.Command{Name: name, Args: args})
		case '$':
			firstIndex := offset
			tks := bytes.SplitN(query[firstIndex:], []byte("\r\n"), 2)
			count, err := strconv.Atoi(string(tks[0][1:]))
			if err != nil {
				fmt.Printf(
					"wrong request. error converting to int in func parseRequest: %#v\n",
					string(tks[0][1:]),
				)
				continue
			}

			pos := offset + len(tks[0]) + len([]byte("\r\n"))
			sl := query[pos : pos+count]
			binData, _ := base64.StdEncoding.DecodeString(emptyRDB)
			if bytes.Equal(sl, binData) {
				fmt.Println("RDB File...")

				rn.handshakeDone = true
				rn.bytesProc.mutex.Lock()
				rn.bytesProc.all = 0
				rn.bytesProc.currReq = 0
				rn.bytesProc.mutex.Unlock()
			}

			offset = pos + count

		default:
			offset++
		}
	}

	return commands, nil
}

func (rn *RESPNode) processRequest(conn net.Conn, command types.Command) error {
	args := command.Args

	switch command.Name {
	case PING:
		if rn.IsSlave && rn.handshakeDone {
			return nil
		}
		return rn.handlePing(conn)

	case ECHO:
		if rn.IsSlave && rn.handshakeDone {
			return nil
		}
		return rn.handleEcho(conn, string(args[0]))

	case SET:
		var expMillSec int64 = -1
		if len(args) > 2 {
			expMills, err := strconv.Atoi(string(args[3]))
			if err != nil {
				return err
			}

			expMillSec = int64(expMills)
		}

		return rn.handleSet(conn, string(args[0]), string(args[1]), expMillSec)

	case GET:
		return rn.handleGet(conn, string(args[0]))

	case INFO:
		arg := ""
		if len(args) > 0 {
			arg = string(args[0])
		}

		return rn.handleInfo(conn, arg)

	case REPLCONF:
		return rn.handleReplconf(conn, string(args[0]))

	case PSYNC:
		if rn.IsSlave && rn.handshakeDone {
			return nil
		}
		return rn.handlePsync(conn)

	case WAIT:
		numReplicas, err := strconv.ParseInt(string(args[0]), 10, 64)
		if err != nil {
			return err
		}

		timeout, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return err
		}

		return rn.handleWait(conn, numReplicas, timeout)

	case CONFIG:
		return rn.handleConfig(conn, string(args[1]))

	case KEYS:
		return rn.handleKeys(conn, args[0])

	case TYPE:
		return rn.handleType(conn, string(args[0]))

	case XADD:
		return rn.handleXAdd(conn, args)

	default:
		if rn.IsSlave && rn.handshakeDone {
			return nil
		}
		return rn.handleUnknown(conn)
	}
}

func (rn *RESPNode) removeKeyAfter(key string, expMillSec int64) {
	timer := time.NewTimer(time.Duration(expMillSec) * time.Millisecond)
	<-timer.C
	rn.cache.Delete(key)
}

package resp

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"nishojib/goredis/internal/parser"
	"nishojib/goredis/internal/rdb"
	"nishojib/goredis/internal/types"
)

const emptyRDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

func (rn *RESPNode) handlePing(conn net.Conn) error {
	err := sendResponse(conn, parser.EncodeSimpleString("PONG"))
	if err != nil {
		return err
	}

	return nil
}

func (rn *RESPNode) handleEcho(conn net.Conn, payload string) error {
	err := sendResponse(conn, parser.EncodeBulkString(payload))
	if err != nil {
		return err
	}

	return nil
}

func (rn *RESPNode) handleSet(conn net.Conn, key string, value string, expMillSec int64) error {
	if expMillSec != -1 {
		go rn.removeKeyAfter(key, expMillSec)
	}

	rn.cache.Store(key, types.NewItem(value, "string", expMillSec))

	if !rn.IsSlave {
		err := sendResponse(conn, parser.EncodeSimpleString("OK"))
		if err != nil {
			return err
		}

		err = rn.propToSlaves(parser.EncodeArray([]string{"SET", key, string(value)}))
		if err != nil {
			fmt.Println("propagated a command. got an error", err)
			return err
		}

		fmt.Println("propagates a command")
	}

	if rn.IsSlave {
		fmt.Println("got a command from MASTER")
	}

	return nil
}

func (rn *RESPNode) handleGet(conn net.Conn, key string) error {
	item := rn.getItemFromStore(key)

	if item.Value == "" {
		err := sendResponse(conn, parser.EncodeBulkString(""))
		if err != nil {
			return err
		}
	} else {
		err := sendResponse(conn, parser.EncodeBulkString(string(item.Value)))
		if err != nil {
			return err
		}
	}
	return nil
}

func (rn *RESPNode) handleInfo(conn net.Conn, arg string) error {
	if arg != "" {
		switch strings.ToLower(arg) {
		case "replication":
			payload := fmt.Sprintf(
				"role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d",
				rn.Role,
				rn.MasterReplID,
				rn.MasterReplOffset,
			)

			err := sendResponse(conn, parser.EncodeBulkString(payload))
			if err != nil {
				return err
			}
		}
	} else {
		err := sendResponse(conn, parser.EncodeBulkString("role:master"))
		if err != nil {
			return err
		}
	}

	return nil
}

func (rn *RESPNode) handleReplconf(conn net.Conn, arg string) error {
	switch strings.ToLower(arg) {
	case "ack":
		if rn.IsSlave && rn.handshakeDone {
			return nil
		}

		rn.SlaveConns.numAck++

		if rn.SlaveConns.numAck >= rn.SlaveConns.numWait {
			rn.SlaveConns.waitChannel <- true
			sendResponse(
				rn.SlaveConns.connWait,
				parser.EncodeInteger(fmt.Sprintf("%d", rn.SlaveConns.numAck)),
			)
			rn.SlaveConns.numAck = 0
			rn.SlaveConns.numWait = 0
		}
	case "getack":
		bytesReceived := strconv.Itoa(rn.bytesProc.all - rn.bytesProc.currReq)
		err := sendResponse(
			conn,
			parser.EncodeArray([]string{"REPLCONF", "ACK", bytesReceived}),
		)
		if err != nil {
			return err
		}
	default:
		err := sendResponse(conn, parser.EncodeSimpleString("OK"))
		if err != nil {
			return err
		}
	}

	return nil
}

func (rn *RESPNode) handlePsync(conn net.Conn) error {
	rn.SlaveConns.mutex.Lock()
	defer rn.SlaveConns.mutex.Unlock()

	err := sendResponse(
		conn,
		parser.EncodeBulkString(fmt.Sprintf("FULLRESYNC %s 0", rn.MasterReplID)),
	)
	if err != nil {
		return err
	}

	rdbFile, err := parser.EncodeRDBFile(emptyRDB)
	if err != nil {
		return err
	}

	err = sendResponse(conn, rdbFile)
	if err != nil {
		return err
	}

	if !rn.IsSlave {
		rn.SlaveConns.conns = append(rn.SlaveConns.conns, conn)
	}

	return nil
}

func (rn *RESPNode) handleUnknown(conn net.Conn) error {
	err := sendResponse(conn, parser.EncodeSimpleString("Unknown command"))
	if err != nil {
		return err
	}

	return nil
}

func (rn *RESPNode) handleWait(conn net.Conn, numReplicas int64, timeout int64) error {
	rn.SlaveConns.numAck = 0
	rn.SlaveConns.connWait = conn

	for _, replica := range rn.SlaveConns.conns {
		if err := sendResponse(replica, parser.EncodeArray([]string{"REPLCONF", "GETACK", "*"})); err != nil {
			fmt.Printf("Error replicating: %v\n", err)
			continue
		}
	}

	if numReplicas == 0 {
		if err := sendResponse(conn, parser.EncodeInteger(fmt.Sprintf("%d", len(rn.SlaveConns.conns)))); err != nil {
			return err
		}
		return nil
	}

	rn.SlaveConns.numWait = int(numReplicas)

	go rn.waitTimeout(conn, timeout)
	return nil
}

func (rn *RESPNode) handleConfig(conn net.Conn, query string) error {
	switch query {
	case "dir":
		return sendResponse(
			conn,
			parser.EncodeArray([]string{"dir", rn.RDBFile.Dir}),
		)
	case "dbfilename":
		return sendResponse(
			conn,
			parser.EncodeArray([]string{"dbfilename", rn.RDBFile.DBFilename}),
		)
	default:
		return nil
	}
}

func (rn *RESPNode) handleKeys(conn net.Conn, query []byte) error {
	rdbValues, err := rdb.ParseRDBFile(rn.RDBFile.Dir, rn.RDBFile.DBFilename)
	if err != nil {
		return err
	}

	if bytes.Equal(query, []byte("*")) {
		if len(rdbValues) > 0 {
			names := []string{}

			for _, value := range rdbValues {
				names = append(names, value.Name)
			}

			return sendResponse(conn, parser.EncodeArray(names))
		}

		return sendResponse(conn, parser.EncodeBulkString(""))
	}

	return nil
}

func (rn *RESPNode) handleType(conn net.Conn, query string) error {
	item := rn.getItemFromStore(query)
	if item.Value == "" {
		_, ok := rn.streamCache.Load(query)
		if !ok {
			return sendResponse(conn, parser.EncodeSimpleString("none"))
		}
		return sendResponse(conn, parser.EncodeSimpleString("stream"))
	}

	return sendResponse(conn, parser.EncodeSimpleString(string(item.Type)))
}

func (rn *RESPNode) handleXAdd(conn net.Conn, args [][]byte) error {
	storeKey := args[0]
	streamKey := args[1]

	items := []types.StreamItem{}

	for i := 2; i < len(args); i += 2 {
		items = append(items, types.StreamItem{
			Key:   string(bytes.ToLower(args[i])),
			Value: string(bytes.ToLower(args[i+1])),
		})
	}

	if bytes.Equal(streamKey, []byte("0-0")) {
		return sendResponse(conn, parser.EncodeSimpleError(ErrGreaterThanZero.Error()))
	}

	if bytes.Equal(streamKey, []byte("*")) {
		currentMilliseconds := time.Now().UnixMilli()
		streamKey = []byte(fmt.Sprintf("%d-%d", currentMilliseconds, 0))
	}

	streamIdParts := strings.Split(string(streamKey), "-")
	streamMilliseconds := streamIdParts[0]
	streamSequence := streamIdParts[1]

	stream, ok := rn.streamCache.Load(string(storeKey))
	if !ok {
		if streamSequence == "*" {
			streamKey = []byte(fmt.Sprintf("%s-%d", streamMilliseconds, 1))
		}

		rn.streamCache.Store(string(storeKey), types.Stream{
			Entries: []types.StreamEntry{{ID: string(streamKey), Items: items}},
		})
		return sendResponse(conn, parser.EncodeBulkString(string(streamKey)))
	}

	lastEntry := stream.Entries[len(stream.Entries)-1]
	idParts := strings.Split(lastEntry.ID, "-")
	milliseconds := idParts[0]
	sequence := idParts[1]

	if streamMilliseconds > milliseconds {
		if streamSequence == "*" {
			streamKey = []byte(fmt.Sprintf("%s-%s", streamMilliseconds, "0"))
		}

		rn.streamCache.Store(string(storeKey), types.Stream{
			Entries: append(stream.Entries, types.StreamEntry{ID: string(streamKey), Items: items}),
		})
		return sendResponse(conn, parser.EncodeBulkString(string(streamKey)))
	}

	if streamMilliseconds == milliseconds {
		sequenceInt, err := strconv.Atoi(sequence)
		if err != nil {
			fmt.Println("Error converting sequence to int")
		}

		var streamSequenceInt int
		if streamSequence == "*" {
			streamSequenceInt = sequenceInt + 1
			streamKey = []byte(fmt.Sprintf("%s-%d", streamMilliseconds, streamSequenceInt))
		} else {
			streamSequenceInt, err = strconv.Atoi(streamSequence)
			if err != nil {
				fmt.Println("Error converting streamSequence to int")
			}
		}

		if streamSequenceInt > sequenceInt {
			rn.streamCache.Store(string(storeKey), types.Stream{
				Entries: append(
					stream.Entries,
					types.StreamEntry{ID: string(streamKey), Items: items},
				),
			})

			return sendResponse(conn, parser.EncodeBulkString(string(streamKey)))
		}
	}

	return sendResponse(conn, parser.EncodeSimpleError(ErrInvalidId.Error()))
}

func (rn *RESPNode) waitTimeout(conn net.Conn, timeout int64) {
	select {
	case <-rn.SlaveConns.waitChannel:
		break
	case <-time.After(time.Duration(timeout) * time.Millisecond):
		if rn.SlaveConns.numAck == 0 {
			sendResponse(conn, parser.EncodeInteger(fmt.Sprintf("%d", len(rn.SlaveConns.conns))))
		} else {
			sendResponse(conn, parser.EncodeInteger(fmt.Sprintf("%d", rn.SlaveConns.numAck)))
		}
	}
}

func (rn *RESPNode) getItemFromStore(key string) types.Item {
	itemVal, ok := rn.cache.Load(key)
	if !ok {
		return types.Item{}
	}

	if itemVal.Expiry != -1 && itemVal.Expiry < time.Now().UnixMilli() {
		return types.Item{}
	}

	return itemVal
}

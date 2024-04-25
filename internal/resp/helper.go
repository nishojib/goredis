package resp

import "net"

func sendResponse(conn net.Conn, payload string) error {
	_, err := conn.Write([]byte(payload))
	if err != nil {
		return err
	}
	return nil
}

package parser

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func EncodeSimpleString(value string) string {
	return fmt.Sprintf("+%s\r\n", value)
}

func EncodeSimpleError(error string) string {
	return fmt.Sprintf("-%s\r\n", error)
}

func EncodeInteger(value string) string {
	if len(value) == 0 {
		return "$-1\r\n"
	}

	return fmt.Sprintf(":%s\r\n", value)
}

func EncodeBulkString(value string) string {
	if len(value) == 0 {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
}

func EncodeArray(values []string) string {
	str := ""

	for _, value := range values {
		str = fmt.Sprintf("%s%s", str, EncodeBulkString(value))
	}

	return fmt.Sprintf("*%d\r\n%s", len(values), str)
}

func EncodeRDBFile(rdb string) (string, error) {
	binData, err := base64.StdEncoding.DecodeString(rdb)
	if err != nil {
		return "", errors.New("failed to parse RDB file")
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(binData)+2, binData), nil
}

func DecodeBulkString(tokens [][]byte) ([]byte, error) {
	header, content := tokens[0], tokens[1]

	length, err := strconv.Atoi(string(header[1:]))
	if err != nil {
		return []byte{}, errors.New(
			"wrong request. error converting to int in func decodeBulkString",
		)
	}

	if len(content) != length {
		return []byte{}, errors.New("wrong request. lengths are not equal in func decodeBulkString")
	}

	return content, nil
}

func DecodeArray(tokens [][]byte) (string, [][]byte, error) {
	header := string(tokens[0])

	n, err := strconv.Atoi(header[1:])
	if err != nil {
		fmt.Println("header: ", header)
		return "", [][]byte{}, fmt.Errorf(
			"wrong request. error converting to int in func decodeArray: %#v",
			header[1:],
		)
	}

	var name string
	var args [][]byte
	for i := range n {
		val, err := DecodeBulkString(tokens[2*i+1 : 2*i+3])
		if err != nil {
			return "", [][]byte{}, err
		}

		if i == 0 {
			name = strings.ToLower(string(val))
		} else {
			args = append(args, val)
		}
	}

	return name, args, nil
}

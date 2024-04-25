package rdb

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"

	"nishojib/goredis/internal/types"
)

type RDBValue struct {
	Name string
	Item types.Item
}

func ParseRDBFile(dir string, filename string) ([]RDBValue, error) {
	file, err := os.Open(fmt.Sprintf("%s/%s", dir, filename))
	if err != nil {
		return []RDBValue{}, fmt.Errorf(
			"mistake reading an rdbfile: %s/%s with error: %s",
			dir,
			filename,
			err.Error(),
		)
	}
	defer file.Close()

	// read the file
	reader := bufio.NewReader(file)

	// read the REDIS string, 5 bytes
	redisStr := make([]byte, 5)
	_, err = reader.Read(redisStr)
	if err != nil {
		return []RDBValue{}, fmt.Errorf("error reading redis string: %s", err.Error())
	}

	// confirm that it is a valid RDB file
	if string(redisStr) != "REDIS" {
		return []RDBValue{}, errors.New("invalid RDB file")
	}

	// read the version, 4 bytes
	version := make([]byte, 4)
	_, err = reader.Read(version)
	if err != nil {
		return []RDBValue{}, fmt.Errorf("error reading version: %s", err.Error())
	}

	if _, err := strconv.Atoi(string(version)); err != nil {
		return []RDBValue{}, errors.New("invalid version")
	}

	values := []RDBValue{}

	dbNumber := -1

	for reader.Buffered() > 0 {
		// read the database header, 1 byte
		dbHeader := make([]byte, 1)
		_, err = reader.Read(dbHeader)
		if err != nil {
			return []RDBValue{}, fmt.Errorf("error reading db header: %s", err.Error())
		}

		fmt.Println("DB header: ", hex.EncodeToString(dbHeader))

		switch dbHeader[0] {
		case 0xFE:
			dbN, _, err := Length(reader)
			if err != nil {
				return []RDBValue{}, fmt.Errorf("error reading db number: %s", err.Error())
			}

			dbNumber = dbN
		case 0xFF:
			// End of the file
			// 8 bytes checksum
			checksum := make([]byte, 8)
			n, err := reader.Read(checksum)
			if err != nil || n != 8 {
				return []RDBValue{}, fmt.Errorf("error reading checksum: %s", err.Error())
			}
		case 0xFD:
			// after database header, there should be a 4 byte integer
			expiryTime := make([]byte, 4)
			_, err := reader.Read(expiryTime)
			if err != nil {
				return []RDBValue{}, fmt.Errorf("error reading expiry time: %s", err.Error())
			}

			_, err = Value(reader)
			if err != nil {
				return []RDBValue{}, fmt.Errorf("error reading value type: %s", err.Error())
			}

			key, err := ReadRedisString(reader)
			if err != nil {
				return []RDBValue{}, fmt.Errorf("error reading key: %s", err.Error())
			}

			val, err := ReadRedisString(reader)
			if err != nil {
				return []RDBValue{}, fmt.Errorf("error reading value: %s", err.Error())
			}

			values = append(values, RDBValue{
				Name: key,
				Item: types.NewItem(val, "string", int64(binary.LittleEndian.Uint64(expiryTime))),
			})

		case 0xFC:
			// next byte is 8 byte unsigned long
			milliseconds := make([]byte, 8)
			_, err := reader.Read(milliseconds)
			if err != nil {
				return []RDBValue{}, fmt.Errorf("error reading milliseconds: %s", err.Error())
			}

			// skip the next byte
			_, err = reader.Discard(1)
			if err != nil {
				return []RDBValue{}, fmt.Errorf("error skipping byte: %s", err.Error())
			}

			key, err := ReadRedisString(reader)
			if err != nil {
				return []RDBValue{}, fmt.Errorf("error reading key: %s", err.Error())
			}

			val, err := ReadRedisString(reader)
			if err != nil {
				return []RDBValue{}, fmt.Errorf("error reading value: %s", err.Error())
			}

			values = append(values, RDBValue{
				Name: key,
				Item: types.Item{
					Value:  val,
					Type:   "string",
					Expiry: int64(binary.LittleEndian.Uint64(milliseconds)),
				},
			})

		case 0xFB:
			// First byte is the keys count
			keysCount := make([]byte, 1)
			_, err := reader.Read(keysCount)
			if err != nil {
				return []RDBValue{}, fmt.Errorf("error reading keys count: %s", err.Error())
			}

			// Byte after keys count is the expiry time in milliseconds
			expiryTime := make([]byte, 1)
			_, err = reader.Read(expiryTime)
			if err != nil {
				fmt.Println("Error reading expiry time", err)
				return []RDBValue{}, fmt.Errorf("error reading expiry time: %s", err.Error())
			}

		case 0xFA:
			// after the database header, there should be 2 strings, one for key and one for value
			key, err := ReadRedisString(reader)
			if err != nil {
				return []RDBValue{}, fmt.Errorf("error reading key: %s", err.Error())
			}

			val, err := ReadRedisString(reader)
			if err != nil {
				return []RDBValue{}, fmt.Errorf("error reading value: %s", err.Error())
			}

			fmt.Println("key", key, "value", val)
		case byte(dbNumber):
			key, err := ReadRedisString(reader)
			if err != nil {
				return []RDBValue{}, fmt.Errorf("error reading key: %s", err.Error())
			}

			val, err := ReadRedisString(reader)
			if err != nil {
				return []RDBValue{}, fmt.Errorf("error reading value: %s", err.Error())
			}

			values = append(values, RDBValue{Name: key, Item: types.NewItem(val, "string", -1)})
		default:
			fmt.Println("Unknown database header", dbHeader)
		}
	}

	return values, nil

}

func ReadRedisString(reader *bufio.Reader) (string, error) {
	strLength, isVal, err := Length(reader)
	if err != nil {
		return "", fmt.Errorf("error reading length: %s", err.Error())
	}

	if isVal {
		return strconv.Itoa(strLength), nil
	}

	str := make([]byte, strLength)
	_, err = reader.Read(str)
	if err != nil {
		return "", fmt.Errorf("error reading string: %s", err.Error())
	}

	return string(str), nil
}

func Length(reader *bufio.Reader) (int, bool, error) {
	firstByte := make([]byte, 1)
	_, err := reader.Read(firstByte)
	if err != nil {
		return 0, false, fmt.Errorf(
			"error reading the first byte on length encoding: %s",
			err.Error(),
		)
	}

	switch firstByte[0] & 0xC0 {
	// if the first two bits are 00, then the length is encoded in the next 6 bits
	case 0:
		return int(firstByte[0] & 0x3F), false, nil

	// if the first two bits are 01, then the length is encoded in the next 14 bits
	case 0x40:
		rest := make([]byte, 2)
		_, err := reader.Read(rest)
		if err != nil {
			return 0, false, fmt.Errorf(
				"error reading rest of the bytes on length encoding: %s",
				err.Error(),
			)
		}

		return int(rest[0])<<8 | int(rest[1]), false, nil

	// if the first two bits are 10, we can discard the next 6 bits and read the next 4 bytes
	case 0x80:
		rest := make([]byte, 4)
		_, err := reader.Read(rest)
		if err != nil {
			return 0, false, fmt.Errorf(
				"error reading rest of the bytes on length encoding: %s",
				err.Error(),
			)
		}

		return int(rest[0])<<24 | int(rest[1])<<16 | int(rest[2])<<8 | int(rest[3]), false, nil

	// if the first two bits are 11, then the length is encoded in the next 6 bits
	case 0xC0:
		switch firstByte[0] & 0x3F {
		// if next 6 bits are 0, then an 8 bit integer follows
		case 0:
			rest := make([]byte, 1)
			_, err := reader.Read(rest)
			if err != nil {
				return 0, false, fmt.Errorf(
					"error reading rest of the bytes on length encoding: %s",
					err.Error(),
				)
			}

			return int(rest[0]), true, nil

		// if next 6 bits are 1, then a 16 bit integer follows
		case 0x3F:
			rest := make([]byte, 2)
			_, err := reader.Read(rest)
			if err != nil {
				return 0, false, fmt.Errorf(
					"error reading rest of the bytes on length encoding: %s",
					err.Error(),
				)
			}

			return int(rest[0])<<8 | int(rest[1]), false, nil

		// if next 6 bits are 2, then a 32 bit integer follows
		case 0x3E:
			rest := make([]byte, 4)
			_, err := reader.Read(rest)
			if err != nil {
				return 0, false, fmt.Errorf(
					"error reading rest of the bytes on length encoding: %s",
					err.Error(),
				)
			}

			return int(rest[0])<<24 | int(rest[1])<<16 | int(rest[2])<<8 | int(rest[1]), false, nil

			// if next 6 bits are 3, then a compressed string follows
		case 0x3D:
			return 0, false, errors.New("compressed string not supported")
		}
	}

	return 0, false, errors.New("invalid length encoding")
}

const (
	StringType = iota
	ListType
	SetType
	ZSetType
	HashType
	ZipmapType
	ZiplistType
	IntsetType
	SortedsetType
	HashZipmapType
	ListZiplistType
	SetIntsetType
	SortedsetZiplistType
	ModuleType
)

func Value(reader *bufio.Reader) (int, error) {
	// read the type of the value
	valueType := make([]byte, 1)
	_, err := reader.Read((valueType))
	if err != nil {
		return -1, fmt.Errorf("error reading value type: %s", err.Error())
	}

	fmt.Println("Value type", valueType)

	switch valueType[0] {
	case 0:
		return StringType, nil
	case 1:
		return ListType, nil
	case 2:
		return SetType, nil
	case 3:
		return ZSetType, nil
	case 4:
		return HashType, nil
	case 9:
		return ZipmapType, nil
	case 10:
		return ZiplistType, nil
	case 11:
		return IntsetType, nil
	case 12:
		return SortedsetType, nil
	case 13:
		return HashZipmapType, nil
	case 14:
		return ListZiplistType, nil
	default:
		return StringType, nil
	}
}

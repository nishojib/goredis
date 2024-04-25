package types

import "time"

type Command struct {
	Name   string
	Args   [][]byte
	Length int
}

func NewCommand(name string, args [][]byte, length int) Command {
	return Command{
		Name:   name,
		Args:   args,
		Length: length,
	}
}

type Item struct {
	Value  string
	Type   string
	Expiry int64
}

func NewItem(value string, vType string, exp int64) Item {
	expiry := exp
	if exp != -1 {
		expiry = time.Now().UnixMilli() + exp
	}

	return Item{
		Value:  value,
		Type:   vType,
		Expiry: expiry,
	}
}

type Stream struct {
	Entries []StreamEntry
}

type StreamEntry struct {
	ID    string
	Items []StreamItem
}

type StreamItem struct {
	Key   string
	Value string
}

package resp

import "errors"

var ErrInvalidId = errors.New(
	"ERR The ID specified in XADD is equal or smaller than the target stream top item",
)
var ErrGreaterThanZero = errors.New("ERR The ID specified in XADD must be greater than 0-0")

package fileparser

import (
	"encoding/binary"
	"fmt"
)

func assertBinarySize(x interface{}, expect int) {
	if sizeX := binary.Size(x); sizeX != expect {
		panic(fmt.Sprintf("%#v size:%v,expected:%v", x, sizeX, expect))
	}
}

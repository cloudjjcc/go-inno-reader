package fileparser

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"
)

func init() {
	assertStructSize(INodeEntry{}, SizeOfINodeEntry)
	assertStructSize(FilHeader{}, SizeOfFilHeader)
	assertStructSize(FSegHeader{}, SizeOfFSegHeader)
	assertStructSize(IndexHeader{}, SizeOfIndexHeader)
}

var mysqlByteOrder = binary.BigEndian

// ReadRawPageAt 读取指定页的数据
func ReadRawPageAt(file *os.File, pageNo PageNo) (*RawPage, error) {
	offset := int64(pageNo * SizeOfPage)
	page := make([]byte, SizeOfPage)
	_, err := file.ReadAt(page, offset)
	if err != nil {
		return nil, err
	}
	return NewRawPage(page), err
}
func ReadPageType(page []byte) FilPageType {
	return FilPageType(mysqlByteOrder.Uint16(page[PageOffsetFilPageType:]))
}

func Parse(file *os.File) {
	pageNo := PageNo(0)
	reader := bufio.NewReader(os.Stdin)
	for {
		page, err := ReadRawPageAt(file, pageNo)
		if err != nil {
			fmt.Println("read page failed:", err)
			return
		}
		filHeader := page.ReadFilHeader()
		filTrailer := page.ReadFilTrailer()
		// check LSN
		if filTrailer.Low32LSN != uint32(filHeader.FilPageLSN&0xffffffff) {
			log.Printf("lsn not match")
			return
		}
		// print page base info
		fmt.Printf("\n--- Page %d ---\n", pageNo)
		fmt.Printf("PageType: %s\n", filHeader.FilPageType)
		fmt.Printf("SpaceID: %d\n", filHeader.FilPageSpaceId)
		fmt.Printf("PageLSN: %d\n", filHeader.FilPageLSN)
		// print help info
		fmt.Println("\ninput command(n=next page,p=prev page,q=quit)\n", file.Name())

		fmt.Print(">>> ")
		cmd, _ := reader.ReadString('\n')
		cmd = strings.TrimSpace(cmd)
		switch cmd {
		case "n":
			pageNo++
		case "p":
			if pageNo > 0 {
				pageNo--
			} else {
				fmt.Println("already first page")
			}
		case "q":
			fmt.Println("quit")
		default:
			fmt.Println("unknown command:", cmd)
			break
		}
	}
}

package ibdparser

import (
	"fmt"
	"strings"
)

const (
	SizeOfINodeEntry    = 192
	FspSegInodesPerPage = 85
)

func init() {
	assertStructSize(INodeEntry{}, SizeOfINodeEntry)
	registerPageParser(PageTypeINode, parseINodePage)
}

type INodeEntry struct {
	FSegID           uint64 //segment id
	FSegNotNullNUsed uint32
	FSegFree         FLSTBaseNode
	FSegNotNull      FLSTBaseNode
	FSegFull         FLSTBaseNode
	FSegMagicN       uint32
	FSegFragArr      [32]uint32
}

type INodePage struct {
	*BasePage
	INodeEntries []INodeEntry
}

func (p *INodePage) String() string {
	sb := &strings.Builder{}
	sb.WriteString(p.BasePage.String())
	sb.WriteString(fmt.Sprintf("Entries: %d\n", len(p.INodeEntries)))

	for i, e := range p.INodeEntries {
		sb.WriteString(fmt.Sprintf("Entry %d: FSegID=%d, NotNullNUsed=%d, Magic=%d\n",
			i, e.FSegID, e.FSegNotNullNUsed, e.FSegMagicN))
	}
	return sb.String()
}
func parseINodePage(t *Tablespace, basePage *BasePage, body []byte) (IPage, error) {
	page := &INodePage{
		BasePage:     basePage,
		INodeEntries: make([]INodeEntry, 0, FspSegInodesPerPage),
	}

	offset := 0
	for i := 0; i < FspSegInodesPerPage; i++ {
		if offset+SizeOfINodeEntry > len(body) {
			break // 防止越界
		}

		entry := INodeEntry{}

		entry.FSegID = mysqlByteOrder.Uint64(body[offset:])
		offset += 8

		entry.FSegNotNullNUsed = mysqlByteOrder.Uint32(body[offset:])
		offset += 4

		// 解析 FLSTBaseNode
		parseFLSTBaseNode := func(node *FLSTBaseNode) {
			node.Len = mysqlByteOrder.Uint32(body[offset:])
			offset += 4
			node.First.PageNo = PageNo(mysqlByteOrder.Uint32(body[offset:]))
			offset += 4
			node.First.Offset = mysqlByteOrder.Uint16(body[offset:])
			offset += 2
			node.Last.PageNo = PageNo(mysqlByteOrder.Uint32(body[offset:]))
			offset += 4
			node.Last.Offset = mysqlByteOrder.Uint16(body[offset:])
			offset += 2
		}

		parseFLSTBaseNode(&entry.FSegFree)
		parseFLSTBaseNode(&entry.FSegNotNull)
		parseFLSTBaseNode(&entry.FSegFull)

		entry.FSegMagicN = mysqlByteOrder.Uint32(body[offset:])
		offset += 4

		for j := 0; j < 32; j++ {
			entry.FSegFragArr[j] = mysqlByteOrder.Uint32(body[offset:])
			offset += 4
		}

		page.INodeEntries = append(page.INodeEntries, entry)
	}

	return page, nil
}

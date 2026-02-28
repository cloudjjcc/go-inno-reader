package fileparser

type FLSTBaseNode struct {
	Len         uint32
	First, Last FilAddress
}
type FLSTNode struct {
	Prev, Next FilAddress
}
type FSPHeader struct {
	FSPSpaceID       uint32
	FSPNotUse        uint32
	FSPSize          uint32
	FSPFreeLimit     uint32
	FSPSpaceFlags    uint32
	FSPFragNUsed     uint32
	FSPFree          FLSTBaseNode
	FSPFreeFrag      FLSTBaseNode
	FSPFullFrag      FLSTBaseNode
	FSPSegID         uint64
	FSPSegInodesFull FLSTBaseNode
	FSPSegInodesFree FLSTBaseNode
}

type XDESEntry struct {
	XDESID          uint64
	XDESFLSTNode    FLSTNode
	XDESState       uint32
	XDESStateBitmap [16]byte
}

// FspHdrPage  file space header page
// 表空间文件头页，它是innodb 表空间的page0,负责管理整个表空间的空间分配信息
type FspHdrPage struct {
	FilHeader
	FSPHeader
	XDESEntries []XDESEntry
	FilTrailer
}

// ParseFspHdrPage 解析FspHdr 页数据
func ParseFspHdrPage(data []byte) (*FspHdrPage, error) {
	page := &FspHdrPage{}
	return page, nil
}

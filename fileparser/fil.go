package fileparser

import (
	"math"
)

type PageNo uint32

const (
	SizeOfPage         = 16 * 1024
	SizeOfFilHeader    = 38
	SizeOfFilTrailer   = 8
	SizeOfFLSTNode     = 12
	SizeOfFLSTBaseNode = 16
	SizeOfINodeEntry   = 192
	SizeOfIndexHeader  = 36
	SizeOfFSegHeader   = 10
	SizeOfPageDirSlot  = 2
)
const (
	PageOffsetFilPageType        = 24
	PageOffsetFilPageData        = 38
	PageOffsetXDESEntry          = 150
	PageOffsetFSegArr            = 50
	FspSegInodesPerPage          = 85
	FilNull               PageNo = math.MaxUint32
	FSegMagicNValue              = 97937874
)

type FilPageType uint16

const (
	PageTypeAllocated  FilPageType = 0
	PageTypeUndoLog    FilPageType = 2
	PageTypeINode      FilPageType = 3
	PageTypeIBufBitmap FilPageType = 5
	PageTypeFSPHDR     FilPageType = 8
	PageTypeXDES       FilPageType = 9
	PageTypeIndex      FilPageType = 17855
	PageTypeSDI        FilPageType = 17853
)

func (fp FilPageType) String() string {
	switch fp {
	case PageTypeAllocated:
		return "Allocated"
	case PageTypeUndoLog:
		return "UndoLog"
	case PageTypeINode:
		return "INode"
	case PageTypeIBufBitmap:
		return "IBufBitmap"
	case PageTypeFSPHDR:
		return "FSPHDR"
	case PageTypeXDES:
		return "XDES"
	case PageTypeIndex:
		return "Index"
	case PageTypeSDI:
		return "SDI"
	default:
		return "Unknown"
	}
}

// FilHeader 表空间页公共头
type FilHeader struct {
	FilPageSizeOrChecksum uint32
	FilPageOffset         PageNo
	FilPagePre            PageNo
	FilPageNext           PageNo
	FilPageLSN            uint64
	FilPageType           FilPageType
	FilPageFileFlushLSN   uint64
	FilPageSpaceId        uint32
	//FilPageAlgorithmV1    uint8
	//FilPageOriginalTypeV1 uint16
	//FilPageOriginalSizeV1 uint16
	//FilPageCompressSizeV1 uint16
}

// FilTrailer 表空间页公共尾
type FilTrailer struct {
	CheckSum uint32
	Low32LSN uint32
}
type FilAddress struct {
	PageNo PageNo
	Offset uint16
}

type FSegHeader struct {
	FSegHdrSpace  uint32
	FSegHdrPageNo uint32
	FSegHdrOffset uint16
}
type PageFormat uint8

const (
	REDUNDANT PageFormat = iota
	COMPACT
)

func (pf PageFormat) String() string {
	switch pf {
	case REDUNDANT:
		return "REDUNDANT"
	case COMPACT:
		return "COMPACT"
	default:
		return "Unknown"
	}

}

// RawPage 原始页数据
type RawPage struct {
	data []byte
}

func (p *RawPage) ReadFilHeader() *FilHeader {
	return &FilHeader{
		FilPageSizeOrChecksum: mysqlByteOrder.Uint32(p.data[0:4]),
		FilPageOffset:         PageNo(mysqlByteOrder.Uint32(p.data[4:8])),
		FilPagePre:            PageNo(mysqlByteOrder.Uint32(p.data[8:12])),
		FilPageNext:           PageNo(mysqlByteOrder.Uint32(p.data[12:16])),
		FilPageLSN:            mysqlByteOrder.Uint64(p.data[16:24]),
		FilPageType:           FilPageType(mysqlByteOrder.Uint16(p.data[24:26])),
		FilPageFileFlushLSN:   mysqlByteOrder.Uint64(p.data[26:34]),
		FilPageSpaceId:        mysqlByteOrder.Uint32(p.data[34:38]),
	}
}
func (p *RawPage) ReadFilTrailer() *FilTrailer {
	data := p.data[SizeOfPage-SizeOfFilTrailer:]
	return &FilTrailer{
		CheckSum: mysqlByteOrder.Uint32(data[0:4]),
		Low32LSN: mysqlByteOrder.Uint32(data[4:8]),
	}
}
func NewRawPage(data []byte) *RawPage {
	return &RawPage{data: data}
}

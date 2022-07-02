package fileparser

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"math"
	"os"
)

func init() {
	assertBinarySize(INodeEntry{}, INodeEntrySize)
	assertBinarySize(FilHeader{}, FilHeaderSize)
	assertBinarySize(FSegHeader{}, FSegHeaderSize)
	assertBinarySize(PageHeader{}, PageHeaderSize)
}

type PageNo uint32

const (
	PageSize                     = 16 * 1024
	PageOffsetFilPageType        = 24
	PageOffsetFilPageData        = 38
	PageOffsetXDESEntry          = 150
	PageOffsetFSegArr            = 50
	FilHeaderSize                = 38
	FLSTNodeSize                 = 12
	FLSTBaseNodeSize             = 16
	INodeEntrySize               = 192
	FspSegInodesPerPage          = 85
	PageHeaderSize               = 56
	FSegHeaderSize               = 10
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

type FilHeader struct {
	FilPageSizeOrChecksum uint32
	FilPageOffset         PageNo
	FilPagePre            PageNo
	FilPageNext           PageNo
	FilPageLSN            uint64
	FilPageType           FilPageType
	FilPageFileFlushLSN   uint8
	FilPageAlgorithmV1    uint8
	FilPageOriginalTypeV1 uint16
	FilPageOriginalSizeV1 uint16
	FilPageCompressSizeV1 uint16
	FilPageSpaceId        uint32
}
type FilAddress struct {
	PageNo PageNo
	Offset uint16
}
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
type PageHeader struct {
	PageNDirSlots  uint16
	PageHeapTop    uint16
	PageNHeap      uint16
	PageFree       uint16
	PageGarbage    uint16
	PageLastInsert uint16
	PageDirection  uint16
	PageNDirection uint16
	PageNRecs      uint16
	PageMaxTrxID   uint64
	PageLevel      uint16
	PageIndexID    uint64
	PageBtrSegLeaf FSegHeader
	PageBtrSegTop  FSegHeader
}
type FSegHeader struct {
	FSegHdrSpace  uint32
	FSegHdrPageNo uint32
	FSegHdrOffset uint16
}

var mysqlByteOrder = binary.BigEndian

func ReadPage(file *os.File) ([]byte, error) {
	page := make([]byte, PageSize)
	_, err := file.Read(page)
	return page, err
}
func ReadPageType(page []byte) FilPageType {
	return FilPageType(mysqlByteOrder.Uint16(page[PageOffsetFilPageType:]))
}

type XDESEntry struct {
	XDESID          uint64
	XDESFLSTNode    FLSTNode
	XDESState       uint32
	XDESStateBitmap [16]byte
}
type INodeEntry struct {
	FSegID           uint64
	FSegNotNullNUsed uint32
	FSegFree         FLSTBaseNode
	FSegNotNull      FLSTBaseNode
	FSegFull         FLSTBaseNode
	FSegMagicN       uint32
	FSegFragArr      [32]uint32
}

func Parse(file *os.File) {
	for {
		page, err := ReadPage(file)
		if err != nil {
			log.Printf("read page failed:%v", err)
			return
		}
		pageReader := bytes.NewReader(page)
		filHeader := new(FilHeader)
		if err := binary.Read(pageReader, mysqlByteOrder, filHeader); err != nil {
			log.Printf("read fil header failed:%v", err)
			return
		}
		switch filHeader.FilPageType {
		case PageTypeFSPHDR:
			log.Printf("page type hdr\n")
			fspHeader := new(FSPHeader)
			_, _ = pageReader.Seek(PageOffsetFilPageData, io.SeekStart)
			//reader := bytes.NewReader(page[PageOffsetFilPageData:])
			if err := binary.Read(pageReader, mysqlByteOrder, fspHeader); err != nil {
				log.Printf("read fsp header failed:%v", err)
				return
			}
			_, _ = pageReader.Seek(PageOffsetXDESEntry, io.SeekStart)
			var xdesEntry XDESEntry
			for i := 0; i < 256; i++ {
				if err := binary.Read(pageReader, mysqlByteOrder, &xdesEntry); err != nil {
					log.Printf("read xdes entry failed:%v", err)
					break
				}
				log.Printf("xdes entry:%+v", xdesEntry)
			}
		case PageTypeIBufBitmap:
			log.Printf("page type ibufbitmap\n")
		case PageTypeXDES:
			log.Printf("page type xdes\n")
		case PageTypeINode:
			log.Printf("page type inode\n")
			_, _ = pageReader.Seek(PageOffsetFSegArr, io.SeekStart)
			var inodeEntry INodeEntry
			for i := 0; i < FspSegInodesPerPage; i++ {
				if err := binary.Read(pageReader, mysqlByteOrder, &inodeEntry); err != nil {
					log.Printf("read inode entry:%v failed:%v", i, err)
				}
				log.Printf("inode entry:%+v", inodeEntry)
			}
		case PageTypeIndex:
			log.Printf("page type index\n")
			_, _ = pageReader.Seek(PageOffsetFilPageData, io.SeekStart)
			var pageHeader PageHeader
			if err := binary.Read(pageReader, mysqlByteOrder, &pageHeader); err != nil {
				log.Printf("read page header failed:%v", err)
			}
			log.Printf("page header:%+v", pageHeader)
		case PageTypeUndoLog:
			log.Printf("page type undo log\n")
		case PageTypeSDI:
			log.Printf("page type sdi")
		default:
			log.Printf("page type unkown\n")
		}
	}
}

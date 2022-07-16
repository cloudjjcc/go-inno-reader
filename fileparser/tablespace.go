package fileparser

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
)

func init() {
	assertBinarySize(INodeEntry{}, SizeOfINodeEntry)
	assertBinarySize(FilHeader{}, SizeOfFilHeader)
	assertBinarySize(FSegHeader{}, SizeOfFSegHeader)
	assertBinarySize(IndexHeader{}, SizeOfIndexHeader)
}

var mysqlByteOrder = binary.BigEndian

func ReadPageFrame(file *os.File) ([]byte, error) {
	page := make([]byte, SizeOfPage)
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
		page, err := ReadPageFrame(file)
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
		filTrailer := new(FilTrailer)
		_, _ = pageReader.Seek(SizeOfPage-SizeOfFilTrailer, io.SeekStart)
		if err := binary.Read(pageReader, mysqlByteOrder, filTrailer); err != nil {
			log.Printf("read fil trailer failed:%v", err)
			return
		}
		if filTrailer.Low32LSN != uint32(filHeader.FilPageLSN&0xffffffff) {
			log.Printf("lsn not match")
			return
		}
		switch filHeader.FilPageType {
		case PageTypeFSPHDR:
			log.Printf("page type hdr\n")
			fspHeader := new(FSPHeader)
			_, _ = pageReader.Seek(PageOffsetFilPageData, io.SeekStart)
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
					continue
				}
				log.Printf("inode entry:%+v", inodeEntry)
			}
		case PageTypeIndex:
			log.Printf("page type index\n")
			_, _ = pageReader.Seek(PageOffsetFilPageData, io.SeekStart)
			index, err := ReadIndex(pageReader)
			if err != nil {
				log.Printf("read index failed:%v", err)
				continue
			}
			log.Printf("index header:%s", index.IndexHeader.String())
		case PageTypeUndoLog:
			log.Printf("page type undo log\n")
		case PageTypeSDI:
			log.Printf("page type sdi")
		default:
			log.Printf("page type unkown\n")
		}
	}
}

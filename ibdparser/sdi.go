package ibdparser

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
)

func init() {
	registerPageParser(PageTypeSDI, parseSDIPage)
}

// SDIPage (Serialized Dictionary Information Page) 序列化字段信息页
type SDIPage struct {
	*BasePage
	Header IndexHeader

	Infimum  SystemRecord
	Supremum SystemRecord

	Records  []*SDIRecord
	DirSlots []uint16
}
type SDIRecord struct {
	Offset          uint16
	Header          RecordHeader
	Type            uint32
	ID              uint64
	TrxID           [6]byte
	RollPtr         [7]byte
	UncompressedLen uint32
	CompressedLen   uint32
	ZipData         []byte
}

func (r *SDIRecord) JSON() ([]byte, error) {

	reader, err := zlib.NewReader(bytes.NewReader(r.ZipData))
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = reader.Close()
	}()

	return io.ReadAll(reader)
}
func parseSDIPage(t *Tablespace, basePage *BasePage, body []byte) (IPage, error) {

	page := &SDIPage{
		BasePage: basePage,
	}

	offset := 0

	// ========================
	// 1 解析 IndexHeader
	// ========================

	page.Header.PageNDirSlots = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	page.Header.PageHeapTop = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	page.Header.PageNHeap = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	page.Header.PageFree = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	page.Header.PageGarbage = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	page.Header.PageLastInsert = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	page.Header.PageDirection = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	page.Header.PageNDirection = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	page.Header.PageNRecs = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	page.Header.PageMaxTrxID = mysqlByteOrder.Uint64(body[offset:])
	offset += 8

	page.Header.PageLevel = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	page.Header.PageIndexID = mysqlByteOrder.Uint64(body[offset:])
	offset += 8

	// FSegHeader
	page.Header.PageBtrSegLeaf.FSegHdrSpace = mysqlByteOrder.Uint32(body[offset:])
	offset += 4
	page.Header.PageBtrSegLeaf.FSegHdrPageNo = PageNo(mysqlByteOrder.Uint32(body[offset:]))
	offset += 4
	page.Header.PageBtrSegLeaf.FSegHdrOffset = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	page.Header.PageBtrSegTop.FSegHdrSpace = mysqlByteOrder.Uint32(body[offset:])
	offset += 4
	page.Header.PageBtrSegTop.FSegHdrPageNo = PageNo(mysqlByteOrder.Uint32(body[offset:]))
	offset += 4
	page.Header.PageBtrSegTop.FSegHdrOffset = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	// ========================
	// 2 解析 infimum
	// ========================

	infimumOffset := offset

	page.Infimum.RecordHeader.InfoFlagsAndNRecOwned = body[offset]
	offset++

	page.Infimum.RecordHeader.OrderAndRecordType = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	page.Infimum.RecordHeader.NextRecordOffset = int16(mysqlByteOrder.Uint16(body[offset:]))
	offset += 2

	copy(page.Infimum.Data[:], body[offset:offset+8])
	offset += 8
	if page.Infimum.RecordType() != RecordTypeInfimum ||
		string(page.Infimum.Data[:7]) != "infimum" {
		fmt.Println("parse infimum failed")
	}
	// ========================
	// 3 解析 supremum
	// ========================

	page.Supremum.RecordHeader.InfoFlagsAndNRecOwned = body[offset]
	offset++

	page.Supremum.RecordHeader.OrderAndRecordType = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	page.Supremum.RecordHeader.NextRecordOffset = int16(mysqlByteOrder.Uint16(body[offset:]))
	offset += 2

	copy(page.Supremum.Data[:], body[offset:offset+8])
	offset += 8
	if page.Supremum.RecordType() != RecordTypeSupremum ||
		string(page.Supremum.Data[:8]) != "supremum" {
		fmt.Println("parse supremum failed")
	}
	// ========================
	// 4 遍历 record list
	// ========================

	offset = infimumOffset + int(page.Infimum.RecordHeader.NextRecordOffset)

	for {

		rec, next := parseSDIRecord(body, uint16(offset))

		if rec == nil || next == 0 {
			break
		}

		if rec.Header.RecordType() == RecordTypeSupremum {
			break
		}
		json, _ := rec.JSON()
		fmt.Println(string(json))
		page.Records = append(page.Records, rec)

		offset = int(next)
	}

	// ========================
	// 5 解析 Page Directory
	// ========================

	offset = SizeOfPage -
		SizeOfFilHeader -
		SizeOfFilTrailer -
		int(page.Header.PageNDirSlots)*2

	page.DirSlots = make([]uint16, page.Header.PageNDirSlots)

	for i := 0; i < int(page.Header.PageNDirSlots); i++ {
		page.DirSlots[i] = mysqlByteOrder.Uint16(body[offset:])
		offset += 2
	}

	return page, nil
}

func parseSDIRecord(body []byte, offset uint16) (*SDIRecord, uint16) {

	pos := int(offset)

	rec := &SDIRecord{}
	rec.Offset = offset

	// RecordHeader
	rec.Header.InfoFlagsAndNRecOwned = body[pos]
	pos++

	rec.Header.OrderAndRecordType = mysqlByteOrder.Uint16(body[pos:])
	pos += 2

	rec.Header.NextRecordOffset = int16(mysqlByteOrder.Uint16(body[pos:]))
	pos += 2

	next := uint16(int16(offset) + rec.Header.NextRecordOffset)

	if int(next) >= len(body) {
		return nil, 0
	}

	// payload
	rec.Type = mysqlByteOrder.Uint32(body[pos:])
	pos += 4

	rec.ID = mysqlByteOrder.Uint64(body[pos:])
	pos += 8

	copy(rec.TrxID[:], body[pos:pos+6])
	pos += 6

	copy(rec.RollPtr[:], body[pos:pos+7])
	pos += 7

	rec.UncompressedLen = mysqlByteOrder.Uint32(body[pos:])
	pos += 4

	rec.CompressedLen = mysqlByteOrder.Uint32(body[pos:])
	pos += 4

	rec.ZipData = body[pos : pos+int(rec.CompressedLen)]

	return rec, next
}

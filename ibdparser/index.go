package ibdparser

import (
	"fmt"
	"strings"
)

const (
	SizeOfIndexHeader  = 56
	SizeOfFSegHeader   = 10
	SizeOfSystemRecord = 13
)

type RecordType uint8

const (
	RecordTypeOrdinary RecordType = 0
	RecordTypeNodePtr  RecordType = 1
	RecordTypeInfimum  RecordType = 2
	RecordTypeSupremum RecordType = 3
)

func init() {
	assertStructSize(IndexHeader{}, SizeOfIndexHeader)
	assertStructSize(FSegHeader{}, SizeOfFSegHeader)
	assertStructSize(SystemRecord{}, SizeOfSystemRecord)
	registerPageParser(PageTypeIndex, parseIndexPage)
}

type SystemRecord struct {
	RecordHeader
	Data [8]byte
}
type RecordHeader struct {
	InfoFlagsAndNRecOwned uint8
	OrderAndRecordType    uint16
	NextRecordOffset      int16 //下一行记录的相对偏移量
}

func (rh *RecordHeader) RecordType() RecordType {
	return RecordType(rh.OrderAndRecordType & 0x7)
}
func (rh *RecordHeader) HeapNo() uint16 {
	return rh.OrderAndRecordType >> 3
}
func (rh *RecordHeader) NOwned() uint8 {
	return rh.InfoFlagsAndNRecOwned & 0x0f
}

func (rh *RecordHeader) IsMinRec() bool {
	return rh.InfoFlagsAndNRecOwned&0x10 != 0
}
func (rh *RecordHeader) IsDeleted() bool {
	return rh.InfoFlagsAndNRecOwned&0x20 != 0
}

type IndexHeader struct {
	/*Page directory中的slot个数*/
	PageNDirSlots uint16
	/*指向当前Page内已使用的空间的末尾位置，即free space的开始位置*/
	PageHeapTop uint16
	/**
	 * records + infimum and supremum system records, and garbage (deleted) records.
	 * Page内所有记录个数，包含用户记录，系统记录以及标记删除的记录，同时当第一个bit设置为1时，
	 * 表示这个page 内是以Compact格式存储的.
	 */
	PageNHeap uint16
	/**
	 * A pointer to the first entry in the list of garbage (deleted) records.
	 * The list is singly-linked together using the “next record” pointers in each record header.
	 */
	PageFree uint16
	/**
	 * 被删除的记录链表上占用的总的字节数，属于可回收的垃圾碎片空间.
	 */
	PageGarbage uint16
	/**
	 * 指向最近一次插入的记录偏移量，主要用于优化顺序插入操作.
	 */
	PageLastInsert uint16
	/**
	 * LEFT, RIGHT, and NO_DIRECTION. sequential inserts (to the left [lower values] or right
	 * [higher values]) or random inserts. 用于指示当前记录的插入顺序以及是否正在进行顺序插入，每次插入时，
	 * PAGE_LAST_INSERT会和当前记录进行比较，以确认插入方向，据此进行插入优化.
	 */
	PageDirection uint16

	PageNDirection uint16
	/**
	 * non-deleted user records in the page.
	 * Page上有效的未被标记删除的用户记录个数.
	 */
	PageNRecs uint16
	/**
	 * 最近一次修改该page记录的事务ID，主要用于辅助判断二级索引记录的可见性.
	 */
	PageMaxTrxID uint64
	/**
	 * Leaf pages are at level 0, and the level increments up the B+tree from there.
	 * In a typical 3-level B+tree, the root will be level 2,
	 * some number of internal non-leaf pages will be level 1, and leaf pages will be level 0.
	 * 该Page所在的btree level，根节点的level最大，叶子节点的level为0.
	 */
	PageLevel uint16
	/**
	 * 该Page归属的索引ID.
	 */
	PageIndexID uint64

	PageBtrSegLeaf FSegHeader
	PageBtrSegTop  FSegHeader
}

func (i *IndexHeader) GetNumOfHeapRecords() uint16 {
	return i.PageNHeap & 0x7fff
}
func (i *IndexHeader) GetPageFormat() PageFormat {
	return PageFormat((i.PageNHeap & 0x8000) >> 15)
}
func (i *IndexHeader) String() string {
	return fmt.Sprintf("[PageNDirSlots:%d,PageNHeapRecords:%d,PageFormat:%s]",
		i.PageNDirSlots, i.GetNumOfHeapRecords(), i.GetPageFormat())
}

type IndexPage struct {
	*BasePage
	Header      IndexHeader
	Infimum     SystemRecord
	Supremum    SystemRecord
	UserRecords []*UserRecord
	DirSlots    []uint16
}

func (p *IndexPage) String() string {
	sb := &strings.Builder{}
	sb.WriteString(p.BasePage.String())
	sb.WriteString(fmt.Sprintf("\n%s\n", p.Header.String()))
	sb.WriteString(fmt.Sprintf("Level: %d, NumRecords: %d\n", p.Header.PageLevel, p.Header.GetNumOfHeapRecords()))
	sb.WriteString(fmt.Sprintf("DirSlots: %d\n", len(p.DirSlots)))
	return sb.String()
}
func parseIndexPage(t *Tablespace, basePage *BasePage, body []byte) (IPage, error) {

	if len(body) < SizeOfIndexHeader+SizeOfFSegHeader*2+2*SizeOfSystemRecord {
		return nil, fmt.Errorf("index page body too small")
	}

	page := &IndexPage{
		BasePage: basePage,
	}

	offset := 0

	// 解析 IndexHeader
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

	// Infimum 系统行
	infimumOffset := offset
	page.Infimum.RecordHeader.InfoFlagsAndNRecOwned = body[offset]
	offset += 1
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

	// Supremum 系统行
	page.Supremum.RecordHeader.InfoFlagsAndNRecOwned = body[offset]
	offset += 1
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
	offset = infimumOffset + int(page.Infimum.RecordHeader.NextRecordOffset)
	for {

		rec, next := parseUserRecord(body, uint16(offset))
		if next == 0 {
			break
		}
		fmt.Println("record offset:", rec.Offset)

		if rec.Header.RecordType() == RecordTypeSupremum {
			break
		}
		page.UserRecords = append(page.UserRecords, rec)
		offset = int(next)
	}
	// 页目录 DirSlots
	offset = len(body) - int(page.Header.PageNDirSlots)*2
	page.DirSlots = make([]uint16, page.Header.PageNDirSlots)
	for i := 0; i < int(page.Header.PageNDirSlots); i++ {
		page.DirSlots[i] = mysqlByteOrder.Uint16(body[offset:])
		offset += 2
	}

	return page, nil
}

type UserRecord struct {
	Offset uint16
	Header RecordHeader
	Data   []byte
}

func parseUserRecord(body []byte, offset uint16) (*UserRecord, uint16) {

	pos := int(offset)

	rec := &UserRecord{}
	rec.Offset = offset + SizeOfFilHeader //body去除了fil_header和fil_trailer,这里的offset是页内偏移量

	rec.Header.InfoFlagsAndNRecOwned = body[pos]
	pos++

	rec.Header.OrderAndRecordType = mysqlByteOrder.Uint16(body[pos:])
	pos += 2

	rec.Header.NextRecordOffset = int16(mysqlByteOrder.Uint16(body[pos:]))
	pos += 2

	next := uint16(int16(offset) + rec.Header.NextRecordOffset)

	if int(next) < pos || int(next) > len(body) {
		return nil, 0
	}
	rec.Data = body[pos : pos+20]

	return rec, next
}
func parseCompactRow() {}

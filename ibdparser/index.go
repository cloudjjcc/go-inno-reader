package ibdparser

import (
	"fmt"
	"strings"
)

const (
	SizeOfIndexHeader  = 36
	SizeOfFSegHeader   = 10
	SizeOfSystemRecord = 13
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
	NextRecordOffset      uint16
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

type IndexTop struct {
	IndexHeader    IndexHeader
	PageBtrSegLeaf FSegHeader
	PageBtrSegTop  FSegHeader
	Infimum        SystemRecord
	Supremum       SystemRecord
}
type IndexPage struct {
	*BasePage
	IndexTop
	DirSlots []uint16
}

func (p *IndexPage) String() string {
	sb := &strings.Builder{}
	sb.WriteString(p.BasePage.String())
	sb.WriteString(fmt.Sprintf("\n%s\n", p.IndexHeader.String()))
	sb.WriteString(fmt.Sprintf("Level: %d, NumRecords: %d\n", p.IndexHeader.PageLevel, p.IndexHeader.GetNumOfHeapRecords()))
	sb.WriteString(fmt.Sprintf("DirSlots: %d\n", len(p.DirSlots)))
	return sb.String()
}
func parseIndexPage(basePage *BasePage, body []byte) (IPage, error) {

	if len(body) < SizeOfIndexHeader+SizeOfFSegHeader*2+2*SizeOfSystemRecord {
		return nil, fmt.Errorf("index page body too small")
	}

	page := &IndexPage{
		BasePage: basePage,
	}

	offset := 0

	// 解析 IndexHeader
	page.IndexHeader.PageNDirSlots = mysqlByteOrder.Uint16(body[offset:])
	offset += 2
	page.IndexHeader.PageHeapTop = mysqlByteOrder.Uint16(body[offset:])
	offset += 2
	page.IndexHeader.PageNHeap = mysqlByteOrder.Uint16(body[offset:])
	offset += 2
	page.IndexHeader.PageFree = mysqlByteOrder.Uint16(body[offset:])
	offset += 2
	page.IndexHeader.PageGarbage = mysqlByteOrder.Uint16(body[offset:])
	offset += 2
	page.IndexHeader.PageLastInsert = mysqlByteOrder.Uint16(body[offset:])
	offset += 2
	page.IndexHeader.PageDirection = mysqlByteOrder.Uint16(body[offset:])
	offset += 2
	page.IndexHeader.PageNDirection = mysqlByteOrder.Uint16(body[offset:])
	offset += 2
	page.IndexHeader.PageNRecs = mysqlByteOrder.Uint16(body[offset:])
	offset += 2
	page.IndexHeader.PageMaxTrxID = mysqlByteOrder.Uint64(body[offset:])
	offset += 8
	page.IndexHeader.PageLevel = mysqlByteOrder.Uint16(body[offset:])
	offset += 2
	page.IndexHeader.PageIndexID = mysqlByteOrder.Uint64(body[offset:])
	offset += 8

	// FSegHeader
	page.PageBtrSegLeaf.FSegHdrSpace = mysqlByteOrder.Uint32(body[offset:])
	offset += 4
	page.PageBtrSegLeaf.FSegHdrPageNo = PageNo(mysqlByteOrder.Uint32(body[offset:]))
	offset += 4
	page.PageBtrSegLeaf.FSegHdrOffset = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	page.PageBtrSegTop.FSegHdrSpace = mysqlByteOrder.Uint32(body[offset:])
	offset += 4
	page.PageBtrSegTop.FSegHdrPageNo = PageNo(mysqlByteOrder.Uint32(body[offset:]))
	offset += 4
	page.PageBtrSegTop.FSegHdrOffset = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	// Infimum 系统行
	copy(page.Infimum.Data[:], body[offset:offset+8])
	offset += 8
	page.Infimum.RecordHeader.InfoFlagsAndNRecOwned = body[offset]
	offset += 1
	page.Infimum.RecordHeader.OrderAndRecordType = mysqlByteOrder.Uint16(body[offset:])
	offset += 2
	page.Infimum.RecordHeader.NextRecordOffset = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	// Supremum 系统行
	copy(page.Supremum.Data[:], body[offset:offset+8])
	offset += 8
	page.Supremum.RecordHeader.InfoFlagsAndNRecOwned = body[offset]
	offset += 1
	page.Supremum.RecordHeader.OrderAndRecordType = mysqlByteOrder.Uint16(body[offset:])
	offset += 2
	page.Supremum.RecordHeader.NextRecordOffset = mysqlByteOrder.Uint16(body[offset:])
	offset += 2

	// 页目录 DirSlots
	page.DirSlots = make([]uint16, page.IndexHeader.PageNDirSlots)
	for i := 0; i < int(page.IndexHeader.PageNDirSlots); i++ {
		page.DirSlots[i] = mysqlByteOrder.Uint16(body[offset:])
		offset += 2
	}

	return page, nil
}

package fileparser

import (
	"encoding/binary"
	"fmt"
	"io"
)

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
type Index struct {
	IndexTop
	DirSlots []uint16
}

func ReadIndex(seeker io.ReadSeeker) (*Index, error) {
	_, _ = seeker.Seek(PageOffsetFilPageData, io.SeekStart)
	idx := &Index{}
	if err := binary.Read(seeker, mysqlByteOrder, &idx.IndexTop); err != nil {
		return nil, err
	}
	idx.DirSlots = make([]uint16, idx.IndexHeader.PageNDirSlots)
	_, _ = seeker.Seek(int64(SizeOfPage-SizeOfFilTrailer-SizeOfPageDirSlot*idx.IndexHeader.PageNDirSlots), io.SeekStart)
	if err := binary.Read(seeker, mysqlByteOrder, &idx.DirSlots); err != nil {
		return nil, err
	}
	return idx, nil
}

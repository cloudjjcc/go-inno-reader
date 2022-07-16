package fileparser

import (
	"encoding/binary"
	"fmt"
	"io"
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
type FilTrailer struct {
	CheckSum uint32
	Low32LSN uint32
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

type SystemRecord struct {
	RecordHeader
	Data [8]byte
}
type RecordHeader struct {
	InfoFlagsAndNRecOwned uint8
	OrderAndRecordType    uint16
	NextRecordOffset      uint16
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

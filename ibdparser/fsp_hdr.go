package ibdparser

import (
	"fmt"
	"strings"
)

const (
	XDESCount          = 256
	SizeOfFspHeader    = 112
	SizeOfXDESEntry    = 40
	SizeOfFLSTNode     = 12
	SizeOfFLSTBaseNode = 16
)
const (
	FSPFlagsPosZipSSize    = 0 //压缩页的block size,为0表示非压缩表
	FSPFlagsPosAtomicBlobs = 1 //使用的是 compressed或者dynamic的行格式
	FSPFlagsPosPageSSize   = 2 //page size
	FSPFlagsPosDataDir     = 3 //如果该表空间显式指定了data_dir,则设置此flag
	FSPFlagsPosShared      = 4 //是否是共享的表空间
	FSPFlagsPosTemporary   = 5 //是否是临时表空间
	FSPFlagsPosEncryption  = 6 //是否是加密的表空间
	FSPFlagsPOSUnused      = 7 //未使用的位
)

// FLSTBaseNode 文件链表
type FLSTBaseNode struct {
	Len         uint32     //链表长度
	First, Last FilAddress //指向头，尾节点
}

// FLSTNode 文件链表节点
type FLSTNode struct {
	Prev, Next FilAddress //指向前一个节点、后一个节点
}
type FSPHeader struct {
	FSPSpaceID       uint32       // 文件对应的space_id
	FSPNotUse        uint32       // 保留字段
	FSPSize          uint32       // 当前表空间总的 page 数
	FSPFreeLimit     uint32       // 当前尚未初始化的最小page_no,从该page往后都尚未加入到表空间的FreeList上
	FSPSpaceFlags    uint32       // 当前表空间的 Flag 信息
	FSPFragNUsed     uint32       // fsp_free_frag 链表上已被使用的page数
	FSPFree          FLSTBaseNode //当Extent中所有page未被使用时，放到该链表上，用于随后的分配
	FSPFreeFrag      FLSTBaseNode //
	FSPFullFrag      FLSTBaseNode //extent中所有page都被使用时，放到该链表上
	FSPSegID         uint64       //当前文件中最大的segment_id+1,用于段分配时的 segment_id
	FSPSegInodesFull FLSTBaseNode //已被完全用满的inode page 链表
	FSPSegInodesFree FLSTBaseNode //至少存在1个空闲inode entry的inode page链表
}

// XDESEntry Extent Description Entry
type XDESEntry struct {
	XDESID       uint64 //extent id
	XDESFLSTNode FLSTNode
	XDESState    XDESState // extent 状态
	//每页用2bit表示状态 64*2 bit=128 bit=16 byte
	//00:free
	//01: allocated
	//10:fragment
	//11:reserved
	XDESStateBitmap [16]byte
}
type XDESState uint32

const (
	XDESFree     XDESState = 1 //存放于free链表
	XDESFreeFrag XDESState = 2 //存放于free_frag链表
	XDESFullFrag XDESState = 3 //存放于full_frag链表
	XDESFseg     XDESState = 4 //该extent归属于 xdes_id记录的值的segment
)

var _ IPage = (*FspHdrPage)(nil)

// FspHdrPage  file space header page
// 表空间文件头页，它是innodb 表空间的page0,负责管理整个表空间的空间分配信息
type FspHdrPage struct {
	*BasePage
	FSPHeader
	XDESEntries []XDESEntry
}

func (p *FspHdrPage) String() string {
	var sb strings.Builder
	sb.WriteString(p.BasePage.String())
	sb.WriteString(fmt.Sprintf("SpaceID:      %d\n", p.FSPSpaceID))
	sb.WriteString(fmt.Sprintf("SpaceSize:    %d pages\n", p.FSPSize))
	sb.WriteString(fmt.Sprintf("FreeLimit:    %d\n", p.FSPFreeLimit))
	sb.WriteString(fmt.Sprintf("SpaceFlags:   0x%x\n", p.FSPSpaceFlags))
	sb.WriteString(fmt.Sprintf("FragNUsed:    %d\n", p.FSPFragNUsed))
	sb.WriteString(fmt.Sprintf("NextSegID:    %d\n", p.FSPSegID))

	// 统计 XDES 状态
	var free, frag, full int

	for _, x := range p.XDESEntries {
		switch x.XDESState {
		case 0:
			free++
		case 1:
			frag++
		case 2:
			full++
		}
	}

	sb.WriteString("\n---- Extent Summary ----\n")
	sb.WriteString(fmt.Sprintf("Free: %d\n", free))
	sb.WriteString(fmt.Sprintf("Frag: %d\n", frag))
	sb.WriteString(fmt.Sprintf("Full: %d\n", full))

	sb.WriteString("\n------------------------\n")

	return sb.String()
}

func parseFspHdrPage(basePage *BasePage, bodyData []byte) (IPage, error) {
	page := &FspHdrPage{
		BasePage: basePage,
	}
	// ======================
	// 1️⃣ 解析 FSPHeader
	// ======================

	pos := 0

	readUint32 := func() uint32 {
		v := mysqlByteOrder.Uint32(bodyData[pos : pos+4])
		pos += 4
		return v
	}
	readUint64 := func() uint64 {
		v := mysqlByteOrder.Uint64(bodyData[pos : pos+8])
		pos += 8
		return v
	}
	readFilAddr := func() FilAddress {
		addr := FilAddress{
			PageNo: PageNo(mysqlByteOrder.Uint32(bodyData[pos : pos+4])),
			Offset: mysqlByteOrder.Uint16(bodyData[pos+4 : pos+6]),
		}
		pos += 6
		return addr
	}
	readFlstBase := func() FLSTBaseNode {
		node := FLSTBaseNode{
			Len: readUint32(),
		}
		node.First = readFilAddr()
		node.Last = readFilAddr()
		return node
	}

	page.FSPHeader = FSPHeader{
		FSPSpaceID:       readUint32(),
		FSPNotUse:        readUint32(),
		FSPSize:          readUint32(),
		FSPFreeLimit:     readUint32(),
		FSPSpaceFlags:    readUint32(),
		FSPFragNUsed:     readUint32(),
		FSPFree:          readFlstBase(),
		FSPFreeFrag:      readFlstBase(),
		FSPFullFrag:      readFlstBase(),
		FSPSegID:         readUint64(),
		FSPSegInodesFull: readFlstBase(),
		FSPSegInodesFree: readFlstBase(),
	}

	// ======================
	// 2️⃣ 解析 XDES
	// ======================

	page.XDESEntries = make([]XDESEntry, XDESCount)

	for i := 0; i < XDESCount; i++ {
		offset := SizeOfFspHeader + i*SizeOfXDESEntry
		data := bodyData[offset : offset+SizeOfXDESEntry]

		entry := XDESEntry{
			XDESID: mysqlByteOrder.Uint64(data[0:8]),
			XDESFLSTNode: FLSTNode{
				Prev: FilAddress{
					PageNo: PageNo(mysqlByteOrder.Uint32(data[8:12])),
					Offset: mysqlByteOrder.Uint16(data[12:14]),
				},
				Next: FilAddress{
					PageNo: PageNo(mysqlByteOrder.Uint32(data[14:18])),
					Offset: mysqlByteOrder.Uint16(data[18:20]),
				},
			},
			XDESState: XDESState(mysqlByteOrder.Uint32(data[20:24])),
		}

		copy(entry.XDESStateBitmap[:], data[24:40])

		page.XDESEntries[i] = entry
	}
	return page, nil
}
func init() {
	registerPageParser(PageTypeFSPHDR, parseFspHdrPage)
	assertStructSize(FSPHeader{}, SizeOfFspHeader)
	assertStructSize(FLSTBaseNode{}, SizeOfFLSTBaseNode)
	assertStructSize(FLSTNode{}, SizeOfFLSTNode)
	assertStructSize(XDESEntry{}, SizeOfXDESEntry)
}

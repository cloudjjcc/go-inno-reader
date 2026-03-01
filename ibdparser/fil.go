package ibdparser

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strings"
)

type PageNo uint32

const (
	SizeOfPage        = 16 * 1024
	SizeOfFilHeader   = 38
	SizeOfFilTrailer  = 8
	SizeOfPageDirSlot = 2
)
const (
	PageOffsetFilPageType        = 24
	PageOffsetFilPageData        = 38
	FilNull               PageNo = math.MaxUint32
	FSegMagicNValue              = 97937874
)

type FilPageType uint16

const (
	PageTypeAllocated              FilPageType = 0
	PageTypeUnused                 FilPageType = 1
	PageTypeUndoLog                FilPageType = 2
	PageTypeINode                  FilPageType = 3
	PageTypeIBufBitmap             FilPageType = 5
	PageTypeFSPHDR                 FilPageType = 8
	PageTypeXDES                   FilPageType = 9
	PageTypeBlob                   FilPageType = 10
	PageTypeZBlob                  FilPageType = 11
	PageTypeZBlob2                 FilPageType = 12
	PageTypeUnknown                FilPageType = 13
	PageTypeCompressed             FilPageType = 14
	PageTypeEncrypted              FilPageType = 15
	PageTypeCompressedAndEncrypted FilPageType = 16
	PageTypeEncryptedRtree         FilPageType = 17
	PageTypeSDIBlob                FilPageType = 18
	PageTypeSDIZBlob               FilPageType = 19
	PageTypeLegacyDBlwr            FilPageType = 20
	PageTypeSDI                    FilPageType = 17853
	PageTypeRTree                  FilPageType = 18854
	PageTypeIndex                  FilPageType = 17855
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
}

// FilTrailer 表空间页公共尾
type FilTrailer struct {
	CheckSum uint32
	Low32LSN uint32
}

// FilAddress 文件地址
type FilAddress struct {
	PageNo PageNo
	Offset uint16
}

type FSegHeader struct {
	FSegHdrSpace  uint32 //描述该segment的inode page所在的space_id
	FSegHdrPageNo PageNo //描述该segment的inode page 的page_no
	FSegHdrOffset uint16 // inode page的页内偏移量
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

// ReadPageType 读取页面类型
func (p *RawPage) ReadPageType() FilPageType {
	return FilPageType(mysqlByteOrder.Uint16(p.data[24:26]))
}

// ParseBasePage 解析基本页面信息
func (p *RawPage) ParseBasePage() *BasePage {
	basePage := &BasePage{}
	basePage.FilPageSizeOrChecksum = mysqlByteOrder.Uint32(p.data[0:4])
	basePage.FilPageOffset = PageNo(mysqlByteOrder.Uint32(p.data[4:8]))
	basePage.FilPagePre = PageNo(mysqlByteOrder.Uint32(p.data[8:12]))
	basePage.FilPageNext = PageNo(mysqlByteOrder.Uint32(p.data[12:16]))
	basePage.FilPageLSN = mysqlByteOrder.Uint64(p.data[16:24])
	basePage.FilPageType = FilPageType(mysqlByteOrder.Uint16(p.data[24:26]))
	basePage.FilPageFileFlushLSN = mysqlByteOrder.Uint64(p.data[26:34])
	basePage.FilPageSpaceId = mysqlByteOrder.Uint32(p.data[34:38])
	data := p.data[SizeOfPage-SizeOfFilTrailer:]
	basePage.CheckSum = mysqlByteOrder.Uint32(data[0:4])
	basePage.Low32LSN = mysqlByteOrder.Uint32(data[4:8])
	return basePage
}

func NewRawPage(data []byte) (*RawPage, error) {
	if len(data) != SizeOfPage {
		return nil, fmt.Errorf("invalid page size:%d", len(data))
	}
	return &RawPage{data: data}, nil
}

type IPage interface {
	GetFilHeader() *FilHeader
	GetFilTrailer() *FilTrailer
	fmt.Stringer
}

func (p *RawPage) ParsePage() (IPage, error) {
	basePage := p.ParseBasePage()
	// check LSN
	if basePage.Low32LSN != uint32(basePage.FilPageLSN&0xffffffff) {
		fmt.Println("warning:lsn not match")
	}
	parser, ok := pageParsers[basePage.FilPageType]
	if !ok {
		return nil, fmt.Errorf("not supported page type:%d", basePage.FilPageType)
	}
	body := p.data[SizeOfFilHeader : SizeOfPage-SizeOfFilTrailer]
	return parser(basePage, body)
}

type pageParser func(basePage *BasePage, bodyData []byte) (IPage, error)

var pageParsers = map[FilPageType]pageParser{}

func registerPageParser(pageType FilPageType, parser pageParser) {
	pageParsers[pageType] = parser
}

type BasePage struct {
	FilHeader
	FilTrailer
}

func (p *BasePage) GetFilHeader() *FilHeader {
	return &p.FilHeader
}

func (p *BasePage) GetFilTrailer() *FilTrailer {
	return &p.FilTrailer
}
func (p *BasePage) String() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("PageType: %s\n", p.FilPageType))
	sb.WriteString(fmt.Sprintf("SpaceID: %d\n", p.FilPageSpaceId))
	sb.WriteString(fmt.Sprintf("PageLSN: %d\n", p.FilPageLSN))
	return sb.String()
}

func init() {
	assertStructSize(FilHeader{}, SizeOfFilHeader)
}

var mysqlByteOrder = binary.BigEndian

// ReadRawPageAt 读取指定页的数据
func ReadRawPageAt(file *os.File, pageNo PageNo) (*RawPage, error) {
	offset := int64(pageNo * SizeOfPage)
	page := make([]byte, SizeOfPage)
	_, err := file.ReadAt(page, offset)
	if err != nil {
		return nil, err
	}
	return NewRawPage(page)
}
func ReadPageType(page []byte) FilPageType {
	return FilPageType(mysqlByteOrder.Uint16(page[PageOffsetFilPageType:]))
}

func Parse(file *os.File) {
	pageNo := PageNo(0)
	reader := bufio.NewReader(os.Stdin)
	for {
		rawPage, err := ReadRawPageAt(file, pageNo)
		if err != nil {
			fmt.Println("read page failed:", err)
			return
		}
		page, err := rawPage.ParsePage()
		if err != nil {
			fmt.Println("parse page failed:", err)
		} else {
			fmt.Printf("\n--- Page %d ---\n", pageNo)
			fmt.Print(page)
		}

		// print help info
		fmt.Println("\ninput command(n=next page,p=prev page,q=quit)\n", file.Name())

		fmt.Print(">>> ")
		cmd, _ := reader.ReadString('\n')
		cmd = strings.TrimSpace(cmd)
		switch cmd {
		case "n":
			pageNo++
		case "p":
			if pageNo > 0 {
				pageNo--
			} else {
				fmt.Println("already first page")
			}
		case "q":
			fmt.Println("quit")
			return
		default:
			fmt.Println("unknown command:", cmd)
		}
	}
}

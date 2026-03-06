package ibdparser

import "fmt"

const (
	SizeofBlobPageHeader = 12
)

func init() {
	assertStructSize(BlobPageHeader{}, SizeofBlobPageHeader)
	registerPageParser(PageTypeBlob, parseBlobPage)
}

type BlobPage struct {
	*BasePage
	BlobPageHeader
	Data []byte
}

type BlobPageHeader struct {
	NextPageNo PageNo
	PrevPageNo PageNo
	Length     uint32
}

func parseBlobPage(t *Tablespace, basePage *BasePage, body []byte) (IPage, error) {
	if len(body) < 12 {
		return nil, fmt.Errorf("body too small for blob page")
	}

	header := BlobPageHeader{
		NextPageNo: PageNo(mysqlByteOrder.Uint32(body[0:4])),
		PrevPageNo: PageNo(mysqlByteOrder.Uint32(body[4:8])),
		Length:     mysqlByteOrder.Uint32(body[8:12]),
	}

	dataLen := int(header.Length)
	if dataLen > len(body)-12 {
		dataLen = len(body) - 12
	}

	page := &BlobPage{
		BasePage:       basePage,
		BlobPageHeader: header,
		Data:           body[12 : 12+dataLen],
	}
	return page, nil
}

package ibdparser

import (
	"fmt"
	"strings"
)

// IBufBitmapPage Insert Buffer Bitmap Page
// 用来记录某个范围内的page是否可以使用insert buffer(change buffer)
type IBufBitmapPage struct {
	*BasePage
	Bitmap []byte
}

func (p *IBufBitmapPage) GetPageBits(pageNo uint32) uint8 {

	index := pageNo % 16384

	byteIndex := index / 4
	if int(byteIndex) >= len(p.Bitmap) {
		return 0
	}
	bitOffset := (index % 4) * 2

	b := p.Bitmap[byteIndex]

	return (b >> bitOffset) & 0x03
}
func (p *IBufBitmapPage) String() string {
	sb := &strings.Builder{}
	sb.WriteString(p.BasePage.String())
	count := map[uint8]int{}
	for i := 0; i < 16384; i++ {
		state := p.GetPageBits(uint32(i))
		count[state]++
	}

	sb.WriteString(fmt.Sprintf("Free pages: %d\n", count[0]))
	sb.WriteString(fmt.Sprintf("Buffered pages: %d\n", count[1]))
	sb.WriteString(fmt.Sprintf("Other pages: %d\n", count[2]+count[3]))

	return sb.String()
}
func parseIBufBitmapPage(t *Tablespace, basePage *BasePage, body []byte) (IPage, error) {
	page := &IBufBitmapPage{
		BasePage: basePage,
		Bitmap:   body,
	}
	return page, nil
}

func init() {
	registerPageParser(PageTypeIBufBitmap, parseIBufBitmapPage)
}

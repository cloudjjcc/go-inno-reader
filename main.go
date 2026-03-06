package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/go-inno-reader/ibdparser"
)

var (
	filePath = flag.String("path", "", "file path")
)

func main() {
	flag.Parse()
	if *filePath == "" {
		flag.Usage()
		return
	}
	file, err := os.Open(*filePath)
	if err != nil {
		panic("open file failed:" + err.Error())
	}
	defer file.Close()

	t, err := ibdparser.NewTableSpace(file)
	if err != nil {
		panic("invalid table space:" + err.Error())
	}
	fmt.Println(t.String())
	pageNo := ibdparser.PageNo(1)
	reader := bufio.NewReader(os.Stdin)
	for {

		fmt.Print(">>> ")
		cmd, _ := reader.ReadString('\n')
		cmd = strings.TrimSpace(cmd)
		switch cmd {
		case "n":
			if pageNo < ibdparser.PageNo(t.FspHdr.FSPSize) {
				pageNo++
			} else {
				fmt.Println("already last page")
				continue
			}
		case "p":
			if pageNo > 0 {
				pageNo--
			} else {
				fmt.Println("already first page")
				continue
			}
		case "q":
			fmt.Println("quit")
			return
		case "h":
			// print help info
			fmt.Println("current page:", pageNo)
			fmt.Println("\ninput command(n=next page,p=prev page,q=quit)\n", file.Name())
			continue
		default:
			fmt.Println("unknown command:", cmd)
			continue
		}
		fmt.Printf("\n--- Page %d ---\n", pageNo)
		page, err := t.ReadPage(pageNo)
		if err != nil {
			fmt.Println("parse page failed:", err)
		} else {
			fmt.Print(page)
		}
	}
}

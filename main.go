package main

import (
	"flag"
	"github.com/go-inno-reader/fileparser"
	"os"
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
	defer func() {
		_ = file.Close()
	}()
	fileparser.Parse(file)
}

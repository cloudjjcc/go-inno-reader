package main

import (
	"github.com/mysql-file-tool/fileparser"
	"os"
)

func main() {
	file, err := os.Open("/Users/cloudjjcc/CLionProjects/mysql-server/cmake-build-debug/data/testdb1/tt1.ibd")
	if err != nil {
		panic("open file failed:" + err.Error())
	}
	defer func() {
		_ = file.Close()
	}()
	fileparser.Parse(file)
}

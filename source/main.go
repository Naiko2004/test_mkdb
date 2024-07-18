package main

import (
	"fmt"
	"runtime"

	btree "test_mkdb/util"
)

func main() {
	fmt.Println("Go version: ", runtime.Version())

	var node btree.BNode
	fmt.Println(node)
}

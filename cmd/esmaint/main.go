package main

import (
	"log"
	"os"
)

func exit(err *error) {
	if *err != nil {
		log.Printf("错误退出: %s", (*err).Error())
		os.Exit(1)
	} else {
		log.Println("正常退出")
	}
}

func main() {
	var err error
	defer exit(&err)
}

package main

import (
	"bitcask/internal/store"
)

func main() {
	//Open sample.txt
	s, err := store.Open("./sample.data")
	if err != nil {
		print("errror: ")
	}

	inf := s.Delete("4")
	if inf != nil {
		print(inf.Error())
	}

	_ = s.Close()

}

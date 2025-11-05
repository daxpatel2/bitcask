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

	defer s.Close()

	//errr := s.Put("4", "Storing this as a main function on key 4")
	//if errr != nil {
	//	print(errr.Error())
	//}

	//by, errrr := s.Get("4")
	//if errrr != nil {
	//	print(errrr.Error())
	//}
	//fmt.Printf(string(by))

	inf := s.Delete("4")
	if inf != nil {
		print(inf.Error())
	}

}

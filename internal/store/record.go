package store

import "fmt"

func formatPayload(key string, value string) []byte {
	//create the header using the struct
	//create the payload
	//append it together and return
	// for now lets just do the normal payload, todo the header later

	//write it as a comma seperated parit
	// i.e key, value /n -> important to do /n for new line
	var payload = []byte(fmt.Sprintf("%s,%s\n", key, value))
	return payload
}

package store

/*
The file header would contain metadata like: time_of_write, bytes
*/
type contentHeader struct {
	timestamp int64
	keyLen    uint32
	valueLen  uint32
}

/*The contents of the file: (key, value) where key is an identifer for that specific data, and values is the values to write (data)*/
type contentItems struct {
	key   string
	value string
}

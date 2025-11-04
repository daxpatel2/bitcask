package store

import (
	"os"
)

/*
Checks if the file exists given the path.
*/
func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		// file exists, return true and error = nil
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	// fall back for generic error catch
	return false, err
}

func appendFile(path string, data []byte) error {
	file, err := os.OpenFile(
		path,
		os.O_APPEND,
		0644,
	)

	if err != nil {
		return err
	}

	defer file.Close()

	_, err = file.Write(data)

	return err
}

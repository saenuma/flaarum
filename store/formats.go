package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type DataF1Elem struct {
	DataKey   string
	DataBegin int64
	DataEnd   int64
}

func ParseDataF1File(path string) (map[string]DataF1Elem, error) {
	ret := make(map[string]DataF1Elem, 0)
	rawF1File, err := os.ReadFile(path)
	if err != nil {
		return ret, err
	}

	cleanedF1File := strings.ReplaceAll(string(rawF1File), "\r", "")
	partsOfRawF1File := strings.Split(cleanedF1File, "\n\n")
	for _, part := range partsOfRawF1File {
		innerParts := strings.Split(strings.TrimSpace(part), "\n")

		var elem DataF1Elem
		for _, line := range innerParts {
			var colonIndex int
			for i, ch := range line {
				if fmt.Sprintf("%c", ch) == ":" {
					colonIndex = i
					break
				}
			}

			if colonIndex == 0 {
				continue
			}

			optName := strings.TrimSpace(line[0:colonIndex])
			optValue := strings.TrimSpace(line[colonIndex+1:])

			if optName == "data_key" {
				elem.DataKey = optValue
			} else if optName == "data_begin" {
				data, err := strconv.ParseInt(optValue, 10, 64)
				if err != nil {
					return ret, errors.New("data_begin is not of type int64")
				}
				elem.DataBegin = data
			} else if optName == "data_end" {
				data, err := strconv.ParseInt(optValue, 10, 64)
				if err != nil {
					return ret, errors.New("data_end is not of type int64")
				}
				elem.DataEnd = data
			}
		}

		if elem.DataKey == "" {
			continue
		}
		ret[elem.DataKey] = elem
	}

	return ret, nil
}

func AppendDataF1File(projName, tableName, name string, elem DataF1Elem) error {
	dataPath, _ := GetDataPath()
	path := filepath.Join(dataPath, projName, tableName, name+".flaa1")
	dataF1Handle, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return errors.Wrap(err, "os error")
	}
	defer dataF1Handle.Close()

	out := fmt.Sprintf("data_key: %s\ndata_begin: %d\ndata_end:%d\n\n", elem.DataKey,
		elem.DataBegin, elem.DataEnd)

	_, err = dataF1Handle.Write([]byte(out))
	if err != nil {
		return errors.Wrap(err, "os error")
	}

	return nil
}

func ReadPortionF2File(projName, tableName, name string, begin, end int64) ([]byte, error) {
	dataPath, _ := GetDataPath()
	path := filepath.Join(dataPath, projName, tableName, name+".flaa2")
	f2FileHandle, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0777)
	if err != nil {
		return []byte{}, errors.Wrap(err, "os error")
	}
	defer f2FileHandle.Close()

	outData := make([]byte, end-begin)
	_, err = f2FileHandle.ReadAt(outData, begin)
	if err != nil {
		return outData, errors.Wrap(err, "os error")
	}

	return outData, nil
}

func RewriteF1File(projName, tableName, name string, elems map[string]DataF1Elem) error {
	dataPath, _ := GetDataPath()
	path := filepath.Join(dataPath, projName, tableName, name+".flaa1")

	out := "\n"
	for _, elem := range elems {
		out += fmt.Sprintf("data_key: %s\ndata_begin: %d\ndata_end:%d\n\n", elem.DataKey,
			elem.DataBegin, elem.DataEnd)
	}

	err := os.WriteFile(path, []byte(out), 0777)
	if err != nil {
		return errors.Wrap(err, "os error")
	}

	return nil
}

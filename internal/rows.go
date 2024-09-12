package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

func ParseDataFormat(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return ParseEncodedRowData(raw)
}

func ParseEncodedRowData(rawData []byte) (map[string]string, error) {
	ret := make(map[string]string)

	cleanedRawData := strings.ReplaceAll(string(rawData), "\r\n", "\n")
	partsOfRawData := strings.Split(cleanedRawData, "\n")
	for _, line := range partsOfRawData {
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

		ret[optName] = optValue

	}

	rawDataStr := string(rawData)
	for k, v := range ret {
		if strings.TrimSpace(v) == "" {
			firstIndex := strings.Index(rawDataStr, fmt.Sprintf("\n%s:", k))
			lastIndex := strings.LastIndex(rawDataStr, fmt.Sprintf("\n%s:", k))
			padding := len(fmt.Sprintf("\n%s:", k))
			if firstIndex != lastIndex {
				ret[k] = rawDataStr[firstIndex+padding : lastIndex]
			}
		}
	}

	return ret, nil
}

func EncodeRowData(projName, tableName string, toWrite map[string]string) string {
	out := "\n"
	for k, v := range toWrite {
		ft := GetFieldType(projName, tableName, k)
		if ft == "text" {
			out += fmt.Sprintf("%s:\n%s\n%s:\n", k, v, k)
		} else {
			out += fmt.Sprintf("%s: %s\n", k, v)
		}
	}

	return out
}

func SaveRowData(projName, tableName, rowId string, toWrite map[string]string) error {
	tablePath := GetTablePath(projName, tableName)

	dataLumpPath := filepath.Join(tablePath, "data.flaa2")
	dataForCurrentRow := EncodeRowData(projName, tableName, toWrite)
	var begin int64
	var end int64
	if DoesPathExists(dataLumpPath) {
		dataLumpHandle, err := os.OpenFile(dataLumpPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return errors.Wrap(err, "os error")
		}
		defer dataLumpHandle.Close()

		stat, err := dataLumpHandle.Stat()
		if err != nil {
			return errors.Wrap(err, "os error")
		}

		size := stat.Size()
		dataLumpHandle.Write([]byte(dataForCurrentRow))
		begin = size
		end = int64(len([]byte(dataForCurrentRow))) + size
	} else {
		err := os.WriteFile(dataLumpPath, []byte(dataForCurrentRow), 0777)
		if err != nil {
			return errors.Wrap(err, "os error")
		}

		begin = 0
		end = int64(len([]byte(dataForCurrentRow)))
	}

	elem := DataF1Elem{rowId, begin, end}
	err := AppendDataF1File(projName, tableName, "data", elem)
	if err != nil {
		return errors.Wrap(err, "os error")
	}

	return nil
}

package internal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	"github.com/pkg/errors"
)

func MakeIndex(projName, tableName, fieldName, newData, rowId string) error {
	dataPath, _ := GetDataPath()
	indexesPath := filepath.Join(dataPath, projName, tableName, fieldName+"_indexes.json")

	indexesObj := make(map[string][]string)
	if !DoesPathExists(indexesPath) {
		indexesObj[newData] = []string{rowId}
	} else {
		rawJson, err := os.ReadFile(indexesPath)
		if err != nil {
			return errors.Wrap(err, "file read error")
		}
		err = json.Unmarshal(rawJson, &indexesObj)
		if err != nil {
			return errors.Wrap(err, "json error")
		}
		indexesSlice, ok := indexesObj[newData]
		if ok {
			if !slices.Contains(indexesSlice, rowId) {
				indexesSlice = append(indexesSlice, rowId)
				indexesObj[newData] = indexesSlice
			}
		} else {
			indexesObj[newData] = []string{rowId}
		}
	}

	indexesJson, err := json.Marshal(indexesObj)
	if err != nil {
		return errors.Wrap(err, "json error")
	}

	os.WriteFile(indexesPath, indexesJson, 0777)
	return nil
}

func IsNotIndexedFieldVersioned(projName, tableName, fieldName, version string) bool {
	versionInt, _ := strconv.Atoi(version)
	ts, _ := GetTableStructureParsed(projName, tableName, versionInt)
	for _, fd := range ts.Fields {
		if fd.FieldName == fieldName && fd.NotIndexed {
			return true
		}
	}

	for _, fd := range ts.Fields {
		if fd.FieldName == fieldName && fd.FieldType == "text" {
			return true
		}
	}

	return false
}

func DeleteIndex(projName, tableName, fieldName, data, rowId, version string) error {

	dataPath, _ := GetDataPath()
	indexesPath := filepath.Join(dataPath, projName, tableName, fieldName+"_indexes.json")

	indexesObj := make(map[string][]string)
	if DoesPathExists(indexesPath) {
		rawJson, err := os.ReadFile(indexesPath)
		if err != nil {
			return errors.Wrap(err, "file read error")
		}
		err = json.Unmarshal(rawJson, &indexesObj)
		if err != nil {
			return errors.Wrap(err, "json error")
		}

		indexesSlice, ok := indexesObj[data]
		if ok {
			if slices.Contains(indexesSlice, rowId) {
				idx := slices.Index(indexesSlice, rowId)
				indexesSlice = slices.Delete(indexesSlice, idx, idx+1)
				indexesObj[data] = indexesSlice
			}
		}
	}

	indexesJson, err := json.Marshal(indexesObj)
	if err != nil {
		return errors.Wrap(err, "json error")
	}

	os.WriteFile(indexesPath, indexesJson, 0777)
	return nil
}

func IsNotIndexedField(projName, tableName, fieldName string) bool {
	ts, _ := GetCurrentTableStructureParsed(projName, tableName)
	for _, fd := range ts.Fields {
		if fd.FieldName == fieldName && fd.NotIndexed {
			return true
		}
	}

	for _, fd := range ts.Fields {
		if fd.FieldName == fieldName && fd.FieldType == "text" {
			return true
		}
	}

	return false
}

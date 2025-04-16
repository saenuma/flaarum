package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	arrayOperations "github.com/adam-hanna/arrayOperations"
	"github.com/pkg/errors"
	"github.com/saenuma/flaarumlib"
)

func MakeIndex(projName, tableName, fieldName, newData, rowId string) error {
	// make exact search indexes
	dataPath, _ := GetDataPath()
	indexesF1Path := filepath.Join(dataPath, projName, tableName, fieldName+"_indexes.flaa1")
	indexesF2Path := filepath.Join(dataPath, projName, tableName, fieldName+"_indexes.flaa2")

	var begin int64
	var end int64
	if !DoesPathExists(indexesF1Path) {
		begin = 0
		err := os.WriteFile(indexesF2Path, []byte(rowId+","), 0777)
		if err != nil {
			return errors.Wrap(err, "os error")
		}
		end = int64(len([]byte(rowId + ",")))
	} else {
		elemsMap, err := ParseDataF1File(indexesF1Path)
		if err != nil {
			return err
		}

		elem, ok := elemsMap[newData]
		var newDataToWrite string
		if !ok {
			newDataToWrite = rowId + ","
		} else {
			readBytes, err := ReadPortionF2File(projName, tableName, fieldName+"_indexes", elem.DataBegin, elem.DataEnd)
			if err != nil {
				return err
			}
			previousEntries := strings.Split(string(readBytes), ",")
			newEntries := arrayOperations.Union(previousEntries, []string{rowId})
			newDataToWrite = strings.Join(newEntries, ",") + ","
		}

		f2IndexesHandle, err := os.OpenFile(indexesF2Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return errors.Wrap(err, "os error")
		}
		defer f2IndexesHandle.Close()

		stat, err := f2IndexesHandle.Stat()
		if err != nil {
			return errors.Wrap(err, "os error")
		}

		size := stat.Size()
		f2IndexesHandle.Write([]byte(newDataToWrite))
		begin = size
		end = int64(len([]byte(newDataToWrite))) + size
	}

	elem := DataF1Elem{newData, begin, end}
	err := AppendDataF1File(projName, tableName, fieldName+"_indexes", elem)
	if err != nil {
		return errors.Wrap(err, "os error")
	}

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

	if ConfirmFieldType(projName, tableName, fieldName, "date", version) {
		valueInTimeType, err := time.Parse(flaarumlib.DATE_FORMAT, data)
		if err != nil {
			return errors.New(fmt.Sprintf("The value '%s' to field '%s' is not a date.", data, fieldName))
		}

		dMap := make(map[string]string)
		f := fieldName
		dMap[f+"_year"] = strconv.Itoa(valueInTimeType.Year())
		dMap[f+"_month"] = strconv.Itoa(int(valueInTimeType.Month()))
		dMap[f+"_day"] = strconv.Itoa(valueInTimeType.Day())

		for toDeleteField, fieldData := range dMap {
			err := DeleteIndex(projName, tableName, toDeleteField, fieldData, rowId, version)
			if err != nil {
				return err
			}
		}

	} else if ConfirmFieldType(projName, tableName, fieldName, "datetime", version) {
		valueInTimeType, err := time.Parse(flaarumlib.DATETIME_FORMAT, data)
		if err != nil {
			return errors.New(fmt.Sprintf("The value '%s' to field '%s' is not a datetime.", data, fieldName))
		}

		dMap := make(map[string]string)
		f := fieldName
		dMap[f+"_year"] = strconv.Itoa(valueInTimeType.Year())
		dMap[f+"_month"] = strconv.Itoa(int(valueInTimeType.Month()))
		dMap[f+"_day"] = strconv.Itoa(valueInTimeType.Day())
		dMap[f+"_hour"] = strconv.Itoa(valueInTimeType.Hour())
		dMap[f+"_date"] = valueInTimeType.Format(flaarumlib.DATE_FORMAT)
		dMap[f+"_tzname"], _ = valueInTimeType.Zone()

		for toDeleteField, fieldData := range dMap {
			err := DeleteIndex(projName, tableName, toDeleteField, fieldData, rowId, version)
			if err != nil {
				return err
			}
		}

	}

	indexesF1Path := filepath.Join(dataPath, projName, tableName, fieldName+"_indexes.flaa1")
	// update flaa1 file by rewriting it.
	elemsMap, err := ParseDataF1File(indexesF1Path)
	if err != nil {
		return err
	}

	elem, ok := elemsMap[data]
	if !ok {
		return nil
	}

	readBytes, err := ReadPortionF2File(projName, tableName, fieldName+"_indexes",
		elem.DataBegin, elem.DataEnd)
	if err != nil {
		fmt.Println("Bad indexes file")
		fmt.Printf("%+v\n", err)
	}
	similarIds := strings.Split(string(readBytes), ",")
	toWriteIds := make([]string, 0)
	for _, oldId := range similarIds {
		if oldId != rowId {
			toWriteIds = append(toWriteIds, oldId)
		}
	}

	if len(toWriteIds) == 0 {
		delete(elemsMap, data)
		err = RewriteF1File(projName, tableName, fieldName+"_indexes", elemsMap)
		if err != nil {
			return err
		}

	} else {
		tablePath := GetTablePath(projName, tableName)
		indexesF2Path := filepath.Join(tablePath, fieldName+"_indexes.flaa2")
		toWriteData := strings.Join(toWriteIds, ",")

		var begin int64
		var end int64
		if DoesPathExists(indexesF2Path) {
			indexesF2Handle, err := os.OpenFile(indexesF2Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
			if err != nil {
				return errors.Wrap(err, "os error")
			}
			defer indexesF2Handle.Close()

			stat, err := indexesF2Handle.Stat()
			if err != nil {
				return errors.Wrap(err, "os error")
			}

			size := stat.Size()
			indexesF2Handle.Write([]byte(toWriteData))
			begin = size
			end = int64(len([]byte(toWriteData))) + size
		} else {
			err := os.WriteFile(indexesF2Path, []byte(toWriteData), 0777)
			if err != nil {
				return errors.Wrap(err, "os error")
			}

			begin = 0
			end = int64(len([]byte(toWriteData)))
		}

		elem := DataF1Elem{data, begin, end}
		err = AppendDataF1File(projName, tableName, fieldName+"_indexes", elem)
		if err != nil {
			return errors.Wrap(err, "os error")
		}
	}

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

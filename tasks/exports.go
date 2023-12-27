package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/otiai10/copy"
	"github.com/saenuma/flaarum/flaarum_shared"
)

const ROW_QTY_PER_FILE = 100000

func exportAsJSON(projName, tableName, eventPath string) {
	cl := getFlaarumCLIClient()
	cl.ProjName = projName

	if !flaarum_shared.DoesTableExists(projName, tableName) {
		P(fmt.Errorf("table '%s' of project '%s' does not exists", tableName, projName))
		return
	}

	dataPath, _ := flaarum_shared.GetDataPath()

	eOutBasePath := filepath.Join(dataPath,
		fmt.Sprintf("flaarum_export_%s_%s_%s", time.Now().Format("20060102T150405"), projName, tableName))

	os.MkdirAll(eOutBasePath, 0777)

	allRowCount, err := cl.AllRowsCount(tableName)
	if err != nil {
		P(err)
		return
	}

	// export data
	count := 1
	for i := int64(0); i < allRowCount; i += ROW_QTY_PER_FILE {
		stmt := fmt.Sprintf(`
		table: %s
		start_index: %d
		limit: %d
	`, tableName, i, i+ROW_QTY_PER_FILE)

		rows, err := cl.Search(stmt)
		if err != nil {
			P(err)
			return
		}

		rawJson, _ := json.Marshal(rows)

		os.WriteFile(filepath.Join(eOutBasePath, fmt.Sprintf("%d.json", count)), rawJson, 0777)
		count += 1
	}

	// export table structure
	tablePath := filepath.Join(dataPath, projName, tableName)
	index := 1
	for {
		testedStructurePath := filepath.Join(tablePath, fmt.Sprintf("structure%d.txt", index))
		newStructurePath := filepath.Join(eOutBasePath, fmt.Sprintf("structure%d.txt", index))
		if flaarum_shared.DoesPathExists(testedStructurePath) {
			copy.Copy(testedStructurePath, newStructurePath)
		} else {
			break
		}
	}

	os.RemoveAll(eventPath)
}

func exportAsCSV(projName, tableName, eventPath string) {
	cl := getFlaarumCLIClient()
	cl.ProjName = projName

	if !flaarum_shared.DoesTableExists(projName, tableName) {
		P(fmt.Errorf("table '%s' of project '%s' does not exists", tableName, projName))
		return
	}

	dataPath, _ := flaarum_shared.GetDataPath()

	eOutBasePath := filepath.Join(dataPath,
		fmt.Sprintf("flaarum_export_%s_%s_%s", time.Now().Format("20060102T150405"), projName, tableName))

	os.MkdirAll(eOutBasePath, 0777)

	versionNum, _ := cl.GetCurrentTableVersionNum(tableName)
	tableStruct, _ := cl.GetTableStructureParsed(tableName, versionNum)
	headers := make([]string, 0)
	for _, fieldStruct := range tableStruct.Fields {
		if fieldStruct.FieldType == "text" {
			P(fmt.Errorf("table field '%s' is of type text and hence cannot be exported to CSV", fieldStruct.FieldName))
			return
		}

		headers = append(headers, fieldStruct.FieldName)
	}

	allRowCount, err := cl.AllRowsCount(tableName)
	if err != nil {
		P(err)
		return
	}

	fileHandle, err := os.Create(filepath.Join(eOutBasePath, "data.csv"))
	if err != nil {
		P(err)
		return
	}
	defer fileHandle.Close()

	csvWriter := csv.NewWriter(fileHandle)

	for i := int64(0); i < allRowCount; i += ROW_QTY_PER_FILE {
		stmt := fmt.Sprintf(`
		table: %s
		start_index: %d
		limit: %d
	`, tableName, i, i+ROW_QTY_PER_FILE)

		rows, err := cl.Search(stmt)
		if err != nil {
			P(err)
			return
		}

		for _, row := range *rows {
			toWriteList := make([]string, 0)

			for _, header := range headers {
				v := row[header]
				var stringOfV string
				switch vInType := v.(type) {
				case int:
					vInStr := strconv.Itoa(vInType)
					stringOfV = vInStr
				case int64:
					vInStr := strconv.FormatInt(vInType, 10)
					stringOfV = vInStr
				case bool:
					var vInStr string
					if vInType {
						vInStr = "t"
					} else if !vInType {
						vInStr = "f"
					}
					stringOfV = vInStr
				case string:
					stringOfV = vInType
				}

				toWriteList = append(toWriteList, stringOfV)
			}

			csvWriter.Write(toWriteList)
		}

		csvWriter.Flush()

	}

	// export table structure
	tablePath := filepath.Join(dataPath, projName, tableName)
	index := 1
	for {
		testedStructurePath := filepath.Join(tablePath, fmt.Sprintf("structure%d.txt", index))
		newStructurePath := filepath.Join(eOutBasePath, fmt.Sprintf("structure%d.txt", index))
		if flaarum_shared.DoesPathExists(testedStructurePath) {
			copy.Copy(testedStructurePath, newStructurePath)
		} else {
			break
		}
	}

	os.RemoveAll(eventPath)
}

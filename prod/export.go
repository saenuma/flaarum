package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gookit/color"
	"github.com/saenuma/flaarum/internal"
	flaarum "github.com/saenuma/flaarumlib"
	"github.com/tidwall/pretty"
)

const VersionFormat = "20060102T150405MST"

func export(projName, format string) {
	tables, err := internal.ListTables(projName)
	if err != nil {
		color.Red.Println(err)
		os.Exit(1)
	}

	for _, tableName := range tables {
		exportTable(projName, tableName, format)
	}
}

func exportTable(project, table, format string) {
	var keyStr string
	inProd := internal.GetSetting("in_production")
	if inProd == "" {
		color.Red.Println("unexpected error. Have you installed  and launched flaarum?")
		os.Exit(1)
	}
	if inProd == "true" {
		keyStrPath := internal.GetKeyStrPath()
		raw, err := os.ReadFile(keyStrPath)
		if err != nil {
			color.Red.Println(err)
			os.Exit(1)
		}
		keyStr = string(raw)
	} else {
		keyStr = "not-yet-set"
	}
	port := internal.GetSetting("port")
	if port == "" {
		color.Red.Println("unexpected error. Have you installed  and launched flaarum?")
		os.Exit(1)
	}
	var cl flaarum.Client

	portInt, err := strconv.Atoi(port)
	if err != nil {
		color.Red.Println("Invalid port setting.")
		os.Exit(1)
	}

	if portInt != internal.PORT {
		cl = flaarum.NewClientCustomPort("127.0.0.1", keyStr, project, portInt)
	} else {
		cl = flaarum.NewClient("127.0.0.1", keyStr, project)
	}

	err = cl.Ping()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	stmt := fmt.Sprintf(`
		table: %s
		order_by: id asc
	`, table)
	rows, err := cl.Search(stmt)
	if err != nil {
		color.Red.Printf("Error running search '%s'.\nError: %s\n", os.Args[3], err)
		os.Exit(1)
	}

	rootPath, err := internal.GetDataPath()
	if err != nil {
		color.Red.Printf("Error ocurred.\nError: %s\n", err)
		os.Exit(1)
	}

	if format == "json" {
		jsonBytes, err := json.Marshal(*rows)
		if err != nil {
			color.Red.Printf("Error ocurred.\nError: %s\n", err)
			os.Exit(1)
		}

		prettyJson := pretty.Pretty(jsonBytes)
		outFilePath := filepath.Join(rootPath, project, table+time.Now().Format(VersionFormat)+".json")
		err = os.WriteFile(outFilePath, prettyJson, 0777)
		if err != nil {
			color.Red.Println("Error writing to file")
			os.Exit(1)
		}

		fmt.Printf("Exported to : %s\n", outFilePath)

	} else if format == "csv" {

		versionNum, _ := cl.GetCurrentTableVersionNum(table)
		tableStruct, _ := cl.GetTableStructureParsed(table, versionNum)
		headers := []string{"id", "_version"}
		for _, fieldStruct := range tableStruct.Fields {
			// if fieldStruct.FieldType == "text" {
			// 	color.Red.Printf("table field '%s' is of type text and hence cannot be exported to CSV\n", fieldStruct.FieldName)
			// 	return
			// }

			headers = append(headers, fieldStruct.FieldName)
		}

		outFilePath := filepath.Join(rootPath, project, table+time.Now().Format(VersionFormat)+".csv")
		fileHandle, err := os.Create(outFilePath)
		if err != nil {
			color.Red.Println("Error creating file " + outFilePath)
			os.Exit(1)
		}
		defer fileHandle.Close()

		csvWriter := csv.NewWriter(fileHandle)
		csvWriter.Write(headers)

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

		fmt.Printf("Exported to : %s\n", outFilePath)
	}

}

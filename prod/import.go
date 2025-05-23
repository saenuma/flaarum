package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/gookit/color"
	"github.com/saenuma/flaarum/internal"
)

func importProject(project, format, path string) {
	dirFIs, err := os.ReadDir(path)
	if err != nil {
		color.Red.Println(err.Error())
		os.Exit(1)
	}

	for _, dirFI := range dirFIs {
		if strings.HasSuffix(dirFI.Name(), "."+format) {
			tableName := strings.ReplaceAll(dirFI.Name(), "."+format, "")
			importTable(project, tableName, format, filepath.Join(path, dirFI.Name()))
		}
	}
}

func importTable(project, table, format, srcPath string) {
	cl := internal.GetLocalFlaarumClient(project)
	existingTables, err := cl.ListTables()
	if err != nil {
		color.Red.Println(err.Error())
		os.Exit(1)
	}

	if !slices.Contains(existingTables, table) {
		color.Red.Printf("table %s does not exists\n", table)
		os.Exit(1)
	}
	tablePath := internal.GetTablePath(project, table)

	// use RAM to speed up import operation
	var buffer bytes.Buffer
	writer := io.Writer(&buffer)
	elemsSlice := make([]internal.DataF1Elem, 0)
	var begin int64

	if format == "json" {
		rawJSON, err := os.ReadFile(srcPath)
		if err != nil {
			color.Red.Println(err.Error())
			os.Exit(1)
		}

		objs := make([]map[string]any, 0)
		err = json.Unmarshal(rawJSON, &objs)
		if err != nil {
			color.Red.Println(err.Error())
			os.Exit(1)
		}

		for _, obj := range objs {
			toWrite, err := cl.ConvertInterfaceMapToStringMap(table, obj)
			if err != nil {
				fmt.Println(err)
				continue
			}
			dataForCurrentRow := internal.EncodeRowData(project, table, toWrite)
			writer.Write([]byte(dataForCurrentRow))

			dataEnd := int64(len([]byte(dataForCurrentRow)))
			elem := internal.DataF1Elem{DataKey: toWrite["id"], DataBegin: begin, DataEnd: begin + dataEnd}
			elemsSlice = append(elemsSlice, elem)
			begin += dataEnd
		}

	} else if format == "csv" {

		f, err := os.Open(srcPath)
		if err != nil {
			color.Red.Println(err.Error())
			os.Exit(1)
		}
		defer f.Close()

		csvReader := csv.NewReader(f)
		records, err := csvReader.ReadAll()
		if err != nil {
			color.Red.Println(err.Error())
			os.Exit(1)
		}

		for _, record := range records[1:] {
			toWrite := make(map[string]string)
			for i, f := range record {
				toWrite[records[0][i]] = f
			}

			dataForCurrentRow := internal.EncodeRowData(project, table, toWrite)
			writer.Write([]byte(dataForCurrentRow))

			dataEnd := int64(len([]byte(dataForCurrentRow)))
			elem := internal.DataF1Elem{DataKey: toWrite["id"], DataBegin: begin, DataEnd: begin + dataEnd}
			elemsSlice = append(elemsSlice, elem)
			begin += dataEnd
		}

	}

	lastIdPath := filepath.Join(tablePath, "lastId.txt")

	lastId := elemsSlice[len(elemsSlice)-1].DataKey
	os.WriteFile(lastIdPath, []byte(lastId), 0777)

	dataLumpPath := filepath.Join(tablePath, "data.flaa2")
	os.WriteFile(dataLumpPath, buffer.Bytes(), 0777)

	var out string
	for _, elem := range elemsSlice {
		out += fmt.Sprintf("data_key: %s\ndata_begin: %d\ndata_end:%d\n\n", elem.DataKey,
			elem.DataBegin, elem.DataEnd)
	}
	dataIndexPath := filepath.Join(tablePath, "data.flaa1")
	os.WriteFile(dataIndexPath, []byte(out), 0777)

	// do reindexing
	reIndex(project, table)

}

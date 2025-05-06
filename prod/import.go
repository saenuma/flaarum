package main

import (
	"encoding/csv"
	"encoding/json"
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

	if format == "json" {
		rawJSON, err := os.ReadFile(srcPath)
		if err != nil {
			color.Red.Println(err.Error())
			os.Exit(1)
		}

		objs := make([]map[string]string, 0)
		err = json.Unmarshal(rawJSON, &objs)
		if err != nil {
			color.Red.Println(err.Error())
			os.Exit(1)
		}

		for _, toWrite := range objs {
			_, err := cl.InsertRowStr(table, toWrite)
			if err != nil {
				color.Red.Println(err)
			}

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

			_, err := cl.InsertRowStr(table, toWrite)
			if err != nil {
				color.Red.Println(err)
			}
		}
	}
}

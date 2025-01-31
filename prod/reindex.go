package main

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/internal"
)

func reIndex(projName, tableName string) error {
	dataPath, _ := internal.GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)
	tmpTableName := tableName + "_ridx_tmp"
	workingTablePath := filepath.Join(dataPath, projName, tmpTableName)

	if internal.DoesPathExists(workingTablePath) {
		os.RemoveAll(workingTablePath)
	}

	os.MkdirAll(workingTablePath, 0777)

	// copy the data to the temporary table
	rawDataFlaa1, err := os.ReadFile(filepath.Join(tablePath, "data.flaa1"))
	if err != nil {
		return errors.Wrap(err, "file read error")
	}
	workingF1Path := filepath.Join(workingTablePath, "data.flaa1")
	os.WriteFile(workingF1Path, rawDataFlaa1, 0777)

	rawDataFlaa2, err := os.ReadFile(filepath.Join(tablePath, "data.flaa2"))
	if err != nil {
		return errors.Wrap(err, "file read error")
	}
	workingF2Path := filepath.Join(workingTablePath, "data.flaa2")
	os.WriteFile(workingF2Path, rawDataFlaa2, 0777)

	// copy the structures to the new table folder
	dirFIs, err := os.ReadDir(tablePath)
	if err != nil {
		return errors.Wrap(err, "directory read error")
	}
	for _, dirFI := range dirFIs {
		if strings.HasPrefix(dirFI.Name(), "structure") && strings.HasSuffix(dirFI.Name(), ".txt") {
			oldStructPath := filepath.Join(tablePath, dirFI.Name())
			raw, _ := os.ReadFile(oldStructPath)
			newStructPath := filepath.Join(workingTablePath, dirFI.Name())
			os.WriteFile(newStructPath, raw, 0777)
		}
	}

	elemsMap, _ := internal.ParseDataF1File(workingF1Path)

	// get all the fields in the data
	fields := make([]string, 0)
	for _, elem := range elemsMap {

		rawRowData, err := internal.ReadPortionF2File(projName, tmpTableName, "data",
			elem.DataBegin, elem.DataEnd)
		if err != nil {
			return err
		}

		rowMap, err := internal.ParseEncodedRowData(rawRowData)
		if err != nil {
			fmt.Println(err)
			continue
		}

		for k := range rowMap {
			if !slices.Contains(fields, k) {
				fields = append(fields, k)
			}
		}

	}

	var wg sync.WaitGroup
	for _, field := range fields {
		if field == "id" {
			continue
		}

		wg.Add(1)
		go func(field string) {
			defer wg.Done()

			for _, elem := range elemsMap {
				rawRowData, err := internal.ReadPortionF2File(projName, tmpTableName, "data",
					elem.DataBegin, elem.DataEnd)
				if err != nil {
					fmt.Println(err)
					continue
				}

				rowMap, err := internal.ParseEncodedRowData(rawRowData)
				if err != nil {
					fmt.Println(err)
					continue
				}

				if !internal.IsNotIndexedField(projName, tmpTableName, field) {
					err := internal.MakeIndex(projName, tmpTableName, field, rowMap[field], elem.DataKey)
					if err != nil {
						fmt.Println(err)
					}
				}
			}

		}(field)
	}
	wg.Wait()

	// delete old table and make temporary default.
	os.RemoveAll(tablePath)
	os.Rename(workingTablePath, tablePath)

	return nil
}

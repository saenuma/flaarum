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

func trimFlaarumFilesProject(projName string) error {

	tables, err := internal.ListTables(projName)
	if err != nil {
		return err
	}

	for _, tableName := range tables {
		err := trimFlaarumFilesTable(projName, tableName)
		if err != nil {
			return err
		}
	}
	return nil
}

func trimFlaarumFilesTable(projName, tableName string) error {

	dataPath, _ := internal.GetRootPath()
	tablePath := filepath.Join(dataPath, projName, tableName)
	tmpTableName := tableName + "_trim_tmp"
	workingTablePath := filepath.Join(dataPath, projName, tmpTableName)

	if internal.DoesPathExists(workingTablePath) {
		os.RemoveAll(workingTablePath)
	}

	os.MkdirAll(workingTablePath, 0777)

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

	refF1Path := filepath.Join(tablePath, "data.flaa1")
	tmpF2Path := filepath.Join(dataPath, projName, tmpTableName, "data.flaa2")
	elemsMap, _ := internal.ParseDataF1File(refF1Path)

	// trim the data files
	for _, elem := range elemsMap {
		rawRowData, err := internal.ReadPortionF2File(projName, tableName, "data",
			elem.DataBegin, elem.DataEnd)
		if err != nil {
			fmt.Printf("%+v", errors.Wrap(err, "read error"))
			continue
		}

		tmpIndexesHandle, err := os.OpenFile(tmpF2Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer tmpIndexesHandle.Close()

		stat, err := tmpIndexesHandle.Stat()

		if err != nil {
			fmt.Println("stats error", err)
			continue
		}

		size := stat.Size()
		tmpIndexesHandle.Write(rawRowData)
		begin := size
		end := int64(len(rawRowData)) + size

		newDataElem := internal.DataF1Elem{DataKey: elem.DataKey, DataBegin: begin, DataEnd: end}
		err = internal.AppendDataF1File(projName, tmpTableName, "data", newDataElem)
		if err != nil {
			fmt.Println(err)
			continue
		}

	}

	// get all the fields in the data
	fields := make([]string, 0)
	for _, elem := range elemsMap {

		rawRowData, err := internal.ReadPortionF2File(projName, tableName, "data",
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

	// do triming
	var wg sync.WaitGroup
	for _, fieldName := range fields {
		if fieldName == "id" {
			continue
		}

		wg.Add(1)
		go func(fieldName string) {
			defer wg.Done()

			indexesF1Path := filepath.Join(dataPath, projName, tableName, fieldName+"_indexes.flaa1")
			tmpIndexesF2Path := filepath.Join(dataPath, projName, tmpTableName, fieldName+"_indexes.flaa2")

			indexesF1ElemsMap, _ := internal.ParseDataF1File(indexesF1Path)

			for _, idxElem := range indexesF1ElemsMap {
				idxElemDataFromF2, err := internal.ReadPortionF2File(projName, tableName, fieldName+"_indexes",
					idxElem.DataBegin, idxElem.DataEnd)
				if err != nil {
					fmt.Println(err)
					continue
				}

				tmpIndexesHandle, err := os.OpenFile(tmpIndexesF2Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
				if err != nil {
					fmt.Println(err)
					continue
				}
				defer tmpIndexesHandle.Close()

				stat, err := tmpIndexesHandle.Stat()
				if err != nil {
					fmt.Println(err)
					continue
				}

				size := stat.Size()
				tmpIndexesHandle.Write(idxElemDataFromF2)
				begin := size
				end := int64(len(idxElemDataFromF2)) + size

				newIdxElem := internal.DataF1Elem{DataKey: idxElem.DataKey, DataBegin: begin, DataEnd: end}
				err = internal.AppendDataF1File(projName, tmpTableName, fieldName+"_indexes", newIdxElem)
				if err != nil {
					fmt.Println(err)
					continue
				}
			}
		}(fieldName)
	}

	wg.Wait()

	// delete old table and make temporary default.
	os.RemoveAll(tablePath)
	os.Rename(workingTablePath, tablePath)

	return nil
}

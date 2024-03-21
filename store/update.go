package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/flaarum_shared"
)

func updateRows(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]

	stmt := r.FormValue("stmt")
	stmtStruct, err := flaarum_shared.ParseSearchStmt(stmt)
	if err != nil {
		flaarum_shared.PrintError(w, err)
		return
	}

	tableName := stmtStruct.TableName
	if !doesTableExists(projName, tableName) {
		printValError(w, errors.New(fmt.Sprintf("table '%s' of project '%s' does not exists.", tableName, projName)))
		return
	}

	rows, err := innerSearch(projName, stmt)
	if err != nil {
		flaarum_shared.PrintError(w, err)
		return
	}

	if len(*rows) == 0 {
		flaarum_shared.PrintError(w, errors.New("There is no data to update. The search statement returned nothing."))
		return
	}

	updatedValues := make(map[string]string)
	for j := 1; ; j++ {
		k := r.FormValue("set" + strconv.Itoa(j) + "_k")
		if k == "" {
			break
		}
		updatedValues[k] = r.FormValue("set" + strconv.Itoa(j) + "_v")
	}

	currentVersion, err := getCurrentVersionNum(projName, tableName)
	if err != nil {
		flaarum_shared.PrintError(w, err)
		return
	}
	updatedValues["_version"] = strconv.Itoa(currentVersion)

	tableStruct, err := getCurrentTableStructureParsed(projName, tableName)
	if err != nil {
		flaarum_shared.PrintError(w, err)
		return
	}

	fieldsDescs := make(map[string]flaarum_shared.FieldStruct)
	for _, fd := range tableStruct.Fields {
		fieldsDescs[fd.FieldName] = fd
	}

	patchedRows := make([]map[string]string, 0)
	for _, row := range *rows {
		newRow := make(map[string]string)
		for k, v := range row {
			if k == "id" {
				newRow[k] = v
				continue
			}
			_, ok := fieldsDescs[k]
			if ok {
				newRow[k] = v
			} else {
				if !flaarum_shared.IsNotIndexedFieldVersioned(projName, tableName, k, row["_version"]) {
					flaarum_shared.DeleteIndex(projName, tableName, k, v, row["id"], row["_version"])
				}
			}
		}
		for k, v := range updatedValues {
			newRow[k] = v
		}
		patchedRows = append(patchedRows, newRow)
	}

	// validation
	for i, row := range patchedRows {
		validatedRow, err := validateAndMutateDataMap(projName, tableName, row, (*rows)[i])
		if err != nil {
			printValError(w, err)
			return
		}
		patchedRows[i] = validatedRow
	}

	createTableMutexIfNecessary(projName, tableName)
	fullTableName := projName + ":" + tableName
	tablesMutexes[fullTableName].Lock()
	defer tablesMutexes[fullTableName].Unlock()

	dataPath, _ := flaarum_shared.GetDataPath()
	dataF1Path := filepath.Join(dataPath, projName, tableName, "data.flaa1")

	elemsMap, err := flaarum_shared.ParseDataF1File(dataF1Path)
	if err != nil {
		flaarum_shared.PrintError(w, err)
		return
	}

	// write null data to flaa2 file
	for _, row := range patchedRows {
		tablePath := flaarum_shared.GetTablePath(projName, tableName)

		dataLumpPath := filepath.Join(tablePath, "data.flaa2")

		begin := elemsMap[row["id"]].DataBegin
		end := elemsMap[row["id"]].DataEnd

		nullData := make([]byte, end-begin)

		if flaarum_shared.DoesPathExists(dataLumpPath) {
			dataLumpHandle, err := os.OpenFile(dataLumpPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
			if err != nil {
				fmt.Println(err)
				continue
			}
			defer dataLumpHandle.Close()

			dataLumpHandle.WriteAt(nullData, begin)
		}
	}
	// create or delete indexes.
	for i, row := range patchedRows {
		for fieldName, newData := range row {
			if fieldName == "id" {
				continue
			}

			if !flaarum_shared.IsNotIndexedField(projName, tableName, fieldName) {
				allOldRows := *rows
				oldRow := allOldRows[i]

				oldData, ok := oldRow[fieldName]
				if ok && oldData != newData {
					err = flaarum_shared.DeleteIndex(projName, tableName, fieldName, oldData, row["id"], (*rows)[i]["_version"])
					if err != nil {
						flaarum_shared.PrintError(w, err)
						return
					}
					err = flaarum_shared.MakeIndex(projName, tableName, fieldName, newData, row["id"])
					if err != nil {
						flaarum_shared.PrintError(w, err)
						return
					}
				}

			}

		}

		// write data
		err = flaarum_shared.SaveRowData(projName, tableName, row["id"], row)
		if err != nil {
			flaarum_shared.PrintError(w, err)
			return
		}
	}

	fmt.Fprintf(w, "ok")
}

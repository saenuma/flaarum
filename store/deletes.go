package main

import (
	"fmt"
	"net/http"
	"path/filepath"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/flaarum_shared"
)

func deleteRows(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]

	stmt := r.FormValue("stmt")
	stmtStruct, err := flaarum_shared.ParseSearchStmt(stmt)
	if err != nil {
		printError(w, err)
		return
	}

	tableName := stmtStruct.TableName
	if !doesTableExists(projName, tableName) {
		printError(w, errors.New(fmt.Sprintf("table '%s' of project '%s' does not exists.", tableName, projName)))
		return
	}

	rows, err := innerSearch(projName, stmt)
	if err != nil {
		printError(w, err)
		return
	}

	existingTables, err := getExistingTables(projName)
	if err != nil {
		printError(w, err)
		return
	}

	relatedRelationshipDetails := make(map[string]flaarum_shared.FKeyStruct)
	for _, tbl := range existingTables {
		ts, err := getCurrentTableStructureParsed(projName, tbl)
		if err != nil {
			printError(w, err)
			return
		}

		for _, fkd := range ts.ForeignKeys {
			if fkd.PointedTable == tableName {
				relatedRelationshipDetails[tbl] = fkd
			}
		}
	}

	for _, row := range *rows {
		for otherTbl, fkd := range relatedRelationshipDetails {
			innerStmt := fmt.Sprintf(`
        table: %s
        where:
          %s = %s
        `, otherTbl, fkd.FieldName, row["id"])

			toCheckRows, err := innerSearch(projName, innerStmt)
			if err != nil {
				printError(w, err)
				return
			}

			if fkd.OnDelete == "on_delete_restrict" {
				if len(*toCheckRows) > 0 {
					printError(w, errors.New(fmt.Sprintf("This row with id '%s' is used in table '%s'",
						row["id"], otherTbl)))
					return
				}

			} else if fkd.OnDelete == "on_delete_delete" {
				otherTblFullName := projName + ":" + otherTbl
				tablesMutexes[otherTblFullName].Lock()

				err := innerDelete(projName, otherTbl, toCheckRows)
				if err != nil {
					printError(w, err)
					tablesMutexes[otherTblFullName].Unlock()
					return
				}
				tablesMutexes[otherTblFullName].Unlock()

			}

		}
	}

	fullTableName := projName + ":" + tableName
	tablesMutexes[fullTableName].Lock()
	defer tablesMutexes[fullTableName].Unlock()

	err = innerDelete(projName, tableName, rows)
	if err != nil {
		printError(w, err)
		return
	}

	fmt.Fprintf(w, "ok")
}

func innerDelete(projName, tableName string, rows *[]map[string]string) error {
	dataPath, _ := GetDataPath()
	dataF1Path := filepath.Join(dataPath, projName, tableName, "data.flaa1")
	// update flaa1 file by rewriting it.
	elemsMap, err := flaarum_shared.ParseDataF1File(dataF1Path)
	if err != nil {
		return err
	}

	for _, row := range *rows {
		// delete index
		for f, d := range row {
			if f == "id" {
				continue
			}

			if !flaarum_shared.IsNotIndexedField(projName, tableName, f) {
				flaarum_shared.DeleteIndex(projName, tableName, f, d, row["id"], row["_version"])
			}
		}
		delete(elemsMap, row["id"])
	}

	err = flaarum_shared.RewriteF1File(projName, tableName, "data", elemsMap)
	if err != nil {
		return err
	}

	return nil
}

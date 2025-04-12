package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/internal"
	"github.com/saenuma/flaarumlib"
)

func deleteRows(w http.ResponseWriter, r *http.Request) {
	projName := r.PathValue("proj")

	stmt := r.FormValue("stmt")
	stmtStruct, err := flaarumlib.ParseSearchStmt(stmt)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	tableName := stmtStruct.TableName
	if !doesTableExists(projName, tableName) {
		internal.PrintError(w, errors.New(fmt.Sprintf("table '%s' of project '%s' does not exists.", tableName, projName)))
		return
	}

	rows, err := innerSearch(projName, stmt)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	existingTables, err := internal.ListTables(projName)
	if err != nil {
		internal.PrintError(w, err)
		return
	}

	relatedRelationshipDetails := make(map[string]flaarumlib.FKeyStruct)
	for _, tbl := range existingTables {
		ts, err := getCurrentTableStructureParsed(projName, tbl)
		if err != nil {
			internal.PrintError(w, err)
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
				internal.PrintError(w, err)
				return
			}

			if fkd.OnDelete == "on_delete_restrict" {
				if len(*toCheckRows) > 0 {
					internal.PrintError(w, errors.New(fmt.Sprintf("This row with id '%s' is used in table '%s'",
						row["id"], otherTbl)))
					return
				}

			} else if fkd.OnDelete == "on_delete_delete" {
				otherTblFullName := projName + ":" + otherTbl
				tablesMutexes[otherTblFullName].Lock()

				err := innerDelete(projName, otherTbl, toCheckRows)
				if err != nil {
					internal.PrintError(w, err)
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
		internal.PrintError(w, err)
		return
	}

	fmt.Fprintf(w, "ok")
}

func innerDelete(projName, tableName string, rows *[]map[string]string) error {
	dataPath, _ := internal.GetDataPath()
	dataF1Path := filepath.Join(dataPath, projName, tableName, "data.flaa1")
	// update flaa1 file by rewriting it.
	elemsMap, err := internal.ParseDataF1File(dataF1Path)
	if err != nil {
		return err
	}

	for _, row := range *rows {
		// write null data to flaa2 file
		tablePath := internal.GetTablePath(projName, tableName)

		dataLumpPath := filepath.Join(tablePath, "data.flaa2")

		begin := elemsMap[row["id"]].DataBegin
		end := elemsMap[row["id"]].DataEnd

		nullData := make([]byte, end-begin)

		if internal.DoesPathExists(dataLumpPath) {
			dataLumpHandle, err := os.OpenFile(dataLumpPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
			if err != nil {
				return errors.Wrap(err, "os error")
			}
			defer dataLumpHandle.Close()

			dataLumpHandle.WriteAt(nullData, begin)
		}

		// delete indexes of deleted data
		for f, d := range row {
			if f == "id" {
				continue
			}

			if !internal.IsNotIndexedField(projName, tableName, f) {
				internal.DeleteIndex(projName, tableName, f, d, row["id"], row["_version"])
			}
		}
		delete(elemsMap, row["id"])
	}

	// rewrite index
	err = internal.RewriteF1File(projName, tableName, "data", elemsMap)
	if err != nil {
		return err
	}

	return nil
}

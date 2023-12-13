package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/gookit/color"
	"github.com/otiai10/copy"
	"github.com/saenuma/flaarum"
	"github.com/saenuma/flaarum/flaarum_shared"
)

func reindex(projName, tableName string) {

	if len(os.Args) < 2 {
		color.Red.Println("expected a command. Open help to view commands.")
		os.Exit(1)
	}

	var keyStr string
	inProd := flaarum_shared.GetSetting("in_production")
	if inProd == "" {
		color.Red.Println("unexpected error. Have you installed  and launched flaarum?")
		os.Exit(1)
	}
	if inProd == "true" {
		keyStrPath := flaarum_shared.GetKeyStrPath()
		raw, err := os.ReadFile(keyStrPath)
		if err != nil {
			color.Red.Println(err)
			os.Exit(1)
		}
		keyStr = string(raw)
	} else {
		keyStr = "not-yet-set"
	}
	port := flaarum_shared.GetSetting("port")
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

	if portInt != flaarum_shared.PORT {
		cl = flaarum.NewClientCustomPort("127.0.0.1", keyStr, projName, portInt)
	} else {
		cl = flaarum.NewClient("127.0.0.1", keyStr, projName)
	}

	err = cl.Ping()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if !flaarum_shared.DoesTableExists(projName, tableName) {
		P(fmt.Errorf("table '%s' of project '%s' does not exists", tableName, projName))
		return
	}

	stmt := fmt.Sprintf(`
		table: %s
	`, tableName)

	rows, err := cl.Search(stmt)
	if err != nil {
		P(err)
		return
	}

	tmpTableName := ".tmp_table_" + flaarum_shared.UntestedRandomString(5)
	dataPath, _ := flaarum_shared.GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)
	tmpTablePath := filepath.Join(dataPath, projName, tmpTableName)

	os.Rename(tablePath, tmpTablePath) // move the old contents to temporary directory
	os.MkdirAll(tablePath, 0777)

	// copy structures from tmpTablePath back to tablePath
	index := 1
	for {
		testedStructurePath := filepath.Join(tmpTablePath, fmt.Sprintf("structure%d.txt", index))
		newStructurePath := filepath.Join(tablePath, fmt.Sprintf("structure%d.txt", index))
		if flaarum_shared.DoesPathExists(testedStructurePath) {
			copy.Copy(testedStructurePath, newStructurePath)
		} else {
			break
		}
	}

	// begin insertion
	for _, toInsert := range *rows {
		_, err = cl.InsertRowAny(tableName, toInsert)
		if err != nil {
			P(err)
		}
	}

	os.RemoveAll(tmpTablePath)

}

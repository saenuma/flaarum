package main

import (
	"crypto/sha512"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/flaarum_shared"
)

func GetDataPath() (string, error) {
	return flaarum_shared.GetDataPath()
}

func nameValidate(name string) error {
	if strings.Contains(name, ".") || strings.Contains(name, " ") || strings.Contains(name, "\t") ||
		strings.Contains(name, "\n") || strings.Contains(name, ":") || strings.Contains(name, "/") ||
		strings.Contains(name, "~") {
		return errors.New("object name must not contain space, '.', ':', '/', ~ ")
	}

	return nil
}

func printError(w http.ResponseWriter, err error) {
	flaarum_shared.PrintError(w, err)
}

func printValError(w http.ResponseWriter, err error) {
	fmt.Printf("%+v\n", err)
	debug := flaarum_shared.GetSetting("debug")
	if debug == "true" {
		http.Error(w, fmt.Sprintf("%+v", err), http.StatusBadRequest)
	} else {
		http.Error(w, fmt.Sprintf("%s", err), http.StatusBadRequest)
	}
}

func doesPathExists(p string) bool {
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return false
	}
	return true
}

func createTableMutexIfNecessary(projName, tableName string) {
	fullTableName := projName + ":" + tableName
	_, ok := tablesMutexes[fullTableName]
	if !ok {
		tablesMutexes[fullTableName] = &sync.RWMutex{}
	}
}

func confirmFieldType(projName, tableName, fieldName, fieldType, version string) bool {
	versionInt, _ := strconv.Atoi(version)
	tableStruct, err := getTableStructureParsed(projName, tableName, versionInt)
	if err != nil {
		return false
	}

	if fieldName == "id" && fieldType == "int" {
		return true
	}

	for _, fd := range tableStruct.Fields {
		if fd.FieldName == fieldName && fd.FieldType == fieldType {
			return true
		}
	}
	return false
}

func MakeHash(data string) string {
	h := sha512.New()
	h.Write([]byte(data))
	bs := h.Sum(nil)
	return fmt.Sprintf("%x", bs)
}

func getTablePath(projName, tableName string) string {
	dataPath, _ := GetDataPath()
	return filepath.Join(dataPath, projName, tableName)
}

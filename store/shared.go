package main

import (
	"crypto/sha512"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/flaarum_shared"
)

func nameValidate(name string) error {
	if strings.Contains(name, ".") || strings.Contains(name, " ") || strings.Contains(name, "\t") ||
		strings.Contains(name, "\n") || strings.Contains(name, ":") || strings.Contains(name, "/") ||
		strings.Contains(name, "~") {
		return errors.New("object name must not contain space, '.', ':', '/', ~ ")
	}

	return nil
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

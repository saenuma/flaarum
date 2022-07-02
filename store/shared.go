package main

import (
	"os"
	"path/filepath"
	"github.com/pkg/errors"
	"strings"
	"net/http"
	"fmt"
	"sync"
	"crypto/sha512"
	"strconv"
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
  fmt.Printf("%+v\n", err)
  debug := flaarum_shared.GetSetting("debug")
  if debug == "true" {
    http.Error(w, fmt.Sprintf("%+v", err), http.StatusInternalServerError)
  } else {
    http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
  }
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
	if ! ok {
		tablesMutexes[fullTableName] = &sync.RWMutex{}
	}
}


func makeSafeIndexValue(val string) string {
	return strings.ReplaceAll(val, "/", "~~ab~~")
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
    if fd.FieldName == fieldName && fd.FieldType == fieldType  {
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


// to be deleted
func makeSafeIndexName(v string) string {
	return v
}

func isNotIndexedField(projName, tableName, fieldName string) bool {
	ts, _ := getCurrentTableStructureParsed(projName, tableName)
  for _, fd := range ts.Fields {
    if fd.FieldName == fieldName && fd.NotIndexed == true {
      return true
    }
  }

	for _, fd := range ts.Fields {
		if fd.FieldName == fieldName && fd.FieldType == "text" {
			return true
		}
	}

  return false
}


func isNotIndexedFieldVersioned(projName, tableName, fieldName, version string) bool {
	return flaarum_shared.IsNotIndexedFieldVersioned(projName, tableName, fieldName, version)
}

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
)


func GetDataPath() (string, error) {
	hd, err := os.UserHomeDir()
	if err != nil {
		return "", errors.Wrap(err, "os error")
	}
	return filepath.Join(hd, ".flaarum_data"), nil
}


func projAndTableNameValidate(name string) error {
	if strings.Contains(name, ".") || strings.Contains(name, " ") || strings.Contains(name, "\t") ||
	strings.Contains(name, "\n") || strings.Contains(name, ":") || strings.Contains(name, "/") {
		return errors.New("object name must not contain space, '.', ':', '/', ")
	}

	return nil
}


func printError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	fmt.Fprintf(w, fmt.Sprintf("%+v", err))
}


func doesPathExists(p string) bool {
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return false
	}
	return true
}


func createTableMutexIfNecessary(projName, tableName string) {
	fullTableName := projName + ":" + tableName
	_, ok := rowsMutexes[fullTableName]
	if ! ok {
		rowsMutexes[fullTableName] = &sync.RWMutex{}
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

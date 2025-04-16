package main

import (
	"crypto/sha512"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/internal"
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
	debug := internal.GetSetting("debug")
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

func MakeHash(data string) string {
	h := sha512.New()
	h.Write([]byte(data))
	bs := h.Sum(nil)
	return fmt.Sprintf("%x", bs)
}

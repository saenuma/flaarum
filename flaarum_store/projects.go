package main

import (
	"net/http"
	"github.com/gorilla/mux"
	"path/filepath"
	"os"
	"github.com/pkg/errors"
	"fmt"
)


func createProject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]

	if err := projAndTableNameValidate(projName); err != nil {
		printError(w, err)
		return
	}

	if projName == "first_proj" {
		printError(w, errors.New("the name 'first_proj' is by default created."))
	}

	dataPath, _ := GetDataPath()

	generalMutex.Lock()
	defer generalMutex.Unlock()

	if ! doesPathExists(filepath.Join(dataPath, projName)) {
		err := os.MkdirAll(filepath.Join(dataPath, projName), 0777)
		if err != nil {
			printError(w, errors.Wrap(err, "os error"))
		}
	}

	fmt.Fprintf(w, "ok")
}

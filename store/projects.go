package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/flaarum_shared"
)

func createProject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]

	if err := nameValidate(projName); err != nil {
		flaarum_shared.PrintError(w, err)
		return
	}

	if isInternalProjectName(projName) {
		flaarum_shared.PrintError(w, errors.New(fmt.Sprintf("project name '%s' is used internally", projName)))
		return
	}

	dataPath, _ := flaarum_shared.GetDataPath()

	projsMutex.Lock()
	defer projsMutex.Unlock()

	if !flaarum_shared.DoesPathExists(filepath.Join(dataPath, projName)) {
		err := os.MkdirAll(filepath.Join(dataPath, projName), 0777)
		if err != nil {
			flaarum_shared.PrintError(w, errors.Wrap(err, "os error"))
			return
		}
	}

	fmt.Fprintf(w, "ok")
}

func deleteProject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]

	if isInternalProjectName(projName) {
		flaarum_shared.PrintError(w, errors.New(fmt.Sprintf("project name '%s' is used internally", projName)))
		return
	}

	dataPath, _ := flaarum_shared.GetDataPath()
	if flaarum_shared.DoesPathExists(filepath.Join(dataPath, projName)) {
		projsMutex.Lock()
		defer projsMutex.Unlock()

		existingTables, err := getExistingTables(projName)
		if err != nil {
			flaarum_shared.PrintError(w, err)
			return
		}

		for _, tableName := range existingTables {
			createTableMutexIfNecessary(projName, tableName)
			fullTableName := projName + ":" + tableName
			tablesMutexes[fullTableName].Lock()
		}

		err = os.RemoveAll(filepath.Join(dataPath, projName))
		if err != nil {
			for _, tableName := range existingTables {
				createTableMutexIfNecessary(projName, tableName)
				fullTableName := projName + ":" + tableName
				tablesMutexes[fullTableName].Unlock()
			}
			flaarum_shared.PrintError(w, errors.Wrap(err, "delete directory failed."))
			return
		}

		for _, tableName := range existingTables {
			createTableMutexIfNecessary(projName, tableName)
			fullTableName := projName + ":" + tableName
			tablesMutexes[fullTableName].Unlock()

			delete(tablesMutexes, fullTableName)
		}
	}

	fmt.Fprintf(w, "ok")
}

func listProjects(w http.ResponseWriter, r *http.Request) {
	dataPath, _ := flaarum_shared.GetDataPath()

	projsMutex.RLock()
	defer projsMutex.RUnlock()

	fis, err := os.ReadDir(dataPath)
	if err != nil {
		flaarum_shared.PrintError(w, errors.Wrap(err, "ioutil error"))
		return
	}

	projs := make([]string, 0)
	for _, fi := range fis {
		if fi.IsDir() && !isInternalProjectName(fi.Name()) {
			projs = append(projs, fi.Name())
		}
	}
	projs = append(projs, "first_proj")

	jsonBytes, err := json.Marshal(projs)
	if err != nil {
		flaarum_shared.PrintError(w, errors.Wrap(err, "json error"))
		return
	}

	w.Write(jsonBytes)
}

func renameProject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]
	newProjName := vars["nproj"]

	if isInternalProjectName(projName) {
		flaarum_shared.PrintError(w, errors.New(fmt.Sprintf("project name '%s' is used internally", projName)))
		return
	}

	if isInternalProjectName(newProjName) {
		flaarum_shared.PrintError(w, errors.New(fmt.Sprintf("project name '%s' is used internally", newProjName)))
		return
	}

	dataPath, _ := flaarum_shared.GetDataPath()

	if !flaarum_shared.DoesPathExists(filepath.Join(dataPath, projName)) {
		flaarum_shared.PrintError(w, errors.New(fmt.Sprintf("the project '%s' does not exists.", projName)))
		return
	}

	if flaarum_shared.DoesPathExists(filepath.Join(dataPath, newProjName)) {
		flaarum_shared.PrintError(w, errors.New(fmt.Sprintf("the project name '%s' already exists.", newProjName)))
		return
	}
	projsMutex.Lock()
	defer projsMutex.Unlock()

	existingTables, err := getExistingTables(projName)
	if err != nil {
		flaarum_shared.PrintError(w, err)
		return
	}

	for _, tableName := range existingTables {
		createTableMutexIfNecessary(projName, tableName)
		fullTableName := projName + ":" + tableName
		tablesMutexes[fullTableName].Lock()
	}

	oldPath := filepath.Join(dataPath, projName)
	newPath := filepath.Join(dataPath, newProjName)
	err = os.Rename(oldPath, newPath)
	if err != nil {
		for _, tableName := range existingTables {
			createTableMutexIfNecessary(projName, tableName)
			fullTableName := projName + ":" + tableName
			tablesMutexes[fullTableName].Unlock()
		}
		flaarum_shared.PrintError(w, errors.Wrap(err, "renamed failed."))
		return
	}

	for _, tableName := range existingTables {
		createTableMutexIfNecessary(projName, tableName)
		fullTableName := projName + ":" + tableName
		tablesMutexes[fullTableName].Unlock()

		delete(tablesMutexes, fullTableName)
	}

	fmt.Fprintf(w, "ok")
}

func isInternalProjectName(projName string) bool {
	if projName == "keyfile" || projName == "first_proj" {
		return true
	}

	if strings.HasPrefix(projName, "flaarum_export_") {
		return true
	}

	return false
}

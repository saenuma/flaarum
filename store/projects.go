package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/internal"
)

func createProject(w http.ResponseWriter, r *http.Request) {

	projName := r.PathValue("proj")

	if err := nameValidate(projName); err != nil {
		internal.PrintError(w, err)
		return
	}

	if isInternalProjectName(projName) {
		internal.PrintError(w, errors.New(fmt.Sprintf("project name '%s' is used internally", projName)))
		return
	}

	dataPath, _ := internal.GetRootPath()

	projsMutex.Lock()
	defer projsMutex.Unlock()

	if !internal.DoesPathExists(filepath.Join(dataPath, projName)) {
		err := os.MkdirAll(filepath.Join(dataPath, projName), 0777)
		if err != nil {
			internal.PrintError(w, errors.Wrap(err, "os error"))
			return
		}
	}

	fmt.Fprintf(w, "ok")
}

func deleteProject(w http.ResponseWriter, r *http.Request) {

	projName := r.PathValue("proj")

	if isInternalProjectName(projName) {
		internal.PrintError(w, errors.New(fmt.Sprintf("project name '%s' is used internally", projName)))
		return
	}

	dataPath, _ := internal.GetRootPath()
	if internal.DoesPathExists(filepath.Join(dataPath, projName)) {
		projsMutex.Lock()
		defer projsMutex.Unlock()

		existingTables, err := internal.ListTables(projName)
		if err != nil {
			internal.PrintError(w, err)
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
			internal.PrintError(w, errors.Wrap(err, "delete directory failed."))
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
	dataPath, _ := internal.GetRootPath()

	projsMutex.RLock()
	defer projsMutex.RUnlock()

	fis, err := os.ReadDir(dataPath)
	if err != nil {
		internal.PrintError(w, errors.Wrap(err, "ioutil error"))
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
		internal.PrintError(w, errors.Wrap(err, "json error"))
		return
	}

	w.Write(jsonBytes)
}

func renameProject(w http.ResponseWriter, r *http.Request) {

	projName := r.PathValue("proj")
	newProjName := r.PathValue("nproj")

	if isInternalProjectName(projName) {
		internal.PrintError(w, errors.New(fmt.Sprintf("project name '%s' is used internally", projName)))
		return
	}

	if isInternalProjectName(newProjName) {
		internal.PrintError(w, errors.New(fmt.Sprintf("project name '%s' is used internally", newProjName)))
		return
	}

	dataPath, _ := internal.GetRootPath()

	if !internal.DoesPathExists(filepath.Join(dataPath, projName)) {
		internal.PrintError(w, errors.New(fmt.Sprintf("the project '%s' does not exists.", projName)))
		return
	}

	if internal.DoesPathExists(filepath.Join(dataPath, newProjName)) {
		internal.PrintError(w, errors.New(fmt.Sprintf("the project name '%s' already exists.", newProjName)))
		return
	}
	projsMutex.Lock()
	defer projsMutex.Unlock()

	existingTables, err := internal.ListTables(projName)
	if err != nil {
		internal.PrintError(w, err)
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
		internal.PrintError(w, errors.Wrap(err, "renamed failed."))
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
	internalNames := []string{"keyfile", "first_proj", "flaarum_exports"}

	for _, iName := range internalNames {
		if projName == iName {
			return true
		}
	}

	return false
}

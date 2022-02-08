package main

import (
	"net/http"
	"github.com/gorilla/mux"
	"path/filepath"
	"os"
	"github.com/pkg/errors"
	"fmt"
	"encoding/json"
)


func createProject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	projName := vars["proj"]

	if err := nameValidate(projName); err != nil {
		printError(w, err)
		return
	}

	if projName == "first_proj" {
		printError(w, errors.New("the name 'first_proj' is by default created."))
		return
	}

	dataPath, _ := GetDataPath()

	projsMutex.Lock()
	defer projsMutex.Unlock()

	if ! doesPathExists(filepath.Join(dataPath, projName)) {
		err := os.MkdirAll(filepath.Join(dataPath, projName), 0777)
		if err != nil {
			printError(w, errors.Wrap(err, "os error"))
			return
		}
	}

	fmt.Fprintf(w, "ok")
}


func deleteProject(w http.ResponseWriter, r *http.Request) {
  vars := mux.Vars(r)
  projName := vars["proj"]

  if projName == "keyfile" || projName == "first_proj" {
    printError(w, errors.New(fmt.Sprintf("project name '%s' is used internally", projName)))
    return
  }

  dataPath, _ := GetDataPath()
  if doesPathExists(filepath.Join(dataPath, projName)) {
    projsMutex.Lock()
    defer projsMutex.Unlock()

    existingTables, err := getExistingTables(projName)
    if err != nil {
      printError(w, err)
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
      printError(w, errors.Wrap(err, "delete directory failed."))
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
	dataPath, _ := GetDataPath()

	projsMutex.RLock()
	defer projsMutex.RUnlock()

	fis, err := os.ReadDir(dataPath)
	if err != nil {
		printError(w, errors.Wrap(err, "ioutil error"))
		return
	}

	projs := make([]string, 0)
	for _, fi := range fis {
		if fi.IsDir() {
			projs = append(projs, fi.Name())
		}
	}

	jsonBytes, err := json.Marshal(projs)
	if err != nil {
		printError(w, errors.Wrap(err, "json error"))
		return
	}

	w.Write(jsonBytes)
}


func renameProject(w http.ResponseWriter, r *http.Request) {
  vars := mux.Vars(r)
  projName := vars["proj"]
  newProjName := vars["nproj"]

  if projName == "keyfile" || projName == "first_proj" {
    printError(w, errors.New(fmt.Sprintf("project name '%s' is used internally", projName)))
    return
  }

  dataPath, _ := GetDataPath()

  if ! doesPathExists(filepath.Join(dataPath, projName)) {
    printError(w, errors.New(fmt.Sprintf("the project '%s' does not exists.", projName)))
    return
  }

  if doesPathExists(filepath.Join(dataPath, newProjName)) {
    printError(w, errors.New(fmt.Sprintf("the project name '%s' already exists.", newProjName)))
    return
  }
  projsMutex.Lock()
  defer projsMutex.Unlock()

  existingTables, err := getExistingTables(projName)
  if err != nil {
    printError(w, err)
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
    printError(w, errors.Wrap(err, "renamed failed."))
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

package main

import (
	"net/http"
	"github.com/gorilla/mux"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var projsMutex *sync.RWMutex // for projects and tables (table data uses different mutexes) creation, editing, deletion
var tablesMutexes map[string]*sync.RWMutex

func init() {
	dataPath, err := GetDataPath()
	if err != nil {
		panic(err)
	}

	// create default project
	err = os.MkdirAll(filepath.Join(dataPath, "first_proj"), 0777)
	if err != nil {
		panic(err)
	}

	// create mutexes
	projsMutex = &sync.RWMutex{}
	tablesMutexes = make(map[string]*sync.RWMutex)
}


func main() {
	r := mux.NewRouter()

	r.HandleFunc("/is-flaarum", func (w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "yeah-flaarum")
	})

	// projects
	r.HandleFunc("/create-project/{proj}", createProject)
	r.HandleFunc("/delete-project/{proj}", deleteProject)
	r.HandleFunc("/list-projects", listProjects)
	r.HandleFunc("/rename-project/{proj}/{nproj}", renameProject)

	// tables
	r.HandleFunc("/create-table/{proj}", createTable)
	r.HandleFunc("/update-table-structure/{proj}", updateTableStructure)
	r.HandleFunc("/get-current-version-num/{proj}/{tbl}", getCurrentVersionNumHTTP)
	r.HandleFunc("/get-table-structure/{proj}/{tbl}/{vnum}", getTableStructureHTTP)

	// rows
	r.HandleFunc("/insert-row/{proj}/{tbl}", insertRow)
	r.HandleFunc("/search-table/{proj}", searchTable)
	r.HandleFunc("/delete-rows/{proj}", deleteRows)
  r.HandleFunc("/delete-fields/{proj}", deleteFields)
  r.HandleFunc("/update-rows/{proj}", updateRows)
  r.HandleFunc("/count-rows/{proj}", countRows)
  r.HandleFunc("/sum-rows/{proj}", sumRows)
	
	err := http.ListenAndServeTLS(":22318", "https-server.crt", "https-server.key", r)
	if err != nil {
		panic(err)
	}
}

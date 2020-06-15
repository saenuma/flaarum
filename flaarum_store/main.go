package main

import (
	"net/http"
	"github.com/gorilla/mux"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var generalMutex *sync.RWMutex // for projects and tables (table data uses different mutexes) creation, editing, deletion
var rowsMutexes map[string]*sync.RWMutex

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
	generalMutex = &sync.RWMutex{}
	rowsMutexes = make(map[string]*sync.RWMutex)
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

	// tables
	r.HandleFunc("/create-table/{proj}", createTable)
	r.HandleFunc("/update-table-structure/{proj}", updateTableStructure)

	// rows
	r.HandleFunc("/insert-row/{proj}/{tbl}", insertRow)
	
	err := http.ListenAndServeTLS(":22318", "https-server.crt", "https-server.key", r)
	if err != nil {
		panic(err)
	}
}

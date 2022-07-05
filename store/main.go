// This is the server that accepts and stores data from clients. It is basically an https server.
package main

import (
	"net/http"
	"github.com/gorilla/mux"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"github.com/saenuma/flaarum/flaarum_shared"
  "github.com/saenuma/zazabul"
  "github.com/pkg/errors"
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

  confPath, err := flaarum_shared.GetConfigPath()
  if err != nil {
    panic(err)
  }

  if ! doesPathExists(confPath) {
    conf, err := zazabul.ParseConfig(flaarum_shared.RootConfigTemplate)
    if err != nil {
      panic(err)
    }
    conf.Write(confPath)
  }
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
  r.HandleFunc("/list-tables/{proj}", listTables)
  r.HandleFunc("/delete-table/{proj}/{tbl}", deleteTable)

	// rows
	r.HandleFunc("/insert-row/{proj}/{tbl}", insertRow)
	r.HandleFunc("/search-table/{proj}", searchTable)
	r.HandleFunc("/delete-rows/{proj}", deleteRows)
  r.HandleFunc("/update-rows/{proj}", updateRows)
  r.HandleFunc("/count-rows/{proj}", countRows)
  r.HandleFunc("/sum-rows/{proj}", sumRows)
  r.HandleFunc("/all-rows-count/{proj}/{tbl}", allRowsCount)

  // stats
  r.HandleFunc("/get-and-delete-stats", getAndDeleteStats)

	r.Use(keyEnforcementMiddleware)

  fmt.Printf("Serving on port: %d\n",flaarum_shared.PORT)

	err := http.ListenAndServeTLS(fmt.Sprintf(":%d", flaarum_shared.PORT), flaarum_shared.G("https-server.crt"),
    flaarum_shared.G("https-server.key"), r)
	if err != nil {
		panic(err)
	}
}


func keyEnforcementMiddleware(next http.Handler) http.Handler {
  return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    if r.URL.Path == "/get-and-delete-stats" {
      next.ServeHTTP(w, r)
    } else {
      inProd := flaarum_shared.GetSetting("in_production")
      if inProd == "" {
        panic(errors.New("Have you installed and launched flaarum.store"))
      } else if inProd == "true" {
        keyStr := r.FormValue("key-str")
        keyPath := flaarum_shared.GetKeyStrPath()
        raw, err := os.ReadFile(keyPath)
        if err != nil {
          http.Error(w, "Improperly Configured Server", http.StatusInternalServerError)
        }
        if keyStr == string(raw) {
          // Call the next handler, which can be another middleware in the chain, or the final handler.
          next.ServeHTTP(w, r)
        } else {
          http.Error(w, "Forbidden", http.StatusForbidden)
        }

      } else {
        // Call the next handler, which can be another middleware in the chain, or the final handler.
        next.ServeHTTP(w, r)
      }
    }

  })
}

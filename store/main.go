// This is the server that accepts and stores data from clients. It is basically an https server.
package main

import (
	"net/http"
	"github.com/gorilla/mux"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"io/ioutil"
	"github.com/bankole7782/flaarum/flaarum_shared"
  "github.com/tidwall/pretty"
  "encoding/json"
)

var projsMutex *sync.RWMutex // for projects and tables (table data uses different mutexes) creation, editing, deletion
var tablesMutexes map[string]*sync.RWMutex
var STOP_WORDS []string

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
    conf := map[string]string {
      "debug": "false",
      "in_production": "false",
      "backup_bucket": "",
    }

    jsonBytes, err := json.Marshal(conf)
    if err != nil {
      panic(err)
    }

    prettyJson := pretty.Pretty(jsonBytes)

    err = ioutil.WriteFile(confPath, prettyJson, 0777)
    if err != nil {
      panic(err)
    }
  }

  // load stop words once
  stopWordsJsonPath := flaarum_shared.G("english-stopwords.json")
  jsonBytes, err := ioutil.ReadFile(stopWordsJsonPath)
  if err != nil {
    panic(err)
  }
  stopWordsList := make([]string, 0)
  err = json.Unmarshal(jsonBytes, &stopWordsList)
  if err != nil {
    panic(err)
  }
  STOP_WORDS = stopWordsList
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
  r.HandleFunc("/empty-table/{proj}/{tbl}", emptyTable)
  r.HandleFunc("/list-tables/{proj}", listTables)
  r.HandleFunc("/rename-table/{proj}/{tbl}/{ntbl}", renameTable)
  r.HandleFunc("/delete-table/{proj}/{tbl}", deleteTable)

	// rows
	r.HandleFunc("/insert-row/{proj}/{tbl}", insertRow)
	r.HandleFunc("/search-table/{proj}", searchTable)
	r.HandleFunc("/delete-rows/{proj}", deleteRows)
  r.HandleFunc("/delete-fields/{proj}", deleteFields)
  r.HandleFunc("/update-rows/{proj}", updateRows)
  r.HandleFunc("/count-rows/{proj}", countRows)
  r.HandleFunc("/sum-rows/{proj}", sumRows)

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
    }
    inProd, err := flaarum_shared.GetSetting("in_production")
    if err != nil {
      panic(err)
    }
    if inProd == "true" || inProd == "t" {
      keyStr := r.FormValue("key-str")
      keyPath := flaarum_shared.GetKeyStrPath()
      raw, err := ioutil.ReadFile(keyPath)
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

  })
}

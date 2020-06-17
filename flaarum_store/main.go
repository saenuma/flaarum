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
      "port": "22318",
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
	
	r.Use(keyEnforcementMiddleware)

  port := getPort()
  fmt.Println("Serving on port: " + port)

	err := http.ListenAndServeTLS(fmt.Sprintf(":%s", port), G("https-server.crt"), G("https-server.key"), r)
	if err != nil {
		panic(err)
	}
}


func G(objectName string) string {
  homeDir, err := os.UserHomeDir()
  if err != nil {
    panic(err)
  }
  folders := make([]string, 0)
  folders = append(folders, filepath.Join(homeDir, "flaarum/flaarum_store"))
  folders = append(folders, os.Getenv("SNAP"))

  for _, dir := range folders {
    testPath := filepath.Join(dir, objectName)
    if doesPathExists(testPath) {
      return testPath
    }
  }

  panic("Improperly configured.")
}



func getPort() string {
  port, err := flaarum_shared.GetSetting("port")
  if err != nil {
    panic(err)
  }

  return port
}



func keyEnforcementMiddleware(next http.Handler) http.Handler {
  return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    inProd, err := flaarum_shared.GetSetting("in_production")
    if err != nil {
      panic(err)
    }
    if inProd == "true" {
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

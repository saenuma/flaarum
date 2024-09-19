// This is the server that accepts and stores data from clients. It is basically an https servehttp.
package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"github.com/saenuma/flaarum/internal"
	"github.com/saenuma/zazabul"
)

var projsMutex *sync.RWMutex // for projects and tables (table data uses different mutexes) creation, editing, deletion
var tablesMutexes map[string]*sync.RWMutex

func main() {
	// initialize
	dataPath, err := internal.GetDataPath()
	if err != nil {
		panic(err)
	}

	// create default project
	firstProjPath := filepath.Join(dataPath, "first_proj")
	if !internal.DoesPathExists(firstProjPath) {
		err = os.MkdirAll(firstProjPath, 0777)
		if err != nil {
			panic(err)
		}
	}

	// create mutexes
	projsMutex = &sync.RWMutex{}
	tablesMutexes = make(map[string]*sync.RWMutex)

	confPath, err := internal.GetConfigPath()
	if err != nil {
		panic(err)
	}

	if !internal.DoesPathExists(confPath) {
		conf, err := zazabul.ParseConfig(internal.RootConfigTemplate)
		if err != nil {
			panic(err)
		}
		conf.Write(confPath)
	}

	http.Handle("/is-flaarum", Q(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "yeah-flaarum")
	}))

	// projects
	http.Handle("/create-project/{proj}", Q(createProject))
	http.Handle("/delete-project/{proj}", Q(deleteProject))
	http.Handle("/list-projects", Q(listProjects))
	http.Handle("/rename-project/{proj}/{nproj}", Q(renameProject))

	// tables
	http.Handle("/create-table/{proj}", Q(createTable))
	http.Handle("/update-table-structure/{proj}", Q(updateTableStructure))
	http.Handle("/get-current-version-num/{proj}/{tbl}", Q(getCurrentVersionNumHTTP))
	http.Handle("/get-table-structure/{proj}/{tbl}/{vnum}", Q(getTableStructureHTTP))
	http.Handle("/list-tables/{proj}", Q(listTables))
	http.Handle("/delete-table/{proj}/{tbl}", Q(deleteTable))

	// rows
	http.Handle("/insert-row/{proj}/{tbl}", Q(insertRow))
	http.Handle("/search-table/{proj}", Q(searchTable))
	http.Handle("/delete-rows/{proj}", Q(deleteRows))
	http.Handle("/update-rows/{proj}", Q(updateRows))
	http.Handle("/count-rows/{proj}", Q(countRows))
	http.Handle("/all-rows-count/{proj}/{tbl}", Q(allRowsCount))

	// http.Use(keyEnforcementMiddleware)

	port := internal.GetSetting("port")

	fmt.Printf("Serving on port: %s\n", port)

	err = http.ListenAndServeTLS(fmt.Sprintf(":%s", port), internal.G("https-server.crt"),
		internal.G("https-server.key"), nil)
	if err != nil {
		panic(err)
	}
}

func Q(f func(w http.ResponseWriter, r *http.Request)) http.Handler {
	return keyEnforcementMiddleware(http.HandlerFunc(f))
}

func keyEnforcementMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inProd := internal.GetSetting("in_production")
		if inProd == "" {
			panic(errors.New("Have you installed and launched flaarum.store"))
		} else if inProd == "true" {
			keyStr := r.FormValue("key-str")
			keyPath := internal.GetKeyStrPath()
			raw, err := os.ReadFile(keyPath)
			if err != nil {
				http.Error(w, "Improperly Configured Server", http.StatusInternalServerError)
			}
			if keyStr == string(raw) {
				// Call the next handler, which can be another middleware in the chain, or the final handlehttp.
				next.ServeHTTP(w, r)
			} else {
				http.Error(w, "Forbidden", http.StatusForbidden)
			}

		} else {
			// Call the next handler, which can be another middleware in the chain, or the final handlehttp.
			next.ServeHTTP(w, r)
		}

	})
}

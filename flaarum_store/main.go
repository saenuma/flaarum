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
}


func main() {
	r := mux.NewRouter()

	r.HandleFunc("/is-flaarum", func (w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "yeah-flaarum")
	})

	r.HandleFunc("/create-project/{proj}", createProject)

	err := http.ListenAndServeTLS(":22318", "https-server.crt", "https-server.key", r)
	if err != nil {
		panic(err)
	}
}

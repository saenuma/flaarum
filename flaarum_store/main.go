package main

import (
	"net/http"
	"github.com/gorilla/mux"
	"fmt"
)


func main() {
	r := mux.NewRouter()

	r.HandleFunc("/is-flaarum", func (w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "yeah-flaarum")
	})

	err := http.ListenAndServeTLS(":22318", "https-server.crt", "https-server.key", r)
	if err != nil {
		panic(err)
	}
}
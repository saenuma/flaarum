package main

import (
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/saenuma/flaarum"
	"github.com/saenuma/flaarum/flaarum_shared"
)

func getFlaarumClient() flaarum.Client {
	var keyStr string
	inProd := flaarum_shared.GetSetting("in_production")
	if inProd == "" {
		fmt.Println("unexpected error. Have you installed  and launched flaarum?")
		os.Exit(1)
	}
	if inProd == "true" {
		keyStrPath := flaarum_shared.GetKeyStrPath()
		raw, err := os.ReadFile(keyStrPath)
		if err != nil {
			log.Println(err)
			panic(err)
		}
		keyStr = string(raw)
	} else {
		keyStr = "not-yet-set"
	}
	port := flaarum_shared.GetSetting("port")
	if port == "" {
		panic("unexpected error. Have you installed  and launched flaarum?")
	}
	var cl flaarum.Client

	portInt, err := strconv.Atoi(port)
	if err != nil {
		panic("Invalid port setting.")
	}

	if portInt != flaarum_shared.PORT {
		cl = flaarum.NewClientCustomPort("127.0.0.1", keyStr, "first_proj", portInt)
	} else {
		cl = flaarum.NewClient("127.0.0.1", keyStr, "first_proj")
	}

	return cl
}

func main() {

	cl := getFlaarumClient()
	err := cl.Ping()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	r := mux.NewRouter()

	r.HandleFunc("/gs/{obj}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		rawObj, err := contentStatics.ReadFile("statics/" + vars["obj"])
		if err != nil {
			panic(err)
		}
		w.Header().Set("Content-Disposition", "attachment; filename="+vars["obj"])
		contentType := http.DetectContentType(rawObj)
		w.Header().Set("Content-Type", contentType)
		w.Write(rawObj)
	})

	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		cl.ProjName = "first_proj"

		tables, err := cl.ListTables()
		if err != nil {
			ErrorPage(w, err)
			return
		}

		if len(tables) == 0 {
			http.Redirect(w, r, "/project/first_proj", http.StatusTemporaryRedirect)
		} else {
			http.Redirect(w, r, "/project/first_proj/"+tables[0], http.StatusTemporaryRedirect)
		}
	})

	r.HandleFunc("/project/{project}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		projects, err := cl.ListProjects()
		if err != nil {
			ErrorPage(w, err)
			return
		}

		currentProject := vars["project"]

		cl.ProjName = currentProject
		tables, err := cl.ListTables()
		if err != nil {
			ErrorPage(w, err)
			return
		}

		if len(tables) > 0 {
			http.Redirect(w, r, "/table/"+currentProject+"/"+tables[0], http.StatusTemporaryRedirect)
			return
		}

		type Context struct {
			Projects               []string
			TablesOfCurrentProject []string
			CurrentProject         string
		}
		tmpl := template.Must(template.ParseFS(content, "templates/base.html", "templates/empty_project.html"))
		tmpl.Execute(w, Context{projects, tables, currentProject})
	})

	r.HandleFunc("/table/{project}/{table}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		projects, err := cl.ListProjects()
		if err != nil {
			ErrorPage(w, err)
			return
		}

		currentProject := vars["project"]

		cl.ProjName = currentProject

		tables, err := cl.ListTables()
		if err != nil {
			ErrorPage(w, err)
			return
		}

		currentTable := vars["table"]

		rows, err := cl.Search(fmt.Sprintf(`
		table: %s
		limit: 100
		order_by: id asc
	`, currentTable))
		if err != nil {
			ErrorPage(w, err)
			return
		}

		vnum, _ := cl.GetCurrentTableVersionNum(currentTable)
		tableDefnParsed, _ := cl.GetTableStructureParsed(currentTable, vnum)

		fields := make([]string, 0)
		innerFields := make([]string, 0)
		fields = append(fields, "id[int]")
		fields = append(fields, "_version[int]")

		innerFields = append(innerFields, []string{"id", "_version"}...)
		for _, fieldStruct := range tableDefnParsed.Fields {
			fields = append(fields, fieldStruct.FieldName+"["+fieldStruct.FieldType+"]")
			innerFields = append(innerFields, fieldStruct.FieldName)
		}

		retRows := make([][]any, 0)
		for _, row := range *rows {
			reportedRowSlice := make([]any, 0)
			for _, field := range innerFields {
				reportedRowSlice = append(reportedRowSlice, row[field])
			}
			retRows = append(retRows, reportedRowSlice)
		}

		count, _ := cl.AllRowsCount(currentTable)
		type Context struct {
			Projects               []string
			TablesOfCurrentProject []string
			CurrentProject         string
			CurrentTable           string
			Fields                 []string
			Rows                   [][]any
			AllRowsCount           int64
			CurrentVersion         int
		}
		tmpl := template.Must(template.ParseFS(content, "templates/base.html", "templates/a_table.html"))
		tmpl.Execute(w, Context{projects, tables, currentProject, currentTable, fields, retRows, count, int(vnum)})
	})

	r.HandleFunc("/new_project", newProjectHandler)
	r.HandleFunc("/new_table", newTableHandler)
	r.HandleFunc("/delete_table", deleteTableHandler)
	r.HandleFunc("/load_insert_frag", loadInsertForm)
	r.HandleFunc("/load_table_structure", loadTableStructureHandler)
	r.HandleFunc("/update_table_structure", updateTableStructureHandler)

	// tables handlers
	r.HandleFunc("/insert_row", insertRowHandler)
	r.HandleFunc("/delete_row", deleteRowHandler)

	http.ListenAndServe(":31314", r)
}

func ErrorPage(w http.ResponseWriter, err error) {
	type Context struct {
		Msg template.HTML
	}
	msg := fmt.Sprintf("%+v", err)
	fmt.Println(msg)
	msg = strings.ReplaceAll(msg, "\n", "<br>")
	msg = strings.ReplaceAll(msg, " ", "&nbsp;")
	msg = strings.ReplaceAll(msg, "\t", "&nbsp;&nbsp;")
	tmpl := template.Must(template.ParseFS(content, "templates/error.html"))
	tmpl.Execute(w, Context{template.HTML(msg)})
}
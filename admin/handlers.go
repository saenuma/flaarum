package main

import (
	"fmt"
	"html/template"
	"net/http"

	"github.com/saenuma/flaarum/flaarum_shared"
)

func newProjectHandler(w http.ResponseWriter, r *http.Request) {
	cl := getFlaarumClient()

	err := cl.CreateProject(r.FormValue("name"))
	if err != nil {
		ErrorPage(w, err)
		return
	}

	http.Redirect(w, r, "/?project="+r.FormValue("name"), http.StatusTemporaryRedirect)

}

func newTableHandler(w http.ResponseWriter, r *http.Request) {
	cl := getFlaarumClient()
	cl.ProjName = r.FormValue("current_project")

	err := cl.CreateTable(r.FormValue("stmt"))
	if err != nil {
		ErrorPage(w, err)
		return
	}

	stmtParsed, _ := flaarum_shared.ParseTableStructureStmt(r.FormValue("stmt"))
	http.Redirect(w, r, "/table/"+cl.ProjName+"/"+stmtParsed.TableName, http.StatusTemporaryRedirect)
}

func loadTableHandler(w http.ResponseWriter, r *http.Request) {
	cl := getFlaarumClient()
	cl.ProjName = r.FormValue("project")

	tableName := r.FormValue("table")

	rows, err := cl.Search(fmt.Sprintf(`
		table: %s
		limit: 100
		order_by: id asc
	`, tableName))
	if err != nil {
		ErrorPage(w, err)
		return
	}

	vnum, _ := cl.GetCurrentTableVersionNum(tableName)
	tableDefnParsed, _ := cl.GetTableStructureParsed(tableName, vnum)

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

	count, _ := cl.AllRowsCount(tableName)
	type Context struct {
		Table          string
		Fields         []string
		Rows           [][]any
		AllRowsCount   int64
		CurrentVersion int
	}
	tmpl := template.Must(template.ParseFS(content, "templates/table_view.html"))
	tmpl.Execute(w, Context{tableName, fields, retRows, count, int(vnum)})
}

func FindIn(container []string, elem string) int {
	for i, o := range container {
		if o == elem {
			return i
		}
	}
	return -1
}

func loadInsertForm(w http.ResponseWriter, r *http.Request) {
	cl := getFlaarumClient()
	cl.ProjName = r.FormValue("project")

	tableName := r.FormValue("table")

	vnum, _ := cl.GetCurrentTableVersionNum(tableName)
	tableDefnParsed, _ := cl.GetTableStructureParsed(tableName, vnum)

	html := "<input type='hidden' name='_table' value='" + r.FormValue("table") + "' />"
	html += "<input type='hidden' name='_project' value='" + r.FormValue("project") + "' />"

	for _, fieldStruct := range tableDefnParsed.Fields {
		// fields[fieldStruct.FieldName] = fieldStruct.FieldType
		// innerFields = append(innerFields, fieldStruct.FieldName)
		html += "<div><label>" + fieldStruct.FieldName + "</label><br>"
		if FindIn([]string{"string", "ipaddr", "url"}, fieldStruct.FieldType) != -1 {
			html += "<input type='text' name='" + fieldStruct.FieldName + "' "
		} else if fieldStruct.FieldType == "int" {
			html += "<input type='number' name='" + fieldStruct.FieldName + "' "
		} else if fieldStruct.FieldType == "float" {
			html += "<input type='number' step='0.001' name='" + fieldStruct.FieldName + "' "
		} else if fieldStruct.FieldType == "date" {
			html += "<input type='date' name='" + fieldStruct.FieldName + "' "
		} else if fieldStruct.FieldType == "datetime" {
			html += "<input type='datetime-local' name='" + fieldStruct.FieldName + "' "
		}

		requiredStr := ""
		if fieldStruct.Required {
			requiredStr = "required"
		}
		html += requiredStr + "/></div><br>"
	}

	fmt.Fprint(w, html)
}

func insertRowHandler(w http.ResponseWriter, r *http.Request) {
	cl := getFlaarumClient()
	cl.ProjName = r.FormValue("_project")
	tableName := r.FormValue("_table")

	vnum, _ := cl.GetCurrentTableVersionNum(tableName)
	tableDefnParsed, _ := cl.GetTableStructureParsed(tableName, vnum)

	inputs := make(map[string]string)
	for _, fieldStruct := range tableDefnParsed.Fields {
		inputs[fieldStruct.FieldName] = r.FormValue(fieldStruct.FieldName)
	}

	_, err := cl.InsertRowStr(tableName, inputs)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, err.Error())
	}

	fmt.Fprint(w, "ok")
}

func loadTableStructureHandler(w http.ResponseWriter, r *http.Request) {
	cl := getFlaarumClient()
	cl.ProjName = r.FormValue("project")
	tableName := r.FormValue("table")

	vnum, _ := cl.GetCurrentTableVersionNum(tableName)
	tableStructure, _ := cl.GetTableStructure(tableName, vnum)
	fmt.Fprint(w, tableStructure)
}

func updateTableStructureHandler(w http.ResponseWriter, r *http.Request) {
	cl := getFlaarumClient()
	cl.ProjName = r.FormValue("current_project")

	err := cl.UpdateTableStructure(r.FormValue("stmt"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, err.Error())
	}

	fmt.Fprint(w, "ok")
}

func deleteRowHandler(w http.ResponseWriter, r *http.Request) {
	cl := getFlaarumClient()
	cl.ProjName = r.FormValue("current_project")

	err := cl.DeleteRows(fmt.Sprintf(`
		table: %s
		where:
			id = %s
		`, r.FormValue("table"), r.FormValue("id")))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, err.Error())
	}

	fmt.Fprint(w, "ok")
}

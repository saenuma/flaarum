// cli provides a terminal interface to the flaarum server.
package main

import (
	"os"
	"fmt"
	"github.com/bankole7782/flaarum"
	"github.com/bankole7782/flaarum/flaarum_shared"
	"io/ioutil"
	"strings"
	"strconv"
	"encoding/json"
	"github.com/gookit/color"
	"github.com/tidwall/pretty"
	"os/exec"
)

func main() {
	if len(os.Args) < 2 {
		color.Red.Println("expected a command. Open help to view commands.")
		os.Exit(1)
	}

	serverPort := flaarum_shared.GetPort()

	var keyStr string
	inProd, err := flaarum_shared.GetSetting("in_production")
	if err != nil {
		color.Red.Println("unexpected error. Have you installed  and launched flaarum?")
		os.Exit(1)	
	}
	if inProd == "true" || inProd == "t" {
		keyStrPath := flaarum_shared.GetKeyStrPath()
		raw, err := ioutil.ReadFile(keyStrPath)
		if err != nil {
			color.Red.Println(err)
			os.Exit(1)
		}
		keyStr = string(raw)
	} else {
		keyStr = "not-yet-set"
	}
	cl := flaarum.NewClient(fmt.Sprintf("https://127.0.0.1:%s/", serverPort), keyStr, "first_proj")

	err = cl.Ping()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	switch os.Args[1] {
	case "--help", "help", "h":
		fmt.Println(`flaarum cli provides some utilites for a flaarum installation.
Please Run this program from the same server that powers your flaarum.
Please don't expose your flaarum database to the internet for security sake.

Directory Commands:
  pwd   Print working directory. This is the directory where the files needed by any command 
        in this cli program must reside.

Project(s) Commands:
	
  lp    List Projects
  cp    Create Project: Expects the name(s) of projects after the command.
  rp    Rename Project. Expects the name of the project to rename  followed by the new name of the project.
  dp    Delete Project. Expects the name(s) of projects after the command.

Table(s) Commands:

  lt    List Tables: Expects a project name after the command.
  ct    Create Table: Expects a project name and the path to a file containing the table structure
  uts   Update Table Structure: Expects a project name and the path to a file containing the table structure
  trc   Table Rows Count: Expects a project and table combo eg. 'first_proj/users'
  ctvn  Current Table Version Number: Expects a project and table combo eg. 'first_proj/users'
  ts    Table Structure Statement: Expects a project and table combo eg. 'first_proj/users' and a valid number.
  dt    Delete Table: Expects one or more project and table combo eg. 'first_proj/users'.
  et    Empty Table: Expects one or more project and table combo eg. 'first_proj/users'.
  st    Search Table: Expects a project and a file containing the search statement.


Table Row Commands:

  ir    Insert a row: Expects a project and table combo eg. 'first_proj/users' and a path containing a
        json representation of the data to be inserted into the mentioned table.

  ur    Update Row: Expects a project, table and id combo eg. 'first_proj/users/31' and a path containing a
        json representation of the data to be inserted into the mentioned table.

  dr    Delete Row: Expects one or more project, table and id combo eg. 'first_proj/users/31'
  vr    View Row: Expects a project, table and id combo eg. 'first_proj/users/31'
			`)

	case "pwd":
		p, err := flaarum_shared.GetFlaarumPath("")
		if err != nil {
			color.Red.Println(err)
			os.Exit(1)
		}
		fmt.Println(p)

	case "lp":
		projects, err := cl.ListProjects()
		if err != nil {
			color.Red.Println(err)
			os.Exit(1)
		}
		fmt.Println("Projects List:\n")
		for _, proj := range projects {
			fmt.Printf("  %s\n", proj)
		}
		fmt.Println()
	case "cp":
		if len(os.Args) < 3 {
			color.Red.Println("'cp' command expects project(s)")
			os.Exit(1)
		}
		for _, arg := range os.Args[2:] {
			err = cl.CreateProject(arg)
			if err != nil {
				color.Red.Printf("Error creating project '%s':\nError: %s\n", arg, err)
				os.Exit(1)
			}
		}
	case "rp":
		if len(os.Args) != 4 {
			color.Red.Println("'rp' command expects an old project arg and a new project arg.")
			os.Exit(1)
		}

		err = cl.RenameProject(os.Args[2], os.Args[3])
		if err != nil {
			color.Red.Printf("Error renaming project from '%s' to '%s'.\nError: %s\n", os.Args[2], os.Args[3], err)
			os.Exit(1)
		}

	case "dp":
		if len(os.Args) < 3 {
			color.Red.Println("'dp' command expects project(s)")
			os.Exit(1)
		}
		for _, arg := range os.Args[2:] {
			err = cl.DeleteProject(arg)
			if err != nil {
				color.Red.Printf("Error deleting project '%s':\nError: %s\n", arg, err)
				os.Exit(1)
			}
		}

	case "lt":
		if len(os.Args) != 3 {
			color.Red.Println("'lt' command expects a project name.")
			os.Exit(1)
		}
		cl.ProjName = os.Args[2]
		tables, err := cl.ListTables()
		if err != nil {
			color.Red.Printf("Error listing tables of project '%s'.\nError: %s\n", os.Args[2], err)
			os.Exit(1)
		}
		fmt.Printf("Tables List of Project '%s' List:\n\n", os.Args[2])
		for _, tbl := range tables {
			fmt.Printf("  %s\n", tbl)
		}
		fmt.Println()

	case "trc":
		if len(os.Args) != 3 {
			color.Red.Println("'trc' command expects a project and table combo eg. 'first_proj/users'.")
			os.Exit(1)
		}
		parts := strings.Split(os.Args[2], "/")
		cl.ProjName = parts[0]
		count, err := cl.CountRows(fmt.Sprintf(`
			table: %s
			`, parts[1]))

		if err != nil {
			color.Red.Printf("Error reading table '%s' of project '%s' row count.\nError: %s\n", parts[1], parts[0], err)
			os.Exit(1)
		}

		fmt.Printf("Count of Rows in Table '%s' of Project '%s': %d\n", parts[1], parts[0], count)

	case "ctvn":
		if len(os.Args) != 3 {
			color.Red.Println("'ctvn' command expects a project and table combo eg. 'first_proj/users'.")
			os.Exit(1)
		}
		parts := strings.Split(os.Args[2], "/")
		cl.ProjName = parts[0]
		vnum, err := cl.GetCurrentTableVersionNum(parts[1])
		if err != nil {
			color.Red.Printf("Error reading current table version number of table '%s' of Project '%s'.\nError: %s\n", 
				parts[1], parts[0], err)
			os.Exit(1)
		}
		fmt.Println(vnum)

	case "ts":
		if len(os.Args) != 4 {
			color.Red.Println("'ts' command expects a project table combo eg. 'first_proj/users' and a valid version number.")
			os.Exit(1)
		}
		parts := strings.Split(os.Args[2], "/")
		cl.ProjName = parts[0]
		vnumStr := os.Args[3]
		vnum, err := strconv.ParseInt(vnumStr, 10, 64)
		if err != nil {
			color.Red.Printf("Number supplied '%s' is not a number.\n", vnumStr)
			os.Exit(1)
		}
		tableStructStmt, err := cl.GetTableStructure(parts[1], vnum)
		if err != nil {
			color.Red.Printf("Error reading table structure number '%s' of table '%s' of Project '%s'.\nError: %s\n", 
				vnumStr, parts[1], parts[0], err)
			os.Exit(1)
		}
		fmt.Println(tableStructStmt)

	case "dt":
		if len(os.Args) < 3 {
			color.Red.Println("'dt' command expects combo(s) of project and table eg. 'first_proj/users'.")
			os.Exit(1)
		}
		for _, arg := range os.Args[2:] {
			parts := strings.Split(arg, "/")
			cl.ProjName = parts[0]

			err = cl.DeleteTable(parts[1])
			if err != nil {
				color.Red.Printf("Error deleting table '%s'. \nError: %s\n", arg, err)
				os.Exit(1)
			}
		}

	case "et":
		if len(os.Args) < 3 {
			color.Red.Println("'et' command expects combo(s) of project and table eg. 'first_proj/users'.")
			os.Exit(1)
		}
		for _, arg := range os.Args[2:] {
			parts := strings.Split(arg, "/")
			cl.ProjName = parts[0]

			err = cl.EmptyTable(parts[1])
			if err != nil {
				color.Red.Printf("Error emptying table '%s'. \nError: %s\n", arg, err)
				os.Exit(1)
			}
		}

	case "ct":
		if len(os.Args) != 4 {
			color.Red.Println("'ct' command expects the project name and a file containing table structure.")
			os.Exit(1)
		}

		inputPath, err := flaarum_shared.GetFlaarumPath(os.Args[3])
		if err != nil {
			color.Red.Println("The supplied path '%s' does not exits.\n", inputPath)
			os.Exit(1)
		}
		raw, err := ioutil.ReadFile(inputPath)
		if err != nil {
			color.Red.Printf("The supplied path '%s' does not exists.\n", inputPath)
			os.Exit(1)
		}

		cl.ProjName = os.Args[2]
		err = cl.CreateTable(string(raw))
		if err != nil {
			color.Red.Printf("Error creating table.\nError: %s\n", err)
			os.Exit(1)
		}

	case "uts":
		if len(os.Args) != 4 {
			color.Red.Println("'uts' command expects the project name and a file containing table structure.")
			os.Exit(1)
		}

		inputPath, err := flaarum_shared.GetFlaarumPath(os.Args[3])
		if err != nil {
			color.Red.Println("The supplied path '%s' does not exits.\n", inputPath)
			os.Exit(1)
		}
		raw, err := ioutil.ReadFile(inputPath)
		if err != nil {
			color.Red.Printf("The supplied path '%s' does not exists.\n", inputPath)
			os.Exit(1)
		}

		cl.ProjName = os.Args[2]
		err = cl.UpdateTableStructure(string(raw))
		if err != nil {
			color.Red.Printf("Error updating table.\nError: %s\n", err)
			os.Exit(1)
		}

	case "ir":
		if len(os.Args) != 4 {
			color.Red.Println(`'ir' command expects a project and table combo eg. 'first_proj/users' and a path containing a
        json representation of the data to be inserted into the mentioned table.`)
			os.Exit(1)
		}

		inputPath, err := flaarum_shared.GetFlaarumPath(os.Args[3])
		if err != nil {
			color.Red.Println("The supplied path '%s' does not exits.\n", inputPath)
			os.Exit(1)
		}
		raw, err := ioutil.ReadFile(inputPath)
		if err != nil {
			color.Red.Printf("The supplied path '%s' does not exists.\n", inputPath)
			os.Exit(1)
		}

		rowData := make(map[string]string)
		err = json.Unmarshal(raw, &rowData)
		if err != nil {
			color.Red.Printf("The json file is not valid.\nError: %s\n", err)
			os.Exit(1)
		}

		parts := strings.Split(os.Args[2], "/")
		cl.ProjName = parts[0]

		retId, err := cl.InsertRowStr(parts[1], rowData)
		if err != nil {
			color.Red.Printf("Error inserting a new row.\nError: %s\n", err)
			os.Exit(1)
		}

		fmt.Println(retId)

	case "ur":
		if len(os.Args) != 4 {
			color.Red.Println(`'ur' command expects a project, table and id combo eg. 'first_proj/users/31' and a path containing a
        json representation of the data to be inserted into the mentioned table.`)
			os.Exit(1)
		}

		inputPath, err := flaarum_shared.GetFlaarumPath(os.Args[3])
		if err != nil {
			color.Red.Println("The supplied path '%s' does not exits.\n", inputPath)
			os.Exit(1)
		}
		raw, err := ioutil.ReadFile(inputPath)
		if err != nil {
			color.Red.Printf("The supplied path '%s' does not exists.\n", inputPath)
			os.Exit(1)
		}

		rowData := make(map[string]string)
		err = json.Unmarshal(raw, &rowData)
		if err != nil {
			color.Red.Printf("The json file is not valid.\nError: %s\n", err)
			os.Exit(1)
		}

		parts := strings.Split(os.Args[2], "/")
		cl.ProjName = parts[0]

		err = cl.UpdateRowsStr(fmt.Sprintf(`
			table: %s
			where:
			  id = %s
			`, parts[1], parts[2]), rowData)
		if err != nil {
			color.Red.Printf("Error updating row.\nError: %s\n", err)
			os.Exit(1)
		}

	case "dr":
		if len(os.Args) < 3 {
			color.Red.Println("'dr' command expects one or more project, table and id combo eg. 'first_proj/users/31'")
			os.Exit(1)
		}

		for _, arg := range os.Args[2:] {
			parts := strings.Split(os.Args[2], "/")
			cl.ProjName = parts[0]

			err = cl.DeleteRows(fmt.Sprintf(`
				table: %s
				where:
				  id = %s
				`, parts[1], parts[2]))
			if err != nil {
				color.Red.Printf("Error deleting '%s'.\nError: %s\n", arg, err)
				os.Exit(1)
			}
		}

	case "vr":
		if len(os.Args) != 3 {
			color.Red.Println("'vr' command expects a project, table and id combo eg. 'first_proj/users/31'")
			os.Exit(1)
		}

		parts := strings.Split(os.Args[2], "/")
		cl.ProjName = parts[0]

		arow, err := cl.SearchForOne(fmt.Sprintf(`
			table: %s expand
			where:
				id = %s
			`, parts[1], parts[2]))
		if err != nil {
			color.Red.Printf("Error viewing row '%s'.\nError: %s\n", os.Args[2], err)
			os.Exit(1)
		}

		for k, v := range *arow {
			fmt.Printf("\"%s\": %v\n", k, v)
		}
		fmt.Println()

	case "st":
		if len(os.Args) != 4 {
			color.Red.Println("'st' expects a project and a file containing the search statment.")
			os.Exit(1)
		}

		inputPath, err := flaarum_shared.GetFlaarumPath(os.Args[3])
		if err != nil {
			color.Red.Println("The supplied path '%s' does not exits.\n", inputPath)
			os.Exit(1)
		}
		raw, err := ioutil.ReadFile(inputPath)
		if err != nil {
			color.Red.Printf("The supplied path '%s' does not exists.\n", inputPath)
			os.Exit(1)
		}

		cl.ProjName = os.Args[2]
		rows, err := cl.Search(string(raw))
		if err != nil {
			color.Red.Printf("Error running search '%s'.\nError: %s\n", os.Args[3], err)
			os.Exit(1)
		}

		jsonBytes, err := json.Marshal(*rows)
		if err != nil {
			color.Red.Printf("Error ocurred.\nError: %s\n", err)
			os.Exit(1)
		}

    prettyJson := pretty.Pretty(jsonBytes)

    cmd := exec.Command("less")
    cmd.Stdin = strings.NewReader(string(prettyJson))
    cmd.Stdout = os.Stdout

    err = cmd.Run()
    if err != nil {
    	color.Red.Println("Error occured.\nError: %s\n", err)
    	os.Exit(1)
    }

	default:
		color.Red.Println("Unexpected command. Run the cli with --help to find out the supported commands.")
		os.Exit(1)
	}

}

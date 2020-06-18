package main

import (
	"os"
	"fmt"
	"github.com/bankole7782/flaarum"
	"github.com/bankole7782/flaarum/flaarum_shared"
	"io/ioutil"
	"strings"
	"strconv"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("expected a command. Open help to view commands.")
		os.Exit(1)
	}

	serverPort := flaarum_shared.GetPort()

	var keyStr string
	inProd, err := flaarum_shared.GetSetting("in_production")
	if err != nil {
		fmt.Println("unexpected error. Have you installed  and launched flaarum?")
		os.Exit(1)	
	}
	if inProd == "true" || inProd == "t" {
		keyStrPath := flaarum_shared.GetKeyStrPath()
		raw, err := ioutil.ReadFile(keyStrPath)
		if err != nil {
			fmt.Println(err)
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

Project(s) Commands:
	
  lp    List Projects
  cp    Create Project: Expects the name(s) of projects after the command.
  rp    Rename Project. Expects the name of the project to rename  followed by the new name of the project.
  dp    Delete Project. Expects the name(s) of projects after the command.

Table(s) Commands:

  lt    List Tables: Expects a project name after the command.
  trc   Table Rows Count: Expects a project and table combo eg. 'first_proj/users'
  ctvn  Current Table Version Number: Expects a project and table combo eg. 'first_proj/users'
  ts    Table Structure Statement: Expects a project and table combo eg. 'first_proj/users' and a valid number.

			`)

	case "lp":
		projects, err := cl.ListProjects()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println("Projects List:\n")
		for _, proj := range projects {
			fmt.Printf("  %s\n", proj)
		}
		fmt.Println()
	case "cp":
		if len(os.Args) < 3 {
			fmt.Println("'cp' command expects project(s)")
			os.Exit(1)
		}
		for _, arg := range os.Args[2:] {
			err = cl.CreateProject(arg)
			if err != nil {
				fmt.Println("Error creating project '%s':\nError: %s", arg, err)
				os.Exit(1)
			}
		}
	case "rp":
		if len(os.Args) != 4 {
			fmt.Println("'rp' command expects an old project arg and a new project arg.")
			os.Exit(1)
		}

		err = cl.RenameProject(os.Args[2], os.Args[3])
		if err != nil {
			fmt.Println("Error renaming project from '%s' to '%s'.\nError: %s", os.Args[2], os.Args[3], err)
			os.Exit(1)
		}

	case "dp":
		if len(os.Args) < 3 {
			fmt.Println("'dp' command expects project(s)")
			os.Exit(1)
		}
		for _, arg := range os.Args[2:] {
			err = cl.DeleteProject(arg)
			if err != nil {
				fmt.Println("Error deleting project '%s':\nError: %s", arg, err)
				os.Exit(1)
			}
		}

	case "lt":
		if len(os.Args) != 3 {
			fmt.Println("'lt' command expects a project name.")
			os.Exit(1)
		}
		cl.ProjName = os.Args[2]
		tables, err := cl.ListTables()
		if err != nil {
			fmt.Println("Error listing tables of project '%s'.\nError: %s", os.Args[2], err)
			os.Exit(1)
		}
		fmt.Printf("Tables List of Project '%s' List:\n\n", os.Args[2])
		for _, tbl := range tables {
			fmt.Printf("  %s\n", tbl)
		}
		fmt.Println()

	case "trc":
		if len(os.Args) != 3 {
			fmt.Println("'trc' command expects a project and table combo eg. 'first_proj/users'.")
			os.Exit(1)
		}
		parts := strings.Split(os.Args[2], "/")
		cl.ProjName = parts[0]
		count, err := cl.CountRows(fmt.Sprintf(`
			table: %s
			`, parts[1]))

		if err != nil {
			fmt.Printf("Error reading table '%s' of project '%s' row count.\nError: %s\n", parts[1], parts[0], err)
			os.Exit(1)
		}

		fmt.Printf("Count of Rows in Table '%s' of Project '%s': %d\n", parts[1], parts[0], count)

	case "ctvn":
		if len(os.Args) != 3 {
			fmt.Println("'ctvn' command expects a project and table combo eg. 'first_proj/users'.")
			os.Exit(1)
		}
		parts := strings.Split(os.Args[2], "/")
		cl.ProjName = parts[0]
		vnum, err := cl.GetCurrentTableVersionNum(parts[1])
		if err != nil {
			fmt.Printf("Error reading current table version number of table '%s' of Project '%s'.\nError: %s\n", 
				parts[1], parts[0], err)
			os.Exit(1)
		}
		fmt.Println(vnum)

	case "ts":
		if len(os.Args) != 4 {
			fmt.Println("'ts' command expects a project table combo eg. 'first_proj/users' and a valid version number.")
			os.Exit(1)
		}
		parts := strings.Split(os.Args[2], "/")
		cl.ProjName = parts[0]
		vnumStr := os.Args[3]
		vnum, err := strconv.ParseInt(vnumStr, 10, 64)
		if err != nil {
			fmt.Printf("Number supplied '%s' is not a number.\n", vnumStr)
			os.Exit(1)
		}
		tableStructStmt, err := cl.GetTableStructure(parts[1], vnum)
		if err != nil {
			fmt.Printf("Error reading table structure number '%s' of table '%s' of Project '%s'.\nError: %s\n", 
				vnumStr, parts[1], parts[0], err)
			os.Exit(1)
		}
		fmt.Println(tableStructStmt)

	default:
		fmt.Println("Unexpected command.")
		os.Exit(1)
	}



}
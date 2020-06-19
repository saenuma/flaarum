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
Please Run this program from the same server that powers your flaarum.
Please don't expose your flaarum database to the internet for security sake.

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

	case "dt":
		if len(os.Args) < 3 {
			fmt.Println("'dt' command expects combo(s) of project and table eg. 'first_proj/users'.")
			os.Exit(1)
		}
		for _, arg := range os.Args[2:] {
			parts := strings.Split(os.Args[2], "/")
			cl.ProjName = parts[0]

			err = cl.DeleteTable(parts[1])
			if err != nil {
				fmt.Println("Error deleting table '%s'. \nError: %s", arg, err)
				os.Exit(1)
			}
		}

	case "et":
		if len(os.Args) < 3 {
			fmt.Println("'et' command expects combo(s) of project and table eg. 'first_proj/users'.")
			os.Exit(1)
		}
		for _, arg := range os.Args[2:] {
			parts := strings.Split(os.Args[2], "/")
			cl.ProjName = parts[0]

			err = cl.EmptyTable(parts[1])
			if err != nil {
				fmt.Println("Error emptying table '%s'. \nError: %s", arg, err)
				os.Exit(1)
			}
		}

	case "ct":
		if len(os.Args) != 4 {
			fmt.Println("'ct' command expects the project name and a file containing project structure.")
			os.Exit(1)
		}

		raw, err := ioutil.ReadFile(os.Args[3])
		if err != nil {
			fmt.Printf("The supplied path '%s' does not exists.\n", os.Args[3])
			os.Exit(1)
		}

		cl.ProjName = os.Args[2]
		err = cl.CreateTable(string(raw))
		if err != nil {
			fmt.Printf("Error creating table.\nError: %s", err)
			os.Exit(1)
		}

	case "uts":
		if len(os.Args) != 4 {
			fmt.Println("'uts' command expects the project name and a file containing project structure.")
			os.Exit(1)
		}

		raw, err := ioutil.ReadFile(os.Args[3])
		if err != nil {
			fmt.Printf("The supplied path '%s' does not exists.\n", os.Args[3])
			os.Exit(1)
		}

		cl.ProjName = os.Args[2]
		err = cl.UpdateTableStructure(string(raw))
		if err != nil {
			fmt.Printf("Error updating table.\nError: %s", err)
			os.Exit(1)
		}
	default:
		fmt.Println("Unexpected command.")
		os.Exit(1)
	}

}

package main

import (
	// "flag"
	"os"
	"fmt"
	"github.com/bankole7782/flaarum"
	"github.com/bankole7782/flaarum/flaarum_shared"
	"io/ioutil"
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

Available Commands:
	
	lp    List Projects
	cp    Create Project: Expects the name(s) of projects after the command.
	rp    Rename Project. Expects the name of the project to rename  followed by the new name of the project.
	dp    Delete Project. Expects the name(s) of projects after the command.
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

	default:
		fmt.Println("Unexpected command.")
		os.Exit(1)
	}



}
// prod provides the commands which helps in making a flaarum server production ready.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/gookit/color"
	"github.com/saenuma/flaarum/flaarum_shared"
	"github.com/saenuma/zazabul"
)

func main() {
	dataPath, _ := flaarum_shared.GetDataPath()
	if len(os.Args) < 2 {
		color.Red.Println("expected a command. Open help to view commands.")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "--help", "help", "h":
		fmt.Printf(`Flaarum's prod makes a flaarum instance production ready.

Supported Commands:

  genssl    Generates the ssl certificates for a flaarum installation

  r         Read the current key string used

  c         Creates / Updates and prints a new key string

  mpr       Make production ready. It also creates a key string.

  ifa       Is flaarum free check. Used to know if a long running task is ongoing. 

  reindex   Run the long running task to reindex a table. It expects
            a project name and table name.

  json      Export a table as json and the table structures. It would be exported to 
	          '%s'. It expects a project name and table name.

      `, dataPath)

	case "r":
		keyPath := flaarum_shared.GetKeyStrPath()
		raw, err := os.ReadFile(keyPath)
		if err != nil {
			color.Red.Printf("Error reading key string path.\nError:%s\n", err)
			os.Exit(1)
		}
		fmt.Println(string(raw))

	case "c":
		keyPath := flaarum_shared.GetKeyStrPath()
		randomString := flaarum_shared.GenerateSecureRandomString(50)

		err := os.WriteFile(keyPath, []byte(randomString), 0777)
		if err != nil {
			color.Red.Printf("Error creating key string path.\nError:%s\n", err)
			os.Exit(1)
		}
		fmt.Print(randomString)

	case "mpr":
		keyPath := flaarum_shared.GetKeyStrPath()
		if !flaarum_shared.DoesPathExists(keyPath) {
			randomString := flaarum_shared.GenerateSecureRandomString(50)

			err := os.WriteFile(keyPath, []byte(randomString), 0777)
			if err != nil {
				color.Red.Printf("Error creating key string path.\nError:%s\n", err)
				os.Exit(1)
			}

		}

		confPath, err := flaarum_shared.GetConfigPath()
		if err != nil {
			panic(err)
		}

		var conf zazabul.Config

		for {
			conf, err = zazabul.LoadConfigFile(confPath)
			if err != nil {
				time.Sleep(3 * time.Second)
				continue
			} else {
				break
			}
		}

		conf.Update(map[string]string{
			"in_production": "true",
			"debug":         "false",
		})

		err = conf.Write(confPath)
		if err != nil {
			panic(err)
		}

	case "ifa":
		status := isLongRunningTaskActive()
		if status {
			color.Red.Println("A long running task is running.")
		} else {
			fmt.Println("No long running task is running.")
		}

	case "genssl":
		rootPath, _ := flaarum_shared.GetRootPath()
		keyPath := filepath.Join(rootPath, "https-server.key")
		crtPath := filepath.Join(rootPath, "https-server.crt")

		exec.Command("openssl", "req", "-x509", "-newkey", "rsa:4096", "-keyout", keyPath,
			"-out", crtPath, "-sha256", "-days", "3650", "-nodes", "-subj",
			"/C=XX/ST=StateName/L=CityName/O=CompanyName/OU=CompanySectionName/CN=CommonNameOrHostname").Run()
		fmt.Println("ok")

	case "reindex":
		if len(os.Args) < 4 {
			color.Red.Println("Expecting the name of the project and the table name in order.")
			os.Exit(1)
		}

		instrData := map[string]string{
			"cmd":     "reindex",
			"project": os.Args[2],
			"table":   os.Args[3],
		}

		dataPath, _ := flaarum_shared.GetDataPath()

		outCommandInstr := filepath.Join(dataPath, flaarum_shared.UntestedRandomString(5)+".instr_json")
		hasLongRunningTaskActive := isLongRunningTaskActive()
		if hasLongRunningTaskActive {
			color.Red.Println("Wait for long running task(s) to be completed.")
			os.Exit(1)
		}

		rawJson, _ := json.Marshal(instrData)
		os.WriteFile(outCommandInstr, rawJson, 0777)

		fmt.Println("Wait for operation to finish before using the database.")

	case "json":
		if len(os.Args) < 4 {
			color.Red.Println("Expecting the name of the project and the table name in order.")
			os.Exit(1)
		}

		instrData := map[string]string{
			"cmd":     "json",
			"project": os.Args[2],
			"table":   os.Args[3],
		}

		dataPath, _ := flaarum_shared.GetDataPath()

		outCommandInstr := filepath.Join(dataPath, flaarum_shared.UntestedRandomString(5)+".instr_json")
		hasLongRunningTaskActive := isLongRunningTaskActive()
		if hasLongRunningTaskActive {
			color.Red.Println("Wait for long running task(s) to be completed.")
			os.Exit(1)
		}

		rawJson, _ := json.Marshal(instrData)
		os.WriteFile(outCommandInstr, rawJson, 0777)

		fmt.Println("Wait for operation to finish before using the database.")

	case "csv":
		if len(os.Args) < 4 {
			color.Red.Println("Expecting the name of the project and the table name in order.")
			os.Exit(1)
		}

		instrData := map[string]string{
			"cmd":     "csv",
			"project": os.Args[2],
			"table":   os.Args[3],
		}

		dataPath, _ := flaarum_shared.GetDataPath()

		outCommandInstr := filepath.Join(dataPath, flaarum_shared.UntestedRandomString(5)+".instr_json")
		hasLongRunningTaskActive := isLongRunningTaskActive()
		if hasLongRunningTaskActive {
			color.Red.Println("Wait for long running task(s) to be completed.")
			os.Exit(1)
		}

		rawJson, _ := json.Marshal(instrData)
		os.WriteFile(outCommandInstr, rawJson, 0777)

		fmt.Println("Wait for operation to finish before using the database.")

	default:
		color.Red.Println("Unexpected command. Run the Flaarum's prod with --help to find out the supported commands.")
		os.Exit(1)
	}

}

func isLongRunningTaskActive() bool {
	dataPath, _ := flaarum_shared.GetDataPath()
	dirFIs, err := os.ReadDir(dataPath)
	if err != nil {
		fmt.Println(err)
		return false
	}

	for _, dirFI := range dirFIs {
		if strings.HasSuffix(dirFI.Name(), ".instr_json") {
			return true
		}
	}

	return false
}

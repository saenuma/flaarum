// prod provides the commands which helps in making a flaarum server production ready.
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/gookit/color"
	"github.com/saenuma/flaarum/flaarum_shared"
	"github.com/saenuma/zazabul"
)

func main() {
	if len(os.Args) < 2 {
		color.Red.Println("expected a command. Open help to view commands.")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "--help", "help", "h":
		fmt.Println(`Flaarum's prod makes a flaarum instance production ready.

Supported Commands:

    r     Read the current key string used

    c     Creates / Updates and prints a new key string

    mpr   Make production ready. It also creates a key string.

    masr  Make autoscaling ready. This is for the control instance. It expects in the following order projectId,
          zone, flaarum_data_instance_name, flaarum_data_instance_ip, machine_class.

          Example: sudo flaarum.prod masr project1 us-central1-a flaarum-2sb 192.168.1.31 n2d

    ast   Autoscaling long running tests. The method of test used here is the generation of random inserts.

          The name of the table created is called 'vals'. Please delete the table after tests.

          This program should be ran from the same local network but from a different machine. Keep checking the
          CPU usage of the flaarum server to see if it passes 70 percent. Autoscaling upwards starts at 70 percent.

          It expects three inputs: the address, the keystring and the number of threads.
          The number of threads should start from twenty.

          This program must be ran with 'sudo'.

          Example: sudo flaarum.prod ast 127.0.0.1 not-yet-set 50

      `)

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
		if !doesPathExists(keyPath) {
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

	case "masr":
		if len(os.Args) != 8 {
			color.Red.Println("Expecting 5 arguments. Check the help for documentation")
			os.Exit(1)
		}

		tmpl := `// project is the Google Cloud Project name
// It can be created either from the Google Cloud Console or from the gcloud command
project:

// zone is the Google Cloud Zone which must be derived from a region.
// for instance a region could be 'us-central1' and the zone could be 'us-central1-a'
zone:

// instance name is the name of the instance that would be controlled
instance:


// instance_ip is the IP address of the instance to be controlled
instance_ip:

// machine_type is the type of machine configuration to use to launch your flaarum server.
// You must get this value from the Google Cloud Compute documentation if not it would fail.
machine_type: e2-highcpu-2

// machine class is either 'e2' or 'n2d'.
// The n2d supports higher machines than the e2. But the e2 seems to be cheaper and it is the default
// in Google Cloud Console. Please consider the documentation for more details.
machine_class: e2

`
		conf, err := zazabul.ParseConfig(tmpl)
		if err != nil {
			panic(err)
		}

		var firstMT = "e2-highcpu-2"
		if os.Args[6] == "n2d" {
			firstMT = "n2d-highcpu-2"
		}
		conf.Update(map[string]string{
			"project":      os.Args[2],
			"zone":         os.Args[3],
			"instance":     os.Args[4],
			"instance_ip":  os.Args[5],
			"machine_type": firstMT,
		})

		confPath, err := flaarum_shared.GetCtlConfigPath()
		if err != nil {
			panic(err)
		}

		err = conf.Write(confPath)
		if err != nil {
			panic(err)
		}

	case "ast":

		if len(os.Args) < 5 {
			color.Red.Println("Expecting three inputs: the address, the keystring and the number of threads. The number of threads should start from twenty.")
			return
		}

		addr := os.Args[2]
		fks := os.Args[3]
		numOfThreads, err := strconv.Atoi(os.Args[4])
		if err != nil {
			color.Red.Println(err.Error())
			return
		}

		fmt.Println("Wait for more than 30 minutes.\nAnd then check if the server resizes.")
		astCommand(addr, fks, numOfThreads)

	default:
		color.Red.Println("Unexpected command. Run the Flaarum's prod with --help to find out the supported commands.")
		os.Exit(1)
	}

}

func doesPathExists(p string) bool {
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return false
	}
	return true
}

// This program launches a server with flaarum installed and setup even with backup
package main

import (
	"github.com/bankole7782/flaarum/flaarum_shared"
	"strings"
	"os"
	"github.com/gookit/color"
	"fmt"
  "github.com/tidwall/pretty"
  "io/ioutil"
  "encoding/json"
  "os/exec"
  "github.com/pkg/errors"
  "path/filepath"
)


const (
	configFileName = "flaarum_config.json"
	resultsFileName = "flaarum_launch_results.json"
)


func GetWritePath(fileName string) (string, error) {
	hd, err := os.UserHomeDir()
	if err != nil {
		return "", errors.Wrap(err, "os error")
	}
	dd := os.Getenv("SNAP_USER_DATA")

	if strings.HasPrefix(dd, filepath.Join(hd, "snap", "go")) || dd == "" {
		dd = filepath.Join(hd, fileName)	
	} else {
		dd = filepath.Join(dd, fileName)
	}
	return dd, nil	
}


func main() {
	if len(os.Args) < 2 {
		color.Red.Println("Expecting a command. Run with help subcommand to view help.")
		os.Exit(1)
	}

	switch os.Args[1] {
  case "--help", "help", "h":
    fmt.Println(`Flaarum's launcher creates and configures a flaarum server on Google Cloud.

Supported Commands:

    init   Creates a config file. Edit to your own requirements. Some of the values can be gotten from
           Google Cloud's documentation. 

    l      Launches a configured instance based on the config created above.

      `)
  case "init":

  	initObject := map[string]string {
  		"project": "",
  		"zone": "",
  		"region": "",
  		"disk-size": "10GB",
  		"machine-type": "f1-micro",
  	}

    jsonBytes, err := json.Marshal(initObject)
    if err != nil {
      panic(err)
    }

    prettyJson := pretty.Pretty(jsonBytes)

    wp, err := GetWritePath(configFileName)
    if err != nil {
    	panic(err)
    }

    err = ioutil.WriteFile(wp, prettyJson, 0777)
    if err != nil {
      panic(err)
    }

    fmt.Printf("Edit the file at '%s' before launching.\n", wp)

  case "l":
    wp, err := GetWritePath(configFileName)
    if err != nil {
    	panic(err)
    }

  	raw, err := ioutil.ReadFile(wp)
  	if err != nil {
  		panic(err)
  	}

  	o := make(map[string]string)
  	err = json.Unmarshal(raw, &o)
  	if err != nil {
  		panic(err)
  	}

		instanceName := fmt.Sprintf("flaarum-%s", strings.ToLower(flaarum_shared.UntestedRandomString(4)))
		diskName := fmt.Sprintf("%s-disk", instanceName)
  	
  	o["instance"] = instanceName
  	o["disk"] = diskName

		cmd0 := exec.Command("gcloud", "services", "enable", "compute.googleapis.com", "--project", o["project"])

		_, err = cmd0.Output()
		if err != nil {
			fmt.Println(string(err.(*exec.ExitError).Stderr))
		}

		scriptPath := flaarum_shared.G("startup_script.sh")
		cmd := exec.Command("gcloud", "compute", "--project", o["project"], "instances", "create", o["instance"], 
			"--zone", o["zone"], "--machine-type", o["machine-type"], "--image", "ubuntu-minimal-2004-focal-v20200702",
			"--image-project", "ubuntu-os-cloud", "--boot-disk-size", "10GB", 
			"--create-disk", "mode=rw,size=10,type=pd-ssd,name=" + o["disk"],
			"--metadata-from-file", "startup-script=" + scriptPath,
		)

		_, err = cmd.Output()
		if err != nil {
			fmt.Println(string(err.(*exec.ExitError).Stderr))
			panic(err)
		}

		cmd2 := exec.Command("gcloud", "compute", "resource-policies", "create", "snapshot-schedule", o["instance"] + "-schdl",
	    "--description", "MY WEEKLY SNAPSHOT SCHEDULE", "--max-retention-days", "60", "--start-time", "22:00",
	    "--weekly-schedule", "sunday", "--region", o["region"], "--on-source-disk-delete", "keep-auto-snapshots",
	    "--project", o["project"],
		)

		_, err = cmd2.Output()
		if err != nil {
			fmt.Println(string(err.(*exec.ExitError).Stderr))
			panic(err)
		}

		cmd3 := exec.Command("gcloud", "compute", "disks", "add-resource-policies", o["disk"], "--resource-policies",
			o["instance"] + "-schdl", "--zone", o["zone"], "--project", o["project"],
		)

		_, err = cmd3.Output()
		if err != nil {
			fmt.Println(string(err.(*exec.ExitError).Stderr))
			panic(err)
		}

		cmd4 := exec.Command("gcloud", "services", "enable", "vpcaccess.googleapis.com", "--project", o["project"])

		_, err = cmd4.Output()
		if err != nil {
			fmt.Println(string(err.(*exec.ExitError).Stderr))
		}


		cmd5 := exec.Command("gcloud", "compute", "networks", "vpc-access", "connectors", "create",
			o["instance"] + "-vpcc", "--network", "default", "--region", o["region"],
			"--range", "10.8.0.0/28", "--project", o["project"])

		_, err = cmd5.Output()
		if err != nil {
			fmt.Println(string(err.(*exec.ExitError).Stderr))
			panic(err)
		}

		fmt.Println("Instance Name: " + o["instance"])
		fmt.Println("VPC Connector: " + o["instance"] + "-vpcc. Needed for Appengine and Cloud run.")
		fmt.Println("Please ssh into your instance. Run 'flaarum.prod r' to get your key for your program.")

		outObject := map[string]string {
			"instance": o["instance"], "vpc_connector": o["instance"] + "-vpcc",
		}

    jsonBytes, err := json.Marshal(outObject)
    if err != nil {
      panic(err)
    }

    outPath, err := GetWritePath(resultsFileName)
    if err != nil {
    	panic(err)
    }

    prettyJson := pretty.Pretty(jsonBytes)

    err = ioutil.WriteFile(outPath, prettyJson, 0777)
    if err != nil {
      panic(err)
    }
	}

}

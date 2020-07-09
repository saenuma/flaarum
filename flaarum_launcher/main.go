// This program launches a server with flaarum installed and setup even with backup
package main

import (
	// "github.com/bankole7782/flaarum/flaarum_shared"
	// "strings"
	"os"
	"github.com/gookit/color"
	// "github.com/bankole7782/mangalar"
	"fmt"
  "github.com/tidwall/pretty"
  "io/ioutil"
  "encoding/json"
)

var toExec = `
gcloud compute --project $project instances create $instance --zone $zone --machine-type $mt \
 --image ubuntu-minimal-2004-focal-v20200702 --image-project ubuntu-os-cloud --boot-disk-size 10GB \
  --boot-disk-type pd-standard


`

const (
	configFileName = "flaarum_config.json"
)
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

    l      Launches a configured instance based on the config created above. And it must
           must be executed from the same directory that the init command was executed.

      `)
  case "init":

  	initObject := map[string]string {
  		"project": "",
  		"zone": "",
  		"disk-size": "10GB",
  		"machine-type": "f1-micro",
  	}

    jsonBytes, err := json.Marshal(initObject)
    if err != nil {
      panic(err)
    }

    prettyJson := pretty.Pretty(jsonBytes)

    err = ioutil.WriteFile(configFileName, prettyJson, 0777)
    if err != nil {
      panic(err)
    }

  case "l":
  	raw, err := ioutil.ReadFile(configFileName)
  	if err != nil {
  		panic(err)
  	}
  	initObject := make(map[string]string)
  	err = json.Unmarshal(raw, &initObject)
  	if err != nil {
  		panic(err)
  	}

  	fmt.Println(initObject)
	}

	// instanceName := fmt.Sprintf("flaarum-%s", strings.ToLower(flaarum_shared.UntestedRandomString(4)))
	// diskName := fmt.Sprintf("%s-disk", instanceName)

	// vars := map[string]string {
	// 	"project": os.Args[1], "zone": os.Args[2],
	// 	"instance": instanceName, "disk": diskName,
	// }

	// fmt.Println(vars)
}
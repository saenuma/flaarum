// prod provides the commands which helps in making a flaarum server production ready.
package main

import (
  "github.com/bankole7782/flaarum/flaarum_shared"
  "io/ioutil"
  "fmt"
  "os"
  "github.com/gookit/color"
  "github.com/bankole7782/zazabul"
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

    mpr   Make production ready. It also creates and prints a key string. It expects a google cloud bucket
          as its only argument.

    masr  Make autoscaling ready. This is for the control instance. It expects in the following order projectId,
          zone, flaarum-data-instance-name, flaarum-data-instance-ip.

          Example: sudo flaarum.prod masr project1 us-central1-a flaarum-2sb 192.168.1.31
      `)

  case "r":
    keyPath := flaarum_shared.GetKeyStrPath()
    raw, err := ioutil.ReadFile(keyPath)
    if err != nil {
      color.Red.Printf("Error reading key string path.\nError:%s\n", err)
      os.Exit(1)
    }
    fmt.Println(string(raw))

  case "c":
    keyPath := flaarum_shared.GetKeyStrPath()
    randomString := flaarum_shared.UntestedRandomString(50)

    err := ioutil.WriteFile(keyPath, []byte(randomString), 0777)
    if err != nil {
      color.Red.Printf("Error creating key string path.\nError:%s\n", err)
      os.Exit(1)
    }
    fmt.Print(randomString)

  case "mpr":
    if len(os.Args) != 3 {
      color.Red.Println("Expecting the backup_bucket as the only argument")
      os.Exit(1)
    }
    keyPath := flaarum_shared.GetKeyStrPath()
    randomString := flaarum_shared.UntestedRandomString(50)

    err := ioutil.WriteFile(keyPath, []byte(randomString), 0777)
    if err != nil {
      color.Red.Printf("Error creating key string path.\nError:%s\n", err)
      os.Exit(1)
    }
    fmt.Print(randomString)

    confPath, err := flaarum_shared.GetConfigPath()
    if err != nil {
      panic(err)
    }

    conf, err := zazabul.LoadConfigFile(confPath)
    if err != nil {
      panic(err)
    }

    conf.Update(map[string]string{
      "backup_bucket": os.Args[2],
      "in_production": "true",
    })

    err = conf.Write(confPath)
    if err != nil {
      panic(err)
    }

  case "masr":
    if len(os.Args) != 6 {
      color.Red.Println("Expecting 4 arguments. Check the help for documentation")
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


// instance-ip is the IP address of the instance to be controlled
instance-ip: 

// machine-type is the type of machine configuration to use to launch your flaarum server.
// You must get this value from the Google Cloud Compute documentation if not it would fail.
// It is not necessary it must be an e2 instance.
machine-type: e2-highcpu-2

`
    conf, err := zazabul.ParseConfig(tmpl)
    if err != nil {
      panic(err)
    }

    conf.Update(map[string]string {
      "project": os.Args[2],
      "zone": os.Args[3],
      "instance": os.Args[4],
      "instance-ip": os.Args[5],
    })


    confPath, err := flaarum_shared.GetCtlConfigPath()
    if err != nil {
      panic(err)
    }

    err = conf.Write(confPath)
    if err != nil {
      panic(err)
    }

  default:
    color.Red.Println("Unexpected command. Run the Flaarum's prod with --help to find out the supported commands.")
    os.Exit(1)
  }


}

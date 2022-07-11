// prod provides the commands which helps in making a flaarum server production ready.
package main

import (
  "github.com/saenuma/flaarum/flaarum_shared"
  "fmt"
  "os"
  "github.com/gookit/color"
  "github.com/saenuma/zazabul"
  "time"
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
    if ! doesPathExists(keyPath) {
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
      "debug": "false",
    })

    err = conf.Write(confPath)
    if err != nil {
      panic(err)
    }

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

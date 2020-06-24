package main

import (
  "github.com/bankole7782/flaarum/flaarum_shared"
  "io/ioutil"
  "fmt"
  "os"
  "github.com/gookit/color"
)


func main() {
  if len(os.Args) < 2 {
    color.Red.Println("expected a command. Open help to view commands.")
    os.Exit(1)
  }

  switch os.Args[1] {
  case "--help", "help", "h":
    fmt.Println(`Flaarum's keyp creates or reads the key string used to authenticate the programs with
Flaarum.

Supported Commands:

    r     Read the current key string used 
    c     Creates and prints a new key string

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

  default:
    color.Red.Println("Unexpected command. Run the Flaarum's keyp with --help to find out the supported commands.")
    os.Exit(1)
  }


}

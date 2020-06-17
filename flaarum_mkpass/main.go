package main

import (
  "github.com/bankole7782/flaarum/flaarum_shared"
  "io/ioutil"
  "fmt"
  "path/filepath"
)


func main() {
  keyPath := filepath.Join("/etc", "flaarum.keyfile")
  randomString := flaarum_shared.UntestedRandomString(50)

  err := ioutil.WriteFile(keyPath, []byte(randomString), 0777)
  if err != nil {
    panic(err)
  }
  fmt.Print(randomString)
}

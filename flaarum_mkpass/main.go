package main

import (
  "github.com/bankole7782/flaarum/flaarum_shared"
  "io/ioutil"
  "fmt"
)


func main() {
  keyPath := flaarum_shared.GetKeyStrPath()
  randomString := flaarum_shared.UntestedRandomString(50)

  err := ioutil.WriteFile(keyPath, []byte(randomString), 0777)
  if err != nil {
    panic(err)
  }
  fmt.Print(randomString)
}

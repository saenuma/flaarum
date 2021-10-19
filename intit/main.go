package main

import (
  "github.com/bankole7782/flaarum/flaarum_shared"
  "fmt"
)


func main() {

  a := make([]flaarum_shared.IntIndexes, 0)
  b := flaarum_shared.AppendToIntIndexes(a, 12, 1)
  fmt.Println("first b", b)
  b = flaarum_shared.AppendToIntIndexes(b, 12, 2)
  fmt.Println("second b", b)
  b = flaarum_shared.AppendToIntIndexes(b, 12, 3)
  fmt.Println("third b", b)
  b = flaarum_shared.AppendToIntIndexes(b, 3, 4)
  fmt.Println("4th b", b)
  b = flaarum_shared.AppendToIntIndexes(b, 333, 5)
  fmt.Println("5th b", b)
  b = flaarum_shared.AppendToIntIndexes(b, 233, 6)
  fmt.Println("6th b", b)
  b = flaarum_shared.AppendToIntIndexes(b, 233, 11)
  fmt.Println("7th b", b)
  b = flaarum_shared.AppendToIntIndexes(b, 236, 12)
  fmt.Println("8th b", b)

  err := flaarum_shared.WriteIntIndexesToFile(b, "/tmp/out.txt")
  if err != nil {
    fmt.Println(err)
  }

  c, err := flaarum_shared.ReadIntIndexesFromFile("/tmp/out.txt")
  if err != nil {
    fmt.Println(err)
  }
  fmt.Println("c", c)

  d := flaarum_shared.RemoveFromIntIndexes(c, 233, 6)
  d = flaarum_shared.RemoveFromIntIndexes(c, 233, 11)
  fmt.Println("d", d)
}

package main

import (
  "os"
  "strings"
  "github.com/pkg/errors"
  "fmt"
  "strconv"
  "sort"
  "path/filepath"
  "github.com/saenuma/flaarum/flaarum_shared"
)

type DataF1Elem struct {
  DataKey string
  DataBegin int64
  DataEnd int64
}


func ParseDataF1File(path string) ([]DataF1Elem, error) {
  rawConf, err := os.ReadFile(path)
  if err != nil {
    return nil, err
  }

  ret := make([]DataF1Elem, 0)

  nl := flaarum_shared.GetNewline()
  partsOfRawConf := strings.Split(string(rawConf), nl + nl)
  for _, part := range partsOfRawConf {
    innerParts := strings.Split(strings.TrimSpace(part), nl)

    var elem DataF1Elem
    for _, line := range innerParts {
      var colonIndex int
      for i, ch := range line {
				if fmt.Sprintf("%c", ch) == ":" {
					colonIndex = i
					break
				}
			}

      if colonIndex == 0 {
        continue
      }

      optName := strings.TrimSpace(line[0:colonIndex])
      optValue := strings.TrimSpace(line[colonIndex + 1: ])

      if optName == "data_key" {
        elem.DataKey = optValue
      } else if optName == "data_begin" {
        data, err := strconv.ParseInt(optValue, 10, 64)
        if err != nil {
          return nil, errors.New("data_begin is not of type int64")
        }
        elem.DataBegin = data
      } else if optName == "data_end" {
        data, err := strconv.ParseInt(optValue, 10, 64)
        if err != nil {
          return nil, errors.New("data_end is not of type int64")
        }
        elem.DataEnd = data
      }
    }

    if elem.DataKey == "" {
      continue
    }
    ret = append(ret, elem)
  }

  sort.Slice(ret, func(i, j int) bool {
    return ret[i].DataKey < ret[j].DataKey
  })

  return ret, nil
}


func WriteDataF1File(projName, tableName, name string, elem DataF1Elem) error {
  tablePath := getTablePath(projName, tableName)
  path := filepath.Join(tablePath, name + ".flaa1")
  dataF1Handle, err := os.OpenFile(path,	os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
  if err != nil {
    return errors.Wrap(err, "os error")
  }
  defer dataF1Handle.Close()

  nl := flaarum_shared.GetNewline()
  out := fmt.Sprintf("data_key: %s%sdata_begin: %d%sdata_end:%d%s%s", elem.DataKey, nl,
    elem.DataBegin, nl, elem.DataEnd, nl, nl)

  _, err = dataF1Handle.Write([]byte(out))
  if err != nil {
    return errors.Wrap(err, "os error")
  }

  return nil
}

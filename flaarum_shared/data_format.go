package flaarum_shared

import (
  "strings"
  "fmt"
  "os"
)


func ParseEncodedRowData(rawData []byte) (map[string]string, error) {
  ret := make(map[string]string)
  nl := GetNewline()

  partsOfRawData := strings.Split(string(rawData), nl)
  for _, line := range partsOfRawData {
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

    ret[optName] = optValue

  }

  rawDataStr := string(rawData)
  for k, v := range ret {
    if strings.TrimSpace(v) == "" {
      firstIndex := strings.Index(rawDataStr, fmt.Sprintf("%s%s:", nl, k))
      lastIndex := strings.LastIndex(rawDataStr, fmt.Sprintf("%s%s:", nl, k))
      padding := len( fmt.Sprintf("%s%s:", nl, k))
      if firstIndex != lastIndex {
        ret[k] = rawDataStr[firstIndex+padding: lastIndex]
      }
    }
  }

  return ret, nil
}


func ParseDataFormat(path string) (map[string]string, error) {
  raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

  return ParseEncodedRowData(raw)
}


func EncodeRowData(projName, tableName string, toWrite map[string]string) string {
  nl := GetNewline()
  out := nl
  for k, v := range toWrite {
    ft := GetFieldType(projName, tableName, k)
    if ft == "text" {
    out += fmt.Sprintf("%s:%s%s%s%s:%s", k, nl, v, nl, k, nl)
    } else {
      out += fmt.Sprintf("%s: %s%s", k, v, nl)
    }
  }

  return out
}

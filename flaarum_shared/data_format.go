package flaarum_shared

import (
  "os"
  "strings"
  "fmt"
  "encoding/json"
  "github.com/pkg/errors"
)


func ParseDataFormat(path string) (map[string]string, error) {
  rawData, err := os.ReadFile(path)
  if err != nil {
    return nil, err
  }

  ret := make(map[string]string)

  partsOfRawData := strings.Split(string(rawData), "\n\n")
  for _, part := range partsOfRawData {
    innerParts := strings.Split(strings.TrimSpace(part), "\n")

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

      ret[optName] = optValue
    }

  }

  rawDataStr := string(rawData)
  for k, v := range ret {
    if strings.TrimSpace(v) == "" {
      firstIndex := strings.Index(rawDataStr, fmt.Sprintf("\n%s:", k))
      lastIndex := strings.LastIndex(rawDataStr, fmt.Sprintf("\n%s:", k))
      padding := len( fmt.Sprintf("\n%s:", k))
      if firstIndex != lastIndex {
        ret[k] = rawDataStr[firstIndex+padding: lastIndex]
      }
    }
  }

  return ret, nil
}


func ReadDataFile(path string) (map[string]string, error) {
	// this file checks if the data was encoded in json or a custom format
	// json was the former default encoding
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	firstChar := string(raw)[0]
	if firstChar == '{' {
		// read json
		rowMap := make(map[string]string)
		err = json.Unmarshal(raw, &rowMap)
		if err != nil {
			return nil, errors.Wrap(err, "json error.")
		}
		return rowMap, nil
	} else {
		// read custom data format
		return ParseDataFormat(path)
	}
}

package flaarum_shared

import (
  "os"
  "strings"
  "fmt"
)


func ParseDataFormat(path string) (map[string]string, error) {
  rawConf, err := os.ReadFile(path)
  if err != nil {
    return nil, err
  }

  ret := make(map[string]string)

  partsOfRawConf := strings.Split(string(rawConf), "\n\n")
  for _, part := range partsOfRawConf {
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

  return ret, nil
}

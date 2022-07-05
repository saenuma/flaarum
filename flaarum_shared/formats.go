package flaarum_shared

import (
  "os"
  "strings"
  "github.com/pkg/errors"
  "fmt"
  "strconv"
  "path/filepath"
)

type DataF1Elem struct {
  DataKey string
  DataBegin int64
  DataEnd int64
}


func ParseDataF1File(path string) (map[string]DataF1Elem, error) {
  rawF1File, err := os.ReadFile(path)
  if err != nil {
    return nil, err
  }

  ret := make(map[string]DataF1Elem, 0)

  nl := GetNewline()
  partsOfRawF1File := strings.Split(string(rawF1File), nl + nl)
  for _, part := range partsOfRawF1File {
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
    ret[elem.DataKey] = elem
  }

  return ret, nil
}


func AppendDataF1File(projName, tableName, name string, elem DataF1Elem) error {
  dataPath, _ := GetDataPath()
  path := filepath.Join(dataPath, projName, tableName, name + ".flaa1")
  dataF1Handle, err := os.OpenFile(path,	os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
  if err != nil {
    return errors.Wrap(err, "os error")
  }
  defer dataF1Handle.Close()

  nl := GetNewline()
  out := fmt.Sprintf("data_key: %s%sdata_begin: %d%sdata_end:%d%s%s", elem.DataKey, nl,
    elem.DataBegin, nl, elem.DataEnd, nl, nl)

  _, err = dataF1Handle.Write([]byte(out))
  if err != nil {
    return errors.Wrap(err, "os error")
  }

  return nil
}


func ReadPortionF2File(projName, tableName, name string, begin, end int64) ([]byte, error) {
  dataPath, _ := GetDataPath()
  path := filepath.Join(dataPath, projName, tableName, name + ".flaa2")
  f2FileHandle, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0777)
  if err != nil {
    return []byte{}, errors.Wrap(err, "os error")
  }
  defer f2FileHandle.Close()

  outData := make([]byte, end - begin)
  _, err = f2FileHandle.ReadAt(outData, begin)
  if err != nil {
    return outData, errors.Wrap(err, "os error")
  }

  return outData, nil
}


func RewriteF1File(projName, tableName, name string, elems map[string]DataF1Elem) error {
  dataPath, _ := GetDataPath()
  path := filepath.Join(dataPath, projName, tableName, name + ".flaa1")

  nl := GetNewline()
  out := nl
  for _, elem := range elems {
    out += fmt.Sprintf("data_key: %s%sdata_begin: %d%sdata_end:%d%s%s", elem.DataKey, nl,
      elem.DataBegin, nl, elem.DataEnd, nl, nl)
  }

  err := os.WriteFile(path, []byte(out), 0777)
  if err != nil {
    return errors.Wrap(err, "os error")
  }

  return nil
}


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

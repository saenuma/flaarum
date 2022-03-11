// inout creates compressed backup files and can restore a flaarum project from the said files.
package main

import (
	"fmt"
	"os"
	"io"
	"time"
	"path/filepath"
	"compress/gzip"
	"github.com/otiai10/copy"
	"github.com/saenuma/mof"
	"github.com/saenuma/flaarum/flaarum_shared"
	"encoding/json"
	"sync"
	"strings"
	"strconv"
	"github.com/pkg/errors"
	"io/fs"
  "log"
)


func isFieldExemptedFromIndexingVersioned(projName, tableName, fieldName, version string) bool {
	versionNum, _ := strconv.Atoi(version)

  td, _ := flaarum_shared.GetTableStructureParsed(projName, tableName, versionNum)
  for _, fd := range td.Fields {
    if fd.FieldName == fieldName && fd.FieldType == "text" {
      return true
    }
  }
  return false

}


func inCommand() {
  dataPath, err := flaarum_shared.GetDataPath()
  if err != nil {
    log.Println(err)
  }

  inCommandInstrPath := filepath.Join(dataPath, "in.in_instr")

  rawCommandOpts, err := os.ReadFile(inCommandInstrPath)
  if err != nil {
		return
  }

  partsOfFile := strings.Split(strings.TrimSpace(string(rawCommandOpts)), "\n")

  projName := partsOfFile[0]
  inputPath := partsOfFile[1]

	log.Printf("Received instruction to import into  Project '%s' from file %s\n", projName, inputPath)

  tmpFolderName := fmt.Sprintf(".flaarumin-%s", flaarum_shared.UntestedRandomString(10))
  tmpFolder := filepath.Join(dataPath, tmpFolderName)

  err = os.MkdirAll(tmpFolder, 0777)
  if err != nil {
    log.Println(err)
  }
  defer os.RemoveAll(tmpFolder)

  inputFile, err := os.Open(inputPath)
  if err != nil {
    log.Println(err)
  }
  defer inputFile.Close()

  zr, err := gzip.NewReader(inputFile)
  if err != nil {
    log.Println(err)
  }

  mofBytes, err := io.ReadAll(zr)
  if err != nil {
    log.Println(err)
  }

  err = os.WriteFile(filepath.Join(tmpFolder, "out.mof"), mofBytes, 0777)
  if err != nil {
    log.Println(err)
  }

  err = mof.UndoMOF(filepath.Join(tmpFolder, "out.mof"), tmpFolder)
  if err != nil {
    log.Println(err)
  }

  tblFIs, err := os.ReadDir(filepath.Join(tmpFolder, "out"))
  if err != nil {
    log.Println(err)
  }

  var wg sync.WaitGroup
  for _, tblFI := range tblFIs {
    wg.Add(1)

    go func(tblFI fs.DirEntry, wg *sync.WaitGroup) {
      defer wg.Done()

      err = copy.Copy(filepath.Join(tmpFolder, "out", tblFI.Name(), "structures"), filepath.Join(dataPath, projName, tblFI.Name(), "structures"))
      if err != nil {
        log.Println(err)
      }
      err = copy.Copy(filepath.Join(tmpFolder, "out", tblFI.Name(), "data"), filepath.Join(dataPath, projName, tblFI.Name(), "data"))
      if err != nil {
        os.MkdirAll(filepath.Join(dataPath, projName, tblFI.Name(), "data"), 0777)
      }
      _ = copy.Copy(filepath.Join(tmpFolder, "out", tblFI.Name(), "lastId"), filepath.Join(dataPath, projName, tblFI.Name(), "lastId"))

      toMakePath := filepath.Join(dataPath, projName, tblFI.Name(), "indexes")
      err = os.MkdirAll(toMakePath, 0777)
      if err != nil {
        log.Println(err)
      }

      toMakePath = filepath.Join(dataPath, projName, tblFI.Name(), "txtinstrs")
      err = os.MkdirAll(toMakePath, 0777)
      if err != nil {
        log.Println(err)
      }

      toMakePath = filepath.Join(dataPath, projName, tblFI.Name(), "intindexes")
      err = os.MkdirAll(toMakePath, 0777)
      if err != nil {
        log.Println(err)
      }

      toMakePath = filepath.Join(dataPath, projName, tblFI.Name(), "timeindexes")
      err = os.MkdirAll(toMakePath, 0777)
      if err != nil {
        log.Println(err)
      }

			toMakePath = filepath.Join(dataPath, projName, tblFI.Name(), "likeindexes")
      err = os.MkdirAll(toMakePath, 0777)
      if err != nil {
        log.Println(err)
      }

      rowFIs, err := os.ReadDir(filepath.Join(dataPath, projName, tblFI.Name(), "data"))
      if err != nil {
        log.Println(err)
      }

      for _, rowFI := range rowFIs {
        rowMap := make(map[string]string)
        rowBytes, err := os.ReadFile(filepath.Join(dataPath, projName, tblFI.Name(), "data", rowFI.Name()))
        if err != nil {
          log.Println(err)
        }
        err = json.Unmarshal(rowBytes, &rowMap)
        if err != nil {
          log.Println(err)
        }

        // create indexes
        for k, v := range rowMap {
          if k == "id" {
            continue
          }

          if isFieldExemptedFromIndexingVersioned(projName, tblFI.Name(), k, rowMap["_version"]) {

            // create a .text file which is a message to the tindexer program.
            newTextFileName := rowFI.Name() + flaarum_shared.TEXT_INTR_DELIM + k + ".text"
            err = os.WriteFile(filepath.Join(dataPath, projName, tblFI.Name(), "txtinstrs", newTextFileName), []byte(v), 0777)
            if err != nil {
              fmt.Printf("%+v\n", errors.Wrap(err, "ioutil error."))
            }
          } else if flaarum_shared.IsNotIndexedFieldVersioned(projName, tblFI.Name(), k, rowMap["_version"]) {
            // don't create indexes
          } else {

            err := flaarum_shared.MakeIndex(projName, tblFI.Name(), k, v, rowFI.Name())
            if err != nil {
              log.Println(err)
            }

          }

        }


      }

    }(tblFI, &wg)


  }

  wg.Wait()

  log.Println("Done importing.")
  os.Remove(inCommandInstrPath)
}


func outCommand() {
  dataPath, err := flaarum_shared.GetDataPath()
  if err != nil {
    log.Println(err)
  }

  outCommandInstrPath := filepath.Join(dataPath, "out.out_instr")

  projNameRaw, err := os.ReadFile(outCommandInstrPath)
  if err != nil {
    return
  }
  projName := strings.TrimSpace(string(projNameRaw))

	log.Println("Received instruction to export Project " + projName)

  tmpFolderName := fmt.Sprintf(".flaarumout-%s", flaarum_shared.UntestedRandomString(10))
  tmpFolder := filepath.Join(dataPath, tmpFolderName)

  err = os.MkdirAll(tmpFolder, 0777)
  if err != nil {
    log.Println(err)
  }
  defer os.RemoveAll(tmpFolder)

  projPath := filepath.Join(dataPath, projName)

  tblFIs, err := os.ReadDir(projPath)
  for _, tblFI := range tblFIs {

    if projName == "first_proj" && tblFI.Name() == "server_stats" {
      continue
    }

    err := copy.Copy(filepath.Join(projPath, tblFI.Name(), "structures"), filepath.Join(tmpFolder, "out", tblFI.Name(), "structures"))
    if err != nil {
      log.Println(err)
    }

    err = copy.Copy(filepath.Join(projPath, tblFI.Name(), "data"), filepath.Join(tmpFolder, "out", tblFI.Name(), "data"))
    if err != nil {
      log.Println(err)
    }

    _ = copy.Copy(filepath.Join(projPath, tblFI.Name(), "lastId"), filepath.Join(tmpFolder, "out", tblFI.Name(), "lastId"))
  }

  err = mof.MOF(filepath.Join(tmpFolder, "out"), filepath.Join(tmpFolder, "out.mof"))
  if err != nil {
    log.Println(err)
  }

  outFileName := fmt.Sprintf("flaarumout-%s.%s", time.Now().Format("20060102T1504"), flaarum_shared.BACKUP_EXT)

  outFilePath := filepath.Join(dataPath, outFileName)

  outFile, err := os.Create(outFilePath)
  if err != nil {
    log.Println(err)
		return
  }
  defer outFile.Close()
  zw := gzip.NewWriter(outFile)
  zw.Name = outFileName
  zw.Comment = "backup output from flaarum"
  zw.ModTime = time.Now()

  mofBytes, err := os.ReadFile(filepath.Join(tmpFolder, "out.mof"))
  if err != nil {
    log.Println(err)
  }
  _, err = zw.Write(mofBytes)
  if err != nil {
    log.Println(err)
  }

  if err := zw.Close(); err != nil {
    log.Println(err)
  }

  log.Printf("Done exporting project '%s' to file '%s'.\n", projName, outFilePath)
  os.Remove(outCommandInstrPath)
}

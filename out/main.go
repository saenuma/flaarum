package main

import (
	"fmt"
	"os"
	"io/ioutil"
	"time"
	"path/filepath"
	"compress/gzip"
	"github.com/otiai10/copy"
	"github.com/bankole7782/mof"
	"github.com/bankole7782/flaarum/flaarum_shared"
	"github.com/gookit/color"
)


func main() {
	if len(os.Args) != 2 {
		color.Red.Println("Expecting the project as the second argument.")
		os.Exit(1)
	}

	projName := os.Args[1]


	dataPath, err := flaarum_shared.GetDataPath()
	if err != nil {
		panic(err)
	}

	tmpFolderName := fmt.Sprintf(".flaarumout-%s", flaarum_shared.UntestedRandomString(10))
	tmpFolder, err := flaarum_shared.GetFlaarumPath(tmpFolderName)
	if err != nil {
		panic(err)
	}

	err = os.MkdirAll(tmpFolder, 0777)
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpFolder)

	projPath := filepath.Join(dataPath, projName)

	tblFIs, err := ioutil.ReadDir(projPath)
	for _, tblFI := range tblFIs {
		err := copy.Copy(filepath.Join(projPath, tblFI.Name(), "structures"), filepath.Join(tmpFolder, "out", tblFI.Name(), "structures"))
		if err != nil {
			panic(err)
		}

		err = copy.Copy(filepath.Join(projPath, tblFI.Name(), "data"), filepath.Join(tmpFolder, "out", tblFI.Name(), "data"))
		if err != nil {
			panic(err)
		}

		_ = copy.Copy(filepath.Join(projPath, tblFI.Name(), "lastId"), filepath.Join(tmpFolder, "out", tblFI.Name(), "lastId"))
	}

	err = mof.MOF(filepath.Join(tmpFolder, "out"), filepath.Join(tmpFolder, "out.mof"))
	if err != nil {
		panic(err)
	}

	outFileName := fmt.Sprintf("flaarumout-%s.%s", time.Now().Format("20060102T1504"), flaarum_shared.BACKUP_EXT)

	outFilePath, err := flaarum_shared.GetFlaarumPath(outFileName)
	if err != nil {
		panic(err)
	}

	outFile, err := os.Create(outFilePath)
	if err != nil {
		panic(err)
	}
	defer outFile.Close()
	zw := gzip.NewWriter(outFile)
	zw.Name = outFileName
	zw.Comment = "backup output from flaarum"
	zw.ModTime = time.Now()

	mofBytes, err := ioutil.ReadFile(filepath.Join(tmpFolder, "out.mof"))
	if err != nil {
		panic(err)
	}
	_, err = zw.Write(mofBytes)
	if err != nil {
		panic(err)
	}

	if err := zw.Close(); err != nil {
		panic(err)
	}

	fmt.Printf(outFilePath)
}

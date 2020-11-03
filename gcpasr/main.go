package main

import (
	"fmt"
	"github.com/go-co-op/gocron"
	"time"
	"github.com/bankole7782/flaarum/flaarum_shared"
	"encoding/json"
	"context"
	"golang.org/x/oauth2/google"
  compute "google.golang.org/api/compute/v1"
  "io/ioutil"
  "strings"
  "net/http"
	"crypto/tls"
	"github.com/pkg/errors"
  "github.com/tidwall/pretty"
)


var confObject map[string]string

var MTs = []string{
	"e2-highcpu-2",
	"e2-highcpu-4",
	"e2-highcpu-8",
	"e2-highcpu-16",
	"e2-highcpu-32",
}

func main() {
  confPath, err := flaarum_shared.GetCtlConfigPath()
  if err != nil {
    panic(err)
  }

	raw, err := ioutil.ReadFile(confPath)
	if err != nil {
		panic(err)
	}

	o := make(map[string]string)
	err = json.Unmarshal(raw, &o)
	if err != nil {
		panic(err)
	}

	confObject = o

	scheduler := gocron.NewScheduler(time.UTC)
	scheduler.Every(6).Hours().Do(resizeMachineType)
	scheduler.StartBlocking()
}


func waitForOperationZone(project, zone string, service *compute.Service, op *compute.Operation) error {
	ctx := context.Background()
	for {
		result, err := service.ZoneOperations.Get(project, zone, op.Name).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("Failed retriving operation status: %s", err)
		}

		if result.Status == "DONE" {
			if result.Error != nil {
				var errors []string
				for _, e := range result.Error.Errors {
					errors = append(errors, e.Message)
				}
				return fmt.Errorf("Operation failed with error(s): %s", strings.Join(errors, ", "))
			}
			break
		}
		time.Sleep(time.Second)
	}
	return nil
}


func resizeMachineType() {
	config := &tls.Config { InsecureSkipVerify: true}
	tr := &http.Transport{TLSClientConfig: config}

	httpCl := &http.Client{Transport: tr}

	resp, err := httpCl.Get(fmt.Sprintf("https:%s:%d/get-and-delete-stats", confObject["flaarum-ip"], flaarum_shared.PORT))
	if err != nil {
		panic(errors.Wrap(err, "http error"))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(errors.Wrap(err, "ioutil error"))
	}

	if resp.StatusCode != 200 {
		panic(errors.New(string(body)))
	}

	respObj := make(map[string]int64)
  err = json.Unmarshal(body, &respObj)
  if err != nil {
    panic(errors.Wrap(err, "json error."))
  }

  nextActionCPU := whatToDo(respObj["cpu_usage"])
  nextActionRAM := whatToDo(respObj["ram_usage"])

  if nextActionCPU == "incr" || nextActionRAM == "incr" {
  	// do increase
  	if confObject["machine-type"] == MTs[len(MTs) - 1] {
  		fmt.Println("No resizing. You've gotten to the max 'e2-highcpu-32'.")
  		return
  	}
  	index := flaarum_shared.FindIn(MTs, confObject["machine-type"])
  	innerResizeMachine(MTs[index + 1])

  	fmt.Println("Successfully resized the flaarum server")
  } else if nextActionCPU == "decr" || nextActionRAM == "dcr" {
  	// do decrease
		if confObject["machine-type"] == MTs[0] {
  		fmt.Println("No resizing. You've gotten to the minimum 'e2-highcpu-2'.")
  		return
  	}
  	index := flaarum_shared.FindIn(MTs, confObject["machine-type"])
  	innerResizeMachine(MTs[index - 1])

  	fmt.Println("Successfully resized the flaarum server")
  } else {
  	fmt.Println("No need for resize.")
  }
}


func whatToDo(state int64) string {
	if state >= 80 {
		return "incr"
	} else if state <= 20 {
		return "dcr"
	} else {
		return "remain"
	}
}


func innerResizeMachine(mt string) {
	ctx := context.Background()
  client, err := google.DefaultClient(ctx, compute.ComputeScope)
  if err != nil {
  	panic(err)
  }

	computeService, err := compute.New(client)
	if err != nil {
		panic(err)
	}

	op, err := computeService.Instances.Stop(confObject["project"], confObject["zone"], confObject["instance"]).Context(ctx).Do()
	if err != nil {
		panic(err)
	}
	err = waitForOperationZone(confObject["project"], confObject["zone"], computeService, op)
	if err != nil {
		panic(err)
	}

	op, err = computeService.Instances.SetMachineType(confObject["project"], confObject["zone"], confObject["instance"], 
		&compute.InstancesSetMachineTypeRequest{
			MachineType: "/zones/" + confObject["zone"] + "/machineTypes/" + mt,		
	}).Context(ctx).Do()
	if err != nil {
		panic(err)
	}
	err = waitForOperationZone(confObject["project"], confObject["zone"], computeService, op)
	if err != nil {
		panic(err)
	}

	op, err = computeService.Instances.Start(confObject["project"], confObject["zone"], confObject["instance"]).Context(ctx).Do()
	if err != nil {
		panic(err)
	}
	err = waitForOperationZone(confObject["project"], confObject["zone"], computeService, op)
	if err != nil {
		panic(err)
	}

	// save the machine-type in use
	confObject["machine-type"] = mt

  jsonBytes, err := json.Marshal(confObject)
  if err != nil {
    panic(err)
  }

  prettyJson := pretty.Pretty(jsonBytes)

  confPath, err := flaarum_shared.GetCtlConfigPath()
  if err != nil {
    panic(err)
  }

  err = ioutil.WriteFile(confPath, prettyJson, 0777)
  if err != nil {
    panic(err)
  }
}

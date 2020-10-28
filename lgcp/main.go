package main

import (
	"fmt"
	"context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	compute "google.golang.org/api/compute/v1"
	"os"
	"github.com/gookit/color"
	"io/ioutil"
	"encoding/json"
	"github.com/bankole7782/flaarum/flaarum_shared"
	"time"
	"github.com/tidwall/pretty"
	"strings"
	"strconv"
)


func main() {
	if len(os.Args) < 2 {
		color.Red.Println("Expecting a command. Run with help subcommand to view help.")
		os.Exit(1)
	}

	switch os.Args[1] {
  case "--help", "help", "h":
    fmt.Println(`lgcp creates and configures a flaarum server on Google Cloud.

Supported Commands:

    initb    Creates a config file for basic deployment (non-autoscaling). Edit to your own requirements. 
             Some of the values can be gotten from Google Cloud's documentation. 

    lb       Launches a configured instance based on the config created above. It expects a launch file (created from 'init' above)
             and a service account credentials file (gotten from Google Cloud).

    initas   Creates a config file for auto scaling deployment. The method of autoscaling is to launch a large server in the
             morning and resize in the evening to a small server. Good for websites facing one country. Edit it to your own 
             requirements. Some of the values can be gotten from Google Cloud's documentation. 

    las      Launches an autoscaling deployment from the config created above. The deployment would have two servers.
             It expects a launch file (created from 'init' above) and a service account credentials file 
             (gotten from Google Cloud).

      `)

  case "initb":

  	initObject := map[string]string {
  		"project": "",
  		"zone": "",
  		"disk-size": "10",
  		"machine-type": "e2-highcpu-2",
  		"backup-bucket": "",
  	}

    jsonBytes, err := json.Marshal(initObject)
    if err != nil {
      panic(err)
    }

    prettyJson := pretty.Pretty(jsonBytes)

    configFileName := "lb" + time.Now().Format("20060102T150405") + ".json"

    writePath, err := flaarum_shared.GetFlaarumPath(configFileName)
    if err != nil {
    	panic(err)
    }

    err = ioutil.WriteFile(writePath, prettyJson, 0777)
    if err != nil {
      panic(err)
    }

    fmt.Printf("Edit the file at '%s' before launching.\n", writePath)

  case "lb":
  	if len(os.Args) != 4 {
  		color.Red.Println("The l command expects a launch file and a service account credentials file.")
  		os.Exit(1)
  	}

    inputPath, err := flaarum_shared.GetFlaarumPath(os.Args[2])
    if err != nil {
    	panic(err)
    }

  	raw, err := ioutil.ReadFile(inputPath)
  	if err != nil {
  		panic(err)
  	}

  	o := make(map[string]string)
  	err = json.Unmarshal(raw, &o)
  	if err != nil {
  		panic(err)
  	}

  	for _, v := range o {
  		if v == "" {
  			color.Red.Println("Every field in the launch file is compulsory.")
  			os.Exit(1)
  		}
  	}

  	credentialsFilePath, err := flaarum_shared.GetFlaarumPath(os.Args[3])
  	if err != nil {
  		panic(err)
  	}

		instanceName := fmt.Sprintf("flaarum-%s", strings.ToLower(flaarum_shared.UntestedRandomString(4)))
		diskName := fmt.Sprintf("%s-disk", instanceName)
  	
  	o["instance"] = instanceName
  	o["disk"] = diskName
  	diskSizeInt, err := strconv.ParseInt(o["disk-size"], 10, 64)
  	if err != nil {
  		color.Red.Println("The 'disk-size' variable must be a number greater or equal to 10")
  		os.Exit(1)
  	}

		var startupScript = `
#! /bin/bash

sudo snap install flaarum
`
		startupScript += "\nsudo flaarum.prod mpr " + o["backup-bucket"] + " \n"
		startupScript += `
sudo snap start flaarum.store
sudo snap start flaarum.tindexer
sudo snap start flaarum.rbackup
`

  	ctx := context.Background()

  	data, err := ioutil.ReadFile(credentialsFilePath)
		if err != nil {
			panic(err)
		}
		creds, err := google.CredentialsFromJSON(ctx, data, compute.ComputeScope)
		if err != nil {
			panic(err)
		}

		client := oauth2.NewClient(ctx, creds.TokenSource)
		
		computeService, err := compute.New(client)
		if err != nil {
			panic(err)
		}

		prefix := "https://www.googleapis.com/compute/v1/projects/" + o["project"]
		imageURL := "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-minimal-2004-focal-v20201014"

		instance := &compute.Instance{
			Name: instanceName,
			Description: "flaarum instance",
			MachineType: prefix + "/zones/" + o["zone"] + "/machineTypes/" + o["machine-type"],
			Disks: []*compute.AttachedDisk{
				{
					AutoDelete: true,
					Boot:       true,
					Type:       "PERSISTENT",

					InitializeParams: &compute.AttachedDiskInitializeParams{
						DiskName:    o["disk"],
						SourceImage: imageURL,
						DiskType: prefix + "/zones/" + o["zone"] + "/diskTypes/pd-ssd",
						DiskSizeGb: diskSizeInt,
					},
				},
			},
			NetworkInterfaces: []*compute.NetworkInterface{
				{
					AccessConfigs: []*compute.AccessConfig{
						{
							Type: "ONE_TO_ONE_NAT",
							Name: "External NAT",
						},
					},
					Network: prefix + "/global/networks/default",
				},
			},
			ServiceAccounts: []*compute.ServiceAccount{
				{
					Email: "default",
					Scopes: []string{
						compute.DevstorageFullControlScope,
						compute.ComputeScope,
					},
				},
			},
			Metadata: &compute.Metadata {
				Items: []*compute.MetadataItems {
					{
						Key: "startup-script",
						Value: &startupScript,
					},
				},
			},
		}

		_, err = computeService.Instances.Insert(o["project"], o["zone"], instance).Do()
		if err != nil {
			panic(err)
		}

		fmt.Println("Instance Name: " + o["instance"])

	case "initas":

		initObject := map[string]string {
  		"project": "",
  		"zone": "",
  		"disk-size": "10",
  		"machine-type-day": "e2-highcpu-8",
  		"machine-type-night": "e2-highcpu-2",
  		"backup-bucket": "",
  		"timezone": "Africa/Lagos",
  	}

    jsonBytes, err := json.Marshal(initObject)
    if err != nil {
      panic(err)
    }

    prettyJson := pretty.Pretty(jsonBytes)

    configFileName := "las" + time.Now().Format("20060102T150405") + ".json"

    writePath, err := flaarum_shared.GetFlaarumPath(configFileName)
    if err != nil {
    	panic(err)
    }

    err = ioutil.WriteFile(writePath, prettyJson, 0777)
    if err != nil {
      panic(err)
    }

    fmt.Printf("Edit the file at '%s' before launching.\n", writePath)

  case "las":

  	if len(os.Args) != 4 {
  		color.Red.Println("The l command expects a launch file and a service account credentials file.")
  		os.Exit(1)
  	}

    inputPath, err := flaarum_shared.GetFlaarumPath(os.Args[2])
    if err != nil {
    	panic(err)
    }

  	raw, err := ioutil.ReadFile(inputPath)
  	if err != nil {
  		panic(err)
  	}

  	o := make(map[string]string)
  	err = json.Unmarshal(raw, &o)
  	if err != nil {
  		panic(err)
  	}

  	for _, v := range o {
  		if v == "" {
  			color.Red.Println("Every field in the launch file is compulsory.")
  			os.Exit(1)
  		}
  	}

  	if _, err = time.LoadLocation(o["timezone"]); err != nil {
  		panic(err)
  	}

  	credentialsFilePath, err := flaarum_shared.GetFlaarumPath(os.Args[3])
  	if err != nil {
  		panic(err)
  	}

  	suffix := strings.ToLower(flaarum_shared.UntestedRandomString(4))
		instanceName := fmt.Sprintf("flaarum-%s", suffix)
		diskName := fmt.Sprintf("%s-disk", instanceName)
  	
  	o["instance"] = instanceName
  	o["control-instance"] = fmt.Sprintf("flaarumctl-%s", suffix)
  	o["control-instance-disk"] = o["control-instance"] + "-disk"

  	o["disk"] = diskName
  	diskSizeInt, err := strconv.ParseInt(o["disk-size"], 10, 64)
  	if err != nil {
  		color.Red.Println("The 'disk-size' variable must be a number greater or equal to 10")
  		os.Exit(1)
  	}

		var startupScript = `
#! /bin/bash

sudo snap install flaarum
`
		startupScript += "\nsudo flaarum.prod mpr " + o["backup-bucket"] + " \n"
		startupScript += `
sudo snap start flaarum.store
sudo snap start flaarum.tindexer
sudo snap start flaarum.rbackup
`

  	ctx := context.Background()

  	data, err := ioutil.ReadFile(credentialsFilePath)
		if err != nil {
			panic(err)
		}
		creds, err := google.CredentialsFromJSON(ctx, data, compute.ComputeScope)
		if err != nil {
			panic(err)
		}

		client := oauth2.NewClient(ctx, creds.TokenSource)
		
		computeService, err := compute.New(client)
		if err != nil {
			panic(err)
		}

		prefix := "https://www.googleapis.com/compute/v1/projects/" + o["project"]
		imageURL := "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-minimal-2004-focal-v20201014"

		instance := &compute.Instance{
			Name: instanceName,
			Description: "flaarum data instance",
			MachineType: prefix + "/zones/" + o["zone"] + "/machineTypes/" + o["machine-type-night"],
			Disks: []*compute.AttachedDisk{
				{
					AutoDelete: true,
					Boot:       true,
					Type:       "PERSISTENT",

					InitializeParams: &compute.AttachedDiskInitializeParams{
						DiskName:    o["disk"],
						SourceImage: imageURL,
						DiskType: prefix + "/zones/" + o["zone"] + "/diskTypes/pd-ssd",
						DiskSizeGb: diskSizeInt,
					},
				},
			},
			NetworkInterfaces: []*compute.NetworkInterface{
				{
					AccessConfigs: []*compute.AccessConfig{
						{
							Type: "ONE_TO_ONE_NAT",
							Name: "External NAT",
						},
					},
					Network: prefix + "/global/networks/default",
				},
			},
			ServiceAccounts: []*compute.ServiceAccount{
				{
					Email: "default",
					Scopes: []string{
						compute.DevstorageFullControlScope,
						compute.ComputeScope,
					},
				},
			},
			Metadata: &compute.Metadata {
				Items: []*compute.MetadataItems {
					{
						Key: "startup-script",
						Value: &startupScript,
					},
				},
			},
		}

		_, err = computeService.Instances.Insert(o["project"], o["zone"], instance).Do()
		if err != nil {
			panic(err)
		}

		var startupScriptControlInstance = `
#! /bin/bash

sudo snap install flaarum
`
		startupScriptControlInstance += "\nsudo flaarum.prod masr " + o["project"] + " " + o["zone"] 
		startupScriptControlInstance += " " + o["instance"] + " " + o["timezone"]
		startupScriptControlInstance += " " + o["machine-type-day"] + " " + o["machine-type-night"] + " \n"
		startupScriptControlInstance += `
sudo snap start flaarum.gcpasr
`

		ctlInstance := &compute.Instance{
			Name: o["control-instance"],
			Description: "flaarum control instance",
			MachineType: prefix + "/zones/" + o["zone"] + "/machineTypes/e2-small",
			Disks: []*compute.AttachedDisk{
				{
					AutoDelete: true,
					Boot:       true,
					Type:       "PERSISTENT",

					InitializeParams: &compute.AttachedDiskInitializeParams{
						DiskName:    o["control-instance-disk"],
						SourceImage: imageURL,
					},
				},
			},
			NetworkInterfaces: []*compute.NetworkInterface{
				{
					AccessConfigs: []*compute.AccessConfig{
						{
							Type: "ONE_TO_ONE_NAT",
							Name: "External NAT",
						},
					},
					Network: prefix + "/global/networks/default",
				},
			},
			ServiceAccounts: []*compute.ServiceAccount{
				{
					Email: "default",
					Scopes: []string{
						compute.DevstorageFullControlScope,
						compute.ComputeScope,
					},
				},
			},
			Metadata: &compute.Metadata {
				Items: []*compute.MetadataItems {
					{
						Key: "startup-script",
						Value: &startupScriptControlInstance,
					},
				},
			},
		}

		_, err = computeService.Instances.Insert(o["project"], o["zone"], ctlInstance).Do()
		if err != nil {
			panic(err)
		}

		fmt.Println("Instance Name: " + o["instance"])  	
		fmt.Println("Control Instance Name: ", o["control-instance"])
  }

}

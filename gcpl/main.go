// gcpl is the launcher program for Google Cloud platform
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gookit/color"
	"github.com/saenuma/flaarum/flaarum_shared"
	"github.com/saenuma/zazabul"
	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
	// "google.golang.org/api/option"
)

func waitForOperationRegion(project, region string, service *compute.Service, op *compute.Operation) error {
	ctx := context.Background()
	for {
		result, err := service.RegionOperations.Get(project, region, op.Name).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("failed retriving operation status: %s", err)
		}

		if result.Status == "DONE" {
			if result.Error != nil {
				var errors []string
				for _, e := range result.Error.Errors {
					errors = append(errors, e.Message)
				}
				return fmt.Errorf("operation failed with error(s): %s", strings.Join(errors, ", "))
			}
			break
		}
		time.Sleep(time.Second)
	}
	return nil
}

func waitForOperationZone(project, zone string, service *compute.Service, op *compute.Operation) error {
	ctx := context.Background()
	for {
		result, err := service.ZoneOperations.Get(project, zone, op.Name).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("failed retriving operation status: %s", err)
		}

		if result.Status == "DONE" {
			if result.Error != nil {
				var errors []string
				for _, e := range result.Error.Errors {
					errors = append(errors, e.Message)
				}
				return fmt.Errorf("operation failed with error(s): %s", strings.Join(errors, ", "))
			}
			break
		}
		time.Sleep(time.Second)
	}
	return nil
}

func main() {
	if len(os.Args) < 2 {
		color.Red.Println("Expecting a command. Run with help subcommand to view help.")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "--help", "help", "h":
		fmt.Println(`gcpl creates and configures a flaarum server on Google Cloud.

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
		configFileName := "lb" + time.Now().Format("20060102T150405") + ".zconf"

		writePath, err := flaarum_shared.GetFlaarumPath(configFileName)
		if err != nil {
			panic(err)
		}

		var tmpl = `// project is the Google Cloud Project name
// It can be created either from the Google Cloud Console or from the gcloud command
project:

// region is the Google Cloud Region name
// Specify the region you want to launch your flaarum server in.
region:


// zone is the Google Cloud Zone which must be derived from the region above.
// for instance a region could be 'us-central1' and the zone could be 'us-central1-a'
zone:

// disk_size is the size of the root disk of the server. The data created is also stored in the root disk.
// It is measured in Gigabytes (GB) and a number is expected.
// 10 is the minimum.
disk_size: 10

// machine_type is the type of machine configuration to use to launch your flaarum server.
// You must get this value from the Google Cloud Compute documentation if not it would fail.
// It is not necessary it must be an e2 instance.
machine_type: e2-highcpu-2

`

		conf, err := zazabul.ParseConfig(tmpl)
		if err != nil {
			panic(err)
		}

		err = conf.Write(writePath)
		if err != nil {
			panic(err)
		}

		fmt.Printf("Edit the file at '%s' before launching.\n", writePath)

	case "lb":
		if len(os.Args) != 4 {
			color.Red.Println("The lb command expects a launch file and a service account credentials file.")
			os.Exit(1)
		}

		inputPath, err := flaarum_shared.GetFlaarumPath(os.Args[2])
		if err != nil {
			panic(err)
		}

		conf, err := zazabul.LoadConfigFile(inputPath)
		if err != nil {
			panic(err)
		}

		for _, item := range conf.Items {
			if item.Value == "" {
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

		diskSizeInt, err := strconv.ParseInt(conf.Get("disk_size"), 10, 64)
		if err != nil {
			color.Red.Println("The 'disk_size' variable must be a number greater or equal to 10")
			os.Exit(1)
		}

		var startupScript = `
#! /bin/bash

sudo snap install flaarum
sudo snap start flaarum.store
sudo flaarum.prod mpr
sudo snap restart flaarum.store
sudo snap stop --disable flaarum.statsr
`

		ctx := context.Background()

		computeService, err := compute.NewService(ctx, option.WithCredentialsFile(credentialsFilePath),
			option.WithScopes(compute.ComputeScope))
		if err != nil {
			panic(err)
		}

		op, err := computeService.Addresses.Insert(conf.Get("project"), conf.Get("region"), &compute.Address{
			AddressType: "INTERNAL",
			Description: "IP address for a flaarum server (" + instanceName + ").",
			Subnetwork:  "regions/" + conf.Get("region") + "/subnetworks/default",
			Name:        instanceName + "-ip",
		}).Context(ctx).Do()
		if err != nil {
			panic(err)
		}

		err = waitForOperationRegion(conf.Get("project"), conf.Get("region"), computeService, op)
		if err != nil {
			panic(err)
		}

		computeAddr, err := computeService.Addresses.Get(conf.Get("project"), conf.Get("region"), instanceName+"-ip").Context(ctx).Do()
		if err != nil {
			panic(err)
		}

		fmt.Println("Flaarum server address: ", computeAddr.Address)

		prefix := "https://www.googleapis.com/compute/v1/projects/" + conf.Get("project")

		image, err := computeService.Images.GetFromFamily("ubuntu-os-cloud", "ubuntu-minimal-2004-lts").Context(ctx).Do()
		if err != nil {
			panic(err)
		}
		imageURL := image.SelfLink

		instance := &compute.Instance{
			Name:        instanceName,
			Description: "flaarum instance",
			MachineType: prefix + "/zones/" + conf.Get("zone") + "/machineTypes/" + conf.Get("machine_type"),
			Disks: []*compute.AttachedDisk{
				{
					AutoDelete: true,
					Boot:       true,
					Type:       "PERSISTENT",

					InitializeParams: &compute.AttachedDiskInitializeParams{
						DiskName:    diskName,
						SourceImage: imageURL,
						DiskType:    prefix + "/zones/" + conf.Get("zone") + "/diskTypes/pd-ssd",
						DiskSizeGb:  diskSizeInt,
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
					Network:   prefix + "/global/networks/default",
					NetworkIP: computeAddr.Address,
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
			Metadata: &compute.Metadata{
				Items: []*compute.MetadataItems{
					{
						Key:   "startup-script",
						Value: &startupScript,
					},
				},
			},
		}

		op, err = computeService.Instances.Insert(conf.Get("project"), conf.Get("zone"), instance).Do()
		if err != nil {
			panic(err)
		}
		err = waitForOperationZone(conf.Get("project"), conf.Get("zone"), computeService, op)
		if err != nil {
			panic(err)
		}

		fmt.Println("Flaarum Server Name: " + instanceName)

	case "initas":

		var tmpl = `// project is the Google Cloud Project name
// It can be created either from the Google Cloud Console or from the gcloud command
project:

// region is the Google Cloud Region name
// Specify the region you want to launch your flaarum server in.
region:


// zone is the Google Cloud Zone which must be derived from the region above.
// for instance a region could be 'us-central1' and the zone could be 'us-central1-a'
zone:


// machine class is either 'e2' or 'n2d'.
// The n2d supports higher machines than the e2. But the e2 seems to be cheaper and it is the default
// in Google Cloud Console. Please consider the documentation for more details.
machine_class: e2

// disk_size is the size of the root disk of the server. The data created is also stored in the root disk.
// It is measured in Gigabytes (GB) and a number is expected.
// 10 is the minimum.
disk_size: 10

`
		configFileName := "las" + time.Now().Format("20060102T150405") + ".zconf"

		writePath, err := flaarum_shared.GetFlaarumPath(configFileName)
		if err != nil {
			panic(err)
		}

		conf, err := zazabul.ParseConfig(tmpl)
		if err != nil {
			panic(err)
		}

		err = conf.Write(writePath)
		if err != nil {
			panic(err)
		}

		fmt.Printf("Edit the file at '%s' before launching.\n", writePath)

	case "las":

		if len(os.Args) != 4 {
			color.Red.Println("The las command expects a launch file and a service account credentials file.")
			os.Exit(1)
		}

		inputPath, err := flaarum_shared.GetFlaarumPath(os.Args[2])
		if err != nil {
			panic(err)
		}

		conf, err := zazabul.LoadConfigFile(inputPath)
		if err != nil {
			panic(err)
		}

		for _, item := range conf.Items {
			if item.Value == "" {
				color.Red.Println("Every field in the launch file is compulsory.")
				os.Exit(1)
			}
		}

		credentialsFilePath, err := flaarum_shared.GetFlaarumPath(os.Args[3])
		if err != nil {
			panic(err)
		}

		suffix := strings.ToLower(flaarum_shared.UntestedRandomString(4))
		instanceName := fmt.Sprintf("flaarum-%s", suffix)
		diskName := fmt.Sprintf("%s-disk", instanceName)
		dataDiskName := fmt.Sprintf("%s-ddisk", instanceName)

		ctlInstanceName := fmt.Sprintf("flaarumctl-%s", suffix)
		ctlInstanceDisk := ctlInstanceName + "-disk"

		diskSizeInt, err := strconv.ParseInt(conf.Get("disk_size"), 10, 64)
		if err != nil {
			color.Red.Println("The 'disk_size' variable must be a number greater or equal to 10")
			os.Exit(1)
		}

		var startupScript = `
#! /bin/bash

sudo snap install flaarum
sudo snap start flaarum.store
sudo flaarum.prod mpr
sudo snap restart flaarum.statsr
`

		ctx := context.Background()

		computeService, err := compute.NewService(ctx, option.WithCredentialsFile(credentialsFilePath),
			option.WithScopes(compute.ComputeScope))
		if err != nil {
			panic(err)
		}

		op, err := computeService.Addresses.Insert(conf.Get("project"), conf.Get("region"), &compute.Address{
			AddressType: "INTERNAL",
			Description: "IP address for a flaarum server (" + instanceName + ").",
			Subnetwork:  "regions/" + conf.Get("region") + "/subnetworks/default",
			Name:        instanceName + "-ip",
		}).Context(ctx).Do()
		if err != nil {
			panic(err)
		}

		err = waitForOperationRegion(conf.Get("project"), conf.Get("region"), computeService, op)
		if err != nil {
			panic(err)
		}

		computeAddr, err := computeService.Addresses.Get(conf.Get("project"), conf.Get("region"), instanceName+"-ip").Context(ctx).Do()
		if err != nil {
			panic(err)
		}

		fmt.Println("Flaarum server address: ", computeAddr.Address)

		prefix := "https://www.googleapis.com/compute/v1/projects/" + conf.Get("project")

		image, err := computeService.Images.GetFromFamily("ubuntu-os-cloud", "ubuntu-minimal-2004-lts").Context(ctx).Do()
		if err != nil {
			panic(err)
		}
		imageURL := image.SelfLink

		op, err = computeService.Disks.Insert(conf.Get("project"), conf.Get("zone"), &compute.Disk{
			Description: "Data disk for a flaarum server (" + instanceName + ").",
			SizeGb:      diskSizeInt,
			Type:        prefix + "/zones/" + conf.Get("zone") + "/diskTypes/pd-ssd",
			Name:        dataDiskName,
		}).Context(ctx).Do()
		if err != nil {
			panic(err)
		}

		err = waitForOperationZone(conf.Get("project"), conf.Get("zone"), computeService, op)
		if err != nil {
			panic(err)
		}

		firstMT := "e2-highcpu-2"
		if conf.Get("machine_class") == "n2d" {
			firstMT = "n2d-highcpu-2"
		}
		instance := &compute.Instance{
			Name:        instanceName,
			Description: "flaarum data instance",
			MachineType: prefix + "/zones/" + conf.Get("zone") + "/machineTypes/" + firstMT,
			Disks: []*compute.AttachedDisk{
				{
					AutoDelete: true,
					Boot:       true,
					Type:       "PERSISTENT",

					InitializeParams: &compute.AttachedDiskInitializeParams{
						DiskName:    diskName,
						SourceImage: imageURL,
						DiskType:    prefix + "/zones/" + conf.Get("zone") + "/diskTypes/pd-ssd",
						DiskSizeGb:  10,
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
					Network:   prefix + "/global/networks/default",
					NetworkIP: computeAddr.Address,
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
			Metadata: &compute.Metadata{
				Items: []*compute.MetadataItems{
					{
						Key:   "startup-script",
						Value: &startupScript,
					},
				},
			},
		}

		op, err = computeService.Instances.Insert(conf.Get("project"), conf.Get("zone"), instance).Do()
		if err != nil {
			panic(err)
		}
		err = waitForOperationZone(conf.Get("project"), conf.Get("zone"), computeService, op)
		if err != nil {
			panic(err)
		}

		var startupScriptCtlInstance = `
#! /bin/bash

sudo snap install flaarum
`
		startupScriptCtlInstance += "\nsudo flaarum.prod masr " + conf.Get("project") + " " + conf.Get("zone")
		startupScriptCtlInstance += " " + instanceName + " " + computeAddr.Address + " " + conf.Get("machine_class") + " \n"
		startupScriptCtlInstance += `
sudo snap restart flaarum.gcpasr
`

		ctlInstance := &compute.Instance{
			Name:        ctlInstanceName,
			Description: "flaarum control instance",
			MachineType: prefix + "/zones/" + conf.Get("zone") + "/machineTypes/f1-micro",
			Disks: []*compute.AttachedDisk{
				{
					AutoDelete: true,
					Boot:       true,
					Type:       "PERSISTENT",

					InitializeParams: &compute.AttachedDiskInitializeParams{
						DiskName:    ctlInstanceDisk,
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
			Metadata: &compute.Metadata{
				Items: []*compute.MetadataItems{
					{
						Key:   "startup-script",
						Value: &startupScriptCtlInstance,
					},
				},
			},
		}

		op, err = computeService.Instances.Insert(conf.Get("project"), conf.Get("zone"), ctlInstance).Do()
		if err != nil {
			panic(err)
		}
		err = waitForOperationZone(conf.Get("project"), conf.Get("zone"), computeService, op)
		if err != nil {
			panic(err)
		}

		fmt.Println("Flaarum Server Name: " + instanceName)
		fmt.Println("Flaarum Control Server Name: ", ctlInstanceName)

	default:
		color.Red.Println("Unexpected command. Run the gcpl with --help to find out the supported commands.")
		os.Exit(1)
	}

}

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
)

const RootConfigTemplate = `// project is the Google Cloud project you want to launch the server into.
// you can create it from your Google Cloud Console.
project:

// zone is the Google Cloud zone you want to lauch your server into.
// please search for valid zones from Google Cloud.
zone:

// disk size is the size of disk for your flaarum server.
// it would be an SSD disk.
disk-size: 10

// machine-type-day is the machine type to use for your flaarum server during the day.
// machine-type determines the CPU and RAM configuration of your flaarum server.
// please search and get your machine-type from Google Cloud website.
machine-type-day: e2-highcpu-8

// machine-type-night is the machine type to use for your flaarum server at night.
// machine-type determines the CPU and RAM configuration of your flaarum server.
// please search and get your machine-type from Google Cloud website.
machine-type-night: e2-highcpu-2

// timezone values can be gotten online.
timezone: Africa/Lagos

`

func main() {
	if len(os.Args) < 2 {
		color.Red.Println("Expecting a command. Run with help subcommand to view help.")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "--help", "help", "h":
		fmt.Println(`lgcp creates and configures a flaarum server on Google Cloud.

Supported Commands:

    init     Creates a config file for auto scaling deployment. The method of autoscaling is to launch a large server in the
             morning and resize in the evening to a small server. Good for websites facing one country. Edit it to your own 
             requirements. Some of the values can be gotten from Google Cloud's documentation. 

    las      Launches an autoscaling deployment from the config created above. The deployment would have two servers.
             It expects a launch file (created from 'init' above) and a service account credentials file 
             (gotten from Google Cloud).

      `)

	case "init":

		confFilename := "las" + time.Now().Format("20060102T150405") + ".zconf"
		confPath, err := flaarum_shared.GetFlaarumPath(confFilename)
		if err != nil {
			panic(err)
		}

		conf, err := zazabul.ParseConfig(RootConfigTemplate)
		if err != nil {
			panic(err)
		}
		conf.Write(confPath)

		fmt.Printf("Edit the file at '%s' before launching.\n", confPath)

	case "las":

		if len(os.Args) != 4 {
			color.Red.Println("The las command expects a launch file and a service account credentials file.")
			os.Exit(1)
		}

		inputPath, err := flaarum_shared.GetFlaarumPath(os.Args[2])
		if err != nil {
			panic(err)
		}
		if !flaarum_shared.DoesPathExists(inputPath) {
			color.Red.Printf("The input %s does not exists.\n", inputPath)
			os.Exit(1)
		}

		conf, err := zazabul.LoadConfigFile(inputPath)
		if err != nil {
			color.Red.Printf("%+v\n", err)
			os.Exit(1)
		}

		for _, confItem := range conf.Items {
			if confItem.Value == "" {
				color.Red.Println("Every field in the launch file is compulsory.")
				os.Exit(1)
			}
		}

		if _, err = time.LoadLocation(conf.Get("timezone")); err != nil {
			color.Red.Println("invalid timezone: \n" + err.Error())
			os.Exit(1)
		}

		credentialsFilePath, err := flaarum_shared.GetFlaarumPath(os.Args[3])
		if err != nil {
			panic(err)
		}
		if !flaarum_shared.DoesPathExists(credentialsFilePath) {
			color.Red.Printf("The input %s does not exists.\n", inputPath)
			os.Exit(1)
		}

		suffix := strings.ToLower(flaarum_shared.UntestedRandomString(4))
		instanceName := fmt.Sprintf("flaarum-%s", suffix)
		diskName := fmt.Sprintf("%s-disk", instanceName)

		f103InstanceName := fmt.Sprintf("flaa103-%s", suffix)
		f103InstanceDisk := f103InstanceName + "-disk"

		diskSizeInt, err := strconv.ParseInt(conf.Get("disk-size"), 10, 64)
		if err != nil {
			color.Red.Println("The 'disk-size' variable must be a number greater or equal to 10")
			os.Exit(1)
		}

		var startupScript = `
#! /bin/bash

sudo apt update
sudo apt install nano
sudo snap install flaarum
sudo flaarum.prod genssl
sudo flaarum.prod mpr
sudo snap restart flaarum.store

`

		ctx := context.Background()
		computeService, err := compute.NewService(ctx, option.WithCredentialsFile(credentialsFilePath),
			option.WithScopes(compute.ComputeScope))
		if err != nil {
			panic(err)
		}

		prefix := "https://www.googleapis.com/compute/v1/projects/" + conf.Get("project")

		image, err := computeService.Images.GetFromFamily("ubuntu-os-cloud", "ubuntu-minimal-2204-lts").Context(ctx).Do()
		if err != nil {
			panic(err)
		}
		imageURL := image.SelfLink

		instance := &compute.Instance{
			Name:        instanceName,
			Description: "flaarum data instance",
			MachineType: prefix + "/zones/" + conf.Get("zone") + "/machineTypes/" + conf.Get("machine-type-day"),
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
						Value: &startupScript,
					},
				},
			},
		}

		op1, err := computeService.Instances.Insert(conf.Get("project"), conf.Get("zone"), instance).Do()
		if err != nil {
			panic(err)
		}

		var startupScriptControlInstance = fmt.Sprintf(`
#! /bin/bash

sudo apt update

# download the files
wget https://sae.ng/static/flaa103/gcresizer
wget https://sae.ng/static/flaa103/gcresizer.service
sudo cp gcresizer.service /etc/systemd/system/gcresizer.service

# put the files in place
sudo mkdir -p /opt/flaa103/
sudo cp gcresizer /opt/flaa103/gcresizer
sudo chmod +x /opt/flaa103/gcresizer

cat > /opt/flaa103/input.txt << EOF
%s
%s
%s
%s
%s
%s

EOF

# start the programs
sudo systemctl daemon-reload
sudo systemctl start gcresizer

`, conf.Get("project"), conf.Get("zone"), instanceName, conf.Get("timezone"),
			conf.Get("machine-type-day"), conf.Get("machine-type-night"),
		)

		ctlInstance := &compute.Instance{
			Name:        f103InstanceName,
			Description: "flaarum control instance",
			MachineType: prefix + "/zones/" + conf.Get("zone") + "/machineTypes/e2-small",
			Disks: []*compute.AttachedDisk{
				{
					AutoDelete: true,
					Boot:       true,
					Type:       "PERSISTENT",

					InitializeParams: &compute.AttachedDiskInitializeParams{
						DiskName:    f103InstanceDisk,
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
						Value: &startupScriptControlInstance,
					},
				},
			},
		}

		op2, err := computeService.Instances.Insert(conf.Get("project"), conf.Get("zone"), ctlInstance).Do()
		if err != nil {
			panic(err)
		}

		err = waitForOperation(conf.Get("project"), conf.Get("zone"), computeService, op1)
		if err != nil {
			panic(err)
		}
		err = waitForOperation(conf.Get("project"), conf.Get("zone"), computeService, op2)
		if err != nil {
			panic(err)
		}

		fmt.Println("Instance Name: " + instanceName)
		fmt.Println("Control Instance Name: ", f103InstanceName)
	}

}

func waitForOperation(project, zone string, service *compute.Service, op *compute.Operation) error {
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

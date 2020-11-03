package main

import (
	"os"
	"fmt"
	"github.com/bankole7782/flaarum"
	"github.com/bankole7782/flaarum/flaarum_shared"
	"github.com/go-co-op/gocron"
	"github.com/gookit/color"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/cpu"
	"time"
	"io/ioutil"
)


func main() {
	scheduler := gocron.NewScheduler(time.UTC)
	scheduler.Every(1).Minute().Do( storeStats )
	scheduler.StartBlocking()
}


func storeStats() {
	var keyStr string
	inProd, err := flaarum_shared.GetSetting("in_production")
	if err != nil {
		color.Red.Println("unexpected error. Have you installed  and launched flaarum?")
		os.Exit(1)	
	}
	if inProd == "true" || inProd == "t" {
		keyStrPath := flaarum_shared.GetKeyStrPath()
		raw, err := ioutil.ReadFile(keyStrPath)
		if err != nil {
			color.Red.Println(err)
			os.Exit(1)
		}
		keyStr = string(raw)
	} else {
		keyStr = "not-yet-set"
	}
	cl := flaarum.NewClient(fmt.Sprintf("https://127.0.0.1:%d/", flaarum_shared.PORT), keyStr, "first_proj")

	err = cl.Ping()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	tables, err := cl.ListTables()
	if err != nil {
		panic(err)
	}

	if flaarum_shared.FindIn(tables, "server_stats") == -1 {
		// table doesn't exist
		err = cl.CreateTable(`
			table: server_stats
			table-type: logs
			fields:
				cpu_usage float required
				ram_usage float required
			::
			`)
		if err != nil {
			panic(err)
		}
	}

	v, _ := mem.VirtualMemory()
	cpuPercent, _ := cpu.Percent(0, false)

	_, err = cl.InsertRowAny("server_stats", map[string]interface{} {
		"cpu_usage": cpuPercent[0], "ram_usage": v.UsedPercent,
	})
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Server stats for " + time.Now().String() + " recorded.")
}

// statsr repeatedly stores the CPU and RAM usage of a server. It is much needed for autoscaling deployments.
package main

import (
	"os"
	"fmt"
	"github.com/saenuma/flaarum"
	"github.com/saenuma/flaarum/flaarum_shared"
	"github.com/go-co-op/gocron"
	"github.com/gookit/color"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/cpu"
	"time"
	"github.com/pkg/errors"
)


func main() {
	scheduler := gocron.NewScheduler(time.UTC)
	scheduler.Every(1).Minute().Do( storeStats )
	scheduler.StartBlocking()
}


func storeStats() {
	var keyStr string
	inProd := flaarum_shared.GetSetting("in_production")
	if inProd == "" {
		color.Red.Println("unexpected error. Have you installed  and launched flaarum?")
		os.Exit(1)
	}
	if inProd == "true" {
		keyStrPath := flaarum_shared.GetKeyStrPath()
		raw, err := os.ReadFile(keyStrPath)
		if err != nil {
			color.Red.Println(err)
			os.Exit(1)
		}
		keyStr = string(raw)

		cl := flaarum.NewClient("127.0.0.1", keyStr, "first_proj")

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
				fields:
					cpu_usage float required
					ram_usage float required
				::
				`)
			if err != nil {
				panic(errors.Wrap(err, "flaarum table error"))
			}
		}

		v, _ := mem.VirtualMemory()
		cpuPercent, _ := cpu.Percent(0, false)

		_, err = cl.InsertRowAny("server_stats", map[string]interface{} {
			"cpu_usage": cpuPercent[0], "ram_usage": v.UsedPercent,
		})
		if err != nil {
			fmt.Printf("%+v\n", errors.Wrap(err, "flaarum insert error"))
		}

		fmt.Println("Server stats for " + time.Now().String() + " recorded.")
	} else {
		fmt.Println("Server not in production so not storing stats.")
		os.Exit(1)
	}

}

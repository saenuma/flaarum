package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/pkg/errors"
)

func getAndDeleteStats(w http.ResponseWriter, r *http.Request) {
	rows, err := innerSearch("first_proj", `
		table: server_stats

		`)
	if err != nil {
		printError(w, errors.Wrap(err, "read rows error"))
		return
	}

	var cpuUsageTotal, ramUsageTotal float64
	for _, r := range *rows {
		cpuUsageFloat, err := strconv.ParseFloat(r["cpu_usage"], 64)
		if err != nil {
			printError(w, errors.Wrap(err, "strconv error"))
			return
		}
		cpuUsageTotal += cpuUsageFloat
		ramUsageFloat, err := strconv.ParseFloat(r["ram_usage"], 64)
		if err != nil {
			printError(w, errors.Wrap(err, "strconv error"))
			return
		}
		ramUsageTotal += ramUsageFloat
	}

	jsonRet := map[string]int64{
		"cpu_avg": int64(math.Ceil(cpuUsageTotal / float64(len(*rows)))),
		"ram_avg": int64(math.Ceil(ramUsageTotal / float64(len(*rows)))),
	}

	err = innerDelete("first_proj", "server_stats", rows)
	if err != nil {
		printError(w, errors.Wrap(err, "delete error"))
		return
	}

	jsonBytes, err := json.Marshal(jsonRet)
	if err != nil {
		printError(w, errors.Wrap(err, "json error"))
		return
	}
	fmt.Fprint(w, string(jsonBytes))

}

package flaarum_shared

import (
	"os"
	"strings"
	"github.com/pkg/errors"
	"github.com/barkimedes/go-deepcopy"
	"fmt"
	"strconv"
	"time"
)


type TimeIndexes struct {
	GoTimeValue time.Time
	Ids []int64
}


func AppendToTimeIndexes(container []TimeIndexes, newTimeValue time.Time, rowId int64) []TimeIndexes {
	var index int
	newEntry := false
	oldEntry := false

	for i, element := range container {
		if element.GoTimeValue.Equal(newTimeValue) {
			oldEntry = true
			index = i
			break
		}
		if element.GoTimeValue.After(newTimeValue) {
			newEntry = true
			index = i
			break
		}
	}

	var newContainer []TimeIndexes
	if oldEntry {
		oldIds := container[index].Ids
		oldIds = append(oldIds, rowId)
		if index == 0 {
			newContainer = []TimeIndexes{TimeIndexes{newTimeValue, oldIds}}
			newContainer = append(newContainer, container[1: ]...)
		} else {
			preContainer, _ := deepcopy.Anything(container[0: index])
			postContainer, _ := deepcopy.Anything(container[index + 1: ])
			newContainer = append(preContainer.([]TimeIndexes), TimeIndexes{newTimeValue, oldIds})
			newContainer = append(newContainer,  postContainer.([]TimeIndexes)...)
		}
	} else if newEntry {
		if index == 0 {
			newContainer = []TimeIndexes{TimeIndexes{newTimeValue, []int64{rowId}}}
			newContainer = append(newContainer, container...)
		} else {
			preContainer := container[0: index]
			postContainer, _ := deepcopy.Anything(container[index: ])
			newContainer = append(preContainer, TimeIndexes{newTimeValue, []int64{rowId}})
			newContainer = append(newContainer, postContainer.([]TimeIndexes)...)
		}
	} else {
		newContainer = append(container, TimeIndexes{newTimeValue, []int64{rowId}})
	}

	return newContainer
}


func RemoveFromTimeIndexes(container []TimeIndexes, newTimeValue time.Time, rowId int64) []TimeIndexes {
	var index int
	found := false
	for i, item := range container {
		if item.GoTimeValue.Equal(newTimeValue) {
			found = true
			index = i
			break
		}
	}

	var newContainer []TimeIndexes

	if found {
		ids := container[index].Ids

		if len(ids) > 1 {
			remainingIds := make([]int64, 0)
			for _, idItem := range ids {
				if idItem == rowId {
					continue
				}
				remainingIds = append(remainingIds, idItem)
			}

			if index == 0 {
				newContainer = []TimeIndexes{TimeIndexes{newTimeValue, remainingIds}}
				newContainer = append(newContainer, container...)
			} else {
				preContainer := container[0: index]
				postContainer := container[index + 1: ]
				newContainer = append(preContainer, TimeIndexes{newTimeValue, remainingIds})
				newContainer = append(newContainer,  postContainer...)
			}

		} else {
			preContainer := container[0: index]
			postContainer, _ := deepcopy.Anything(container[index + 1: ])
			newContainer = append(preContainer, postContainer.([]TimeIndexes)...)
		}
	} else {
		newContainer = container
	}

	return newContainer
}


func ReadTimeIndexesFromFile(timeIndexesFile string, dataType string) ([]TimeIndexes, error) {
	ret := make([]TimeIndexes, 0)

	rawTimeIndexesFile, err := os.ReadFile(timeIndexesFile)
	if err != nil {
		return ret, errors.Wrap(err, "os error")
	}
	lines := strings.Split(string(rawTimeIndexesFile), "\n")
	for _, line := range lines {

		if len( strings.TrimSpace(line) ) == 0 {
			continue
		}
		
		lineParts := strings.Split(line, "::")
		
		var timeValue time.Time
		if dataType == "date" {
			timeValue, err = time.Parse(DATE_FORMAT, strings.TrimSpace(lineParts[0]))
			if err != nil {
				return ret, errors.Wrap(err, "time error")
			}
		} else if dataType == "datetime" {
			timeValue, err = time.Parse(DATETIME_FORMAT, strings.TrimSpace(lineParts[0]))
			if err != nil {
				return ret, errors.Wrap(err, "time error")
			}
		}
		
		ids := make([]int64, 0)
		for _, part := range strings.Split(lineParts[1], ",") {
			readId, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
			if err != nil {
				return ret, errors.Wrap(err, "strconv error")
			}
			ids = append(ids, readId)
		}

		ret = append(ret, TimeIndexes{timeValue, ids})
	}

	return ret , nil
}


func WriteTimeIndexesToFile(in []TimeIndexes, timeIndexesFile, dataType string) error {
	var sb strings.Builder
	for _, timeIndexes := range in {
		var innerSb strings.Builder
		ids := make([]string, 0)
		for i, intt := range timeIndexes.Ids {
			strOfInt := strconv.FormatInt(intt, 10)
			ids = append(ids, strOfInt)
			if i == len(timeIndexes.Ids) - 1 {
				innerSb.WriteString(strOfInt)
			} else {
				innerSb.WriteString(strOfInt + ",")
			}
		}

		var outTimeValue string
		if dataType == "date" {
			outTimeValue = timeIndexes.GoTimeValue.Format(DATE_FORMAT)
		} else if dataType == "datetime" {
			outTimeValue = timeIndexes.GoTimeValue.Format(DATETIME_FORMAT)
		}

		out := fmt.Sprintf("%s:: %s", outTimeValue, innerSb.String())
		sb.WriteString(out + "\n")
	}
	err := os.WriteFile(timeIndexesFile, []byte(sb.String()), 0777)
	if err != nil {
		return errors.Wrap(err, "os error")
	}
	return nil
}

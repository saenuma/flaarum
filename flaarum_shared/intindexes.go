package flaarum_shared

import (
	"os"
	"strings"
	"github.com/pkg/errors"
	"github.com/barkimedes/go-deepcopy"
	"fmt"
	"strconv"
)


type IntIndexes struct {
	IntIndex int64
	Ids []int64
}

func AppendToIntIndexes(container []IntIndexes, intKey, rowId int64) []IntIndexes {
	var index int
	newEntry := false
	oldEntry := false

	for i, element := range container {
		if element.IntIndex == intKey {
			oldEntry = true
			index = i
			break
		}
		if element.IntIndex > intKey {
			newEntry = true
			index = i
			break
		}
	}

	var newContainer []IntIndexes
	if oldEntry {
		oldIds := container[index].Ids
		oldIds = append(oldIds, rowId)
		if index == 0 {
			newContainer = []IntIndexes{IntIndexes{intKey, oldIds}}
			newContainer = append(newContainer, container[1: ]...)
		} else {
			preContainer, _ := deepcopy.Anything(container[0: index])
			postContainer, _ := deepcopy.Anything(container[index + 1: ])
			newContainer = append(preContainer.([]IntIndexes), IntIndexes{intKey, oldIds})
			newContainer = append(newContainer,  postContainer.([]IntIndexes)...)
		}
	} else if newEntry {
		if index == 0 {
			newContainer = []IntIndexes{IntIndexes{intKey, []int64{rowId}}}
			newContainer = append(newContainer, container...)
		} else {
			preContainer := container[0: index]
			postContainer, _ := deepcopy.Anything(container[index: ])
			newContainer = append(preContainer, IntIndexes{intKey, []int64{rowId}})
			newContainer = append(newContainer, postContainer.([]IntIndexes)...)
		}
	} else {
		newContainer = append(container, IntIndexes{intKey, []int64{rowId}})
	}

	return newContainer
}


func RemoveFromIntIndexes(container []IntIndexes, intKey, rowId int64) []IntIndexes {
	var index int
	found := false
	for i, item := range container {
		if item.IntIndex == intKey {
			found = true
			index = i
			break
		}
	}

	var newContainer []IntIndexes

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
				newContainer = []IntIndexes{IntIndexes{intKey, remainingIds}}
				newContainer = append(newContainer, container...)
			} else {
				preContainer := container[0: index]
				postContainer := container[index + 1: ]
				newContainer = append(preContainer, IntIndexes{intKey, remainingIds})
				newContainer = append(newContainer,  postContainer...)
			}

		} else {
			preContainer := container[0: index]
			postContainer, _ := deepcopy.Anything(container[index + 1: ])
			newContainer = append(preContainer, postContainer.([]IntIndexes)...)
		}
	} else {
		newContainer = container
	}

	return newContainer
}


func ReadIntIndexesFromFile(intIndexesFile string) ([]IntIndexes, error) {
	ret := make([]IntIndexes, 0)

	rawIntIndexesFile, err := os.ReadFile(intIndexesFile)
	if err != nil {
		return ret, errors.Wrap(err, "os error")
	}
	lines := strings.Split(string(rawIntIndexesFile), "\n")
	for _, line := range lines {
		colonIndex := 0

		for i, ch := range line {
			if fmt.Sprintf("%c", ch) == ":" {
				colonIndex = i
				break
			}
		}

		if colonIndex == 0 {
			continue
		}

		intIndex, err := strconv.ParseInt(line[0: colonIndex], 10, 64)
		if err != nil {
			return ret, errors.Wrap(err, "strconv error")
		}

		ids := make([]int64, 0)
		for _, part := range strings.Split(line[colonIndex + 1: ], ",") {
			readId, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
			if err != nil {
				return ret, errors.Wrap(err, "strconv error")
			}
			ids = append(ids, readId)
		}

		ret = append(ret, IntIndexes{intIndex, ids})
	}

	return ret , nil
}


func WriteIntIndexesToFile(in []IntIndexes, intIndexesFile string) error {
	var sb strings.Builder
	for _, intIndexes := range in {
		var innerSb strings.Builder
		ids := make([]string, 0)
		for i, intt := range intIndexes.Ids {
			strOfInt := strconv.FormatInt(intt, 10)
			ids = append(ids, strOfInt)
			if i == len(intIndexes.Ids) - 1 {
				innerSb.WriteString(strOfInt)
			} else {
				innerSb.WriteString(strOfInt + ",")
			}
		}

		out := fmt.Sprintf("%d: %s", intIndexes.IntIndex, innerSb.String())
		sb.WriteString(out + "\n")
	}
	err := os.WriteFile(intIndexesFile, []byte(sb.String()), 0777)
	if err != nil {
		return errors.Wrap(err, "os error")
	}
	return nil
}

package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type StationData struct {
	min float64
	max float64
	sum float64
	num int
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	file, err := os.Open("/Users/sam/Developer/1brc/small_measurements.txt")
	check(err)
	defer file.Close()

	stations := make(map[string]*StationData)
	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		check(scanner.Err())

		line := strings.Split(scanner.Text(), ";")
		name := line[0]
		temp, err := strconv.ParseFloat(line[1], 64)
		check(err)

		dat, ok := stations[name]
		if ok {
			if temp < dat.min {
				dat.min = temp
			}
			if temp > dat.max {
				dat.max = temp
			}
			dat.sum += temp
			dat.num++
		} else {
			stations[name] = &StationData{
				min: temp,
				max: temp,
				sum: temp,
				num: 1,
			}
		}
		if (i%50000000) == 0 && i != 0 {
			fmt.Printf("Parsed %d lines\n", i)
		}
		i++
	}

	for k, v := range stations {
		fmt.Printf("%-30s|%6.2f|%6.2f|%6.2f|\n", k, v.min, v.max, (v.sum / float64(v.num)))
	}
	fmt.Printf("\n%d stations total\n", len(stations))
}

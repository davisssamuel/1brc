package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

type StationData struct {
	min float64
	max float64
	sum float64
	num int
}

type Container struct {
	mu       sync.Mutex
	stations map[string]*StationData
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func (c *Container) ParseLine(line string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	s := strings.Split(line, ";")
	name := s[0]
	temp, err := strconv.ParseFloat(s[1], 64)
	check(err)

	dat, ok := c.stations[name]
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
		c.stations[name] = &StationData{
			min: temp,
			max: temp,
			sum: temp,
			num: 1,
		}
	}
}

func main() {
	file, err := os.Open("measurements.txt")
	check(err)
	defer file.Close()

	c := Container{stations: make(map[string]*StationData)}
	var wg sync.WaitGroup

	reader := bufio.NewReader(file)
	i := 0
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		check(err)
		if (i%50000000) == 0 && i != 0 {
			fmt.Printf(". ")
		}
		i++
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.ParseLine(strings.TrimSuffix(line, "\n"))
		}()
	}
	wg.Wait()

	for k, v := range c.stations {
		fmt.Printf("%-30s|%6.2f|%6.2f|%6.2f|\n", k, v.min, v.max, (v.sum / float64(v.num)))
	}
}

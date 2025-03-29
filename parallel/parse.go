package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type StationData struct {
	min   float64
	max   float64
	sum   float64
	count int
}

type StationsContainer struct {
	mu       sync.Mutex
	stations map[string]*StationData
}

func ProcessChunk(filePath string, offset int64, size int64, stations *StationsContainer, wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	section := io.NewSectionReader(file, offset, size)
	scanner := bufio.NewScanner(section)
	for scanner.Scan() {
		line := scanner.Text()
		slice := strings.Split(line, ";")
		name := slice[0]
		temp, err := strconv.ParseFloat(slice[1], 64)
		if err != nil {
			panic(err)
		}

		stations.mu.Lock()
		station, ok := stations.stations[name]
		if ok {
			if temp < station.min {
				station.min = temp
			}
			if temp > station.max {
				station.max = temp
			}
			station.sum += temp
			station.count++
		} else {
			stations.stations[name] = &StationData{
				min:   temp,
				max:   temp,
				sum:   temp,
				count: 1,
			}
		}
		stations.mu.Unlock()
	}
}

func main() {

	filePath := "/Users/sam/Developer/1brc/measurements.txt"
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Determine the number of workers based off CPU cores
	workers := runtime.GOMAXPROCS(0) - 1
	var wg sync.WaitGroup
	wg.Add(workers)

	// Set buffer size based off file size and number of workers
	fi, err := file.Stat()
	if err != nil {
		panic(err)
	}
	fileSize := fi.Size()
	bufferSize := int(math.Ceil(float64(fileSize) / float64(workers)))
	fmt.Printf("Setting buffer size to %d bytes\n", bufferSize)

	offset := int64(0)
	size := int64(bufferSize)
	s := StationsContainer{stations: make(map[string]*StationData)}

	// Split file into chunks
	for range workers {

		// Check if the offsetEnd exceeds the file size
		if offset+size > fileSize {
			size = fileSize
		} else {

			// Increase chunk size till a newline is reached
			b := make([]byte, 1)
			_, err = file.ReadAt(b, offset+size)
			if err != nil {
				panic(err)
			}
			for string(b) != "\n" {
				size++
				_, err = file.ReadAt(b, offset+size)
				if err != nil {
					panic(err)
				}
			}
		}

		// chunk := make([]byte, 50)
		// _, err = file.ReadAt(chunk, offsetEnd-50)
		// if err != nil {
		// 	panic(err)
		// }
		// Send chunk to be processed
		fmt.Printf("Creating worker to process %d-%d bytes...\n", offset, offset+size)
		go ProcessChunk(filePath, offset, size, &s, &wg)

		// Update the start of the next chunk
		offset = int64(offset + size + 1)
		size = int64(bufferSize)
	}
	wg.Wait()

	// Print results
	for k, v := range s.stations {
		fmt.Printf("%-30s|%6.2f|%6.2f|%6.2f|\n", k, v.min, v.max, (v.sum / float64(v.count)))
	}
	fmt.Printf("\n%d stations total\n", len(s.stations))
}

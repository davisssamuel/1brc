package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
)

type Data struct {
	name string
	temp float64
}

type StationData struct {
	min float64
	max float64
	sum float64
	num int
}

// type Container struct {
// 	mu       sync.Mutex
// 	stations map[string]*StationData
// }

func processChunks(chunks <-chan string, dat chan<- *Data, done chan<- bool) {
	for chunk := range chunks {
		fmt.Printf("Processing chunk...\n")
		lines := strings.Split(chunk, "\n")
		lines = lines[:len(lines)-1]
		for _, line := range lines {
			l := strings.Split(line, ";")
			temp, err := strconv.ParseFloat(l[1], 64)
			check(err)
			dat <- &Data{l[0], temp}
		}
	}
	done <- true
}

func proccessLine(lines <-chan string, dat chan<- *Data, done chan<- bool) {
	for line := range lines {
		l := strings.Split(line, ";")
		temp, err := strconv.ParseFloat(l[1], 64)
		check(err)
		dat <- &Data{l[0], temp}
	}
	done <- true
}

func calculateData(dat <-chan *Data, result chan<- map[string]*StationData) {
	stations := make(map[string]*StationData)
	for d := range dat {
		name := d.name
		temp := d.temp

		station, ok := stations[name]
		if ok {
			if temp < station.min {
				station.min = temp
			}
			if temp > station.max {
				station.max = temp
			}
			station.sum += temp
			station.num++
		} else {
			stations[name] = &StationData{
				min: temp,
				max: temp,
				sum: temp,
				num: 1,
			}
		}
	}
	result <- stations
}

func check(err error) {
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}
}

func main() {

	file, err := os.Open("/Users/sam/Developer/1brc/measurements.txt")
	check(err)
	defer file.Close()

	// c := Container{stations: make(map[string]*StationData)}
	// stations := make(map[string]*StationData)

	workers := 2 * runtime.GOMAXPROCS(0)

	fi, err := file.Stat()
	check(err)
	bufferSize := int(fi.Size()) / workers

	chunks := make(chan string)
	// lines := make(chan string)
	dat := make(chan *Data)
	result := make(chan map[string]*StationData)
	done := make(chan bool)

	for i := 0; i < workers; i++ {
		go processChunks(chunks, dat, done)
	}
	go calculateData(dat, result)

	reader := bufio.NewReader(file)
	for {
		buffer := make([]byte, bufferSize)
		n, err := reader.Read(buffer)
		check(err)
		for n > 0 && buffer[n-1] != '\n' {
			b, err := reader.ReadByte()
			check(err)
			buffer = append(buffer[:n], b)
			n++
		}
		chunks <- string(buffer[:n])

		if err == io.EOF {
			break
		}

		// print status to console
		fmt.Printf("Read %d bytes\n", len(buffer))
	}
	close(chunks)

	for i := 0; i < workers; i++ {
		<-done
	}
	close(dat)
	close(done)

	stations := <-result

	for k, v := range stations {
		fmt.Printf("%-30s|%6.2f|%6.2f|%6.2f|\n", k, v.min, v.max, (v.sum / float64(v.num)))
	}
	fmt.Printf("\n%d stations total\n", len(stations))
}

package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
)

type Buffer struct {
	offset int64
	size   int
}

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

func processChunks(file os.File, chunks <-chan *Buffer, dat chan<- *Data, done chan<- bool) {
	for chunk := range chunks {

		buffer := make([]byte, chunk.size)
		n, err := file.ReadAt(buffer, chunk.offset)
		check(err)

		lines := strings.Split(string(buffer[:n]), "\n")
		lines = lines[:len(lines)-1]
		for _, line := range lines {
			s := strings.Split(line, ";")
			name := s[0]
			temp, err := strconv.ParseFloat(s[1], 64)
			check(err)

			dat <- &Data{name, temp}
		}
	}
	done <- true
}

func calculateData(dat <-chan *Data, result chan<- map[string]*StationData) {
	stations := make(map[string]*StationData)

	i := 1
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
		if (i % 50000000) == 0 {
			fmt.Printf("Parsed %d lines\n", i)
		}
		i++
	}
	result <- stations
}

func check(err error) {
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}
}

func main() {

	file, err := os.Open("/Users/sam/Developer/1brc/small_measurements.txt")
	check(err)
	defer file.Close()

	workers := runtime.GOMAXPROCS(0)
	// workers := 100000
	// workers := 3

	fi, err := file.Stat()
	check(err)
	fmt.Printf("%d\n", int(fi.Size()))
	bufferSize := int(math.Pow(2, math.Floor(math.Log2(float64(fi.Size())/100))))

	// bufferSize := 1_000_000_000 // 1 GB
	// bufferSize := 500_000_000   // 0.5 GB
	// bufferSize := 250_000_000   // 0.25 GB
	// bufferSize := 10_000_000    // 10 MB

	chunks := make(chan *Buffer)
	dat := make(chan *Data)
	result := make(chan map[string]*StationData)
	done := make(chan bool)

	for i := 0; i < workers; i++ {
		go processChunks(*file, chunks, dat, done)
	}
	go calculateData(dat, result)

	offsetStart := int64(0)
	offsetEnd := bufferSize
	b := make([]byte, 1)

	fmt.Printf("bufferSize: %d\n", bufferSize)

	for err != io.EOF {
		_, err = file.ReadAt(b, int64(offsetEnd))
		check(err)
		for string(b) != "\n" {
			offsetEnd++
			_, err = file.ReadAt(b, int64(offsetEnd))
			check(err)
		}

		// for debugging
		// chunk := make([]byte, (offsetEnd - int(offsetStart)))
		// _, _ = file.ReadAt(chunk, offsetStart)
		// fmt.Printf(">>> bytes %d to %d\n%s\n\n", offsetStart, offsetEnd, chunk)

		chunks <- &Buffer{offset: offsetStart, size: (offsetEnd - int(offsetStart))}

		offsetStart = int64(offsetEnd + 1)
		offsetEnd += bufferSize

		// time.Sleep(3 * time.Second)
	}
	close(chunks)

	for i := 0; i < workers; i++ {
		<-done
	}
	close(done)
	close(dat)

	stations := <-result

	close(result)

	for k, v := range stations {
		fmt.Printf("%-30s|%6.2f|%6.2f|%6.2f|\n", k, v.min, v.max, (v.sum / float64(v.num)))
	}
	fmt.Printf("\n%d stations total\n", len(stations))
}

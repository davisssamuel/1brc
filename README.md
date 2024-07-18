# The One Billion Row Challenge

[The challenge](https://www.morling.dev/blog/one-billion-row-challenge/) is simple: given a text file containing 1 billion rows, where each row is a temperature reading from a given weather station, calculate the min, max, and mean temperatures for each weather station *as fast as possible*. I chose to tackle the challenge using Go because I wanted to make use of Goroutines.

# Sequential Solution

Reading in a file, splitting lines, and calculating data is not incredibly complicated. The hard part is making it fast. I knew reading the 14 GB measurements file sequentially or line-by-line would be slow. Nevertheless, I created a sequential solution so I could compare benchmarks between it and the parallel solution.  

# Parallel Solution

Because I can only calculate data as fast as I can get it from the operating system, my idea was to split the file into chunks, sending each chunk to 1 of n routines to be parsed.

## July 2024

As of July 2024, my solution is create chunks and send them through a channel to 1 of n routines that parses each line in the chunk and sends the weather station name and reading through another channel to a single routine that updates a dictionary containing the min, max, and mean readings for each station. 

I create each chunk by reading n bytes into the file using the [`ReadAt`](https://pkg.go.dev/os#File.ReadAt) method. This allows me to skip a specific number of bytes into the file instead of having to read line-by-line. If the character `ReadAt` lands on is not a newline, I keep reading byte-by-byte, incrementing the `offsetEnd`, till I hit a newline character. Then, based off the `offsetStart` and `offsetEnd`, I determine the size of the chunk and send the `offset` and `size` through the `chunks` channel to one of the routines that parses the chunk. After the chunk is sent, I set the `offsetStart` to the `offsetEnd` and read n bytes again.

```go
offsetStart := int64(0)
offsetEnd := bufferSize
b := make([]byte, 1)

for err != io.EOF {
	_, err = file.ReadAt(b, int64(offsetEnd))
	check(err)
	for string(b) != "\n" {
		offsetEnd++
		_, err = file.ReadAt(b, int64(offsetEnd))
		check(err)
	}
	chunks <- &Buffer{offset: offsetStart, size: (offsetEnd - int(offsetStart))}
	offsetStart = int64(offsetEnd + 1)
	offsetEnd += bufferSize
}
```

Unfortunately, this solution is slower than sequentially parsing, which is something I'm working to resolve whenever I can. Right now, sequential parsing takes approx. 3-4 min, whereas parallel parsing takes approx. 9-10 min.

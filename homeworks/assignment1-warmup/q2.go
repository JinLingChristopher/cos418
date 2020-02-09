package cos418_hw1_1

import (
	"bufio"
	"io"
	"math"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// TODO: implement me
	// HINT: use for loop over `nums`
	partial := 0
	for x := range nums {
		partial += x
	}
	out <- partial
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.


func sum(num int, fileName string) int {
	// TODO: implement me
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers
	file, err := os.Open(fileName)
	checkError(err)
	defer file.Close()
	nums, err := readInts(file)
	checkError(err)

	numChans := make([]chan int, num)
	out := make(chan int, num)
	subLen := (len(nums) / num) + 1

	for i := 0; i < num; i++ {

		numChans[i] = make(chan int, subLen)
		go sumWorker(numChans[i], out)

		left := i * subLen
		right := int(math.Min(float64(len(nums)), float64((i+1)*subLen)))
		go func(ch chan int, a, b int) {
			for j := a; j < b; j++ {
				ch <- nums[j]
			}
			close(ch)
		}(numChans[i], left, right)
	}

	result := 0
	for i:= 0; i < num; i++ {
		result += <- out
	}

	return result
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}

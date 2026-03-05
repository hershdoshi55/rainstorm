package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

type TupleID struct {
	Stage     int    `json:"stage"`
	TaskIndex int    `json:"task_index"`
	Seq       uint64 `json:"seq"`
}

type TupleType string

const (
	Data TupleType = "DATA"
	EOS  TupleType = "EOS"
	Done TupleType = "DONE"
)

// Tuple is the unit of data passed between stages.
type Tuple struct {
	ID    TupleID   `json:"id"` // This ID indicates the origin of the tuple
	Type  TupleType `json:"type"`
	Key   string    `json:"key"`
	Value string    `json:"value"`
}

var logger *log.Logger

// write a file where the input will be a tuple,
// and inside it, will have a dict of key value pairs
// which stores a aggregate by key it received in the Tuple.
func init() {
	// Log file path using current timestamp
	filename := fmt.Sprintf("/home/mp4/aggregate-wrapper-%s.log", time.Now().Format("20060102-150405"))
	f, err := os.OpenFile(filename,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		// Fall back to stderr ONLY if log file fails
		logger = log.New(os.Stderr, "[grep-wrapper] ", log.LstdFlags)
		logger.Println("Failed to open log file:", err)
		return
	}

	logger = log.New(f, "[aggregate-wrapper] ", log.LstdFlags)
}

func main() {

	if len(os.Args) > 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s \n", os.Args[0])
		os.Exit(1)
	}

	counts := make(map[string]int64)

	sc := bufio.NewScanner(os.Stdin)

	logger.Println("Program started, waiting for input tuples...")

	for sc.Scan() {
		line := sc.Text()

		logger.Println("Input line:", line)

		// -------- 1. Parse input tuple --------

		var t Tuple
		if err := json.Unmarshal([]byte(line), &t); err != nil {
			logger.Println("Failed to parse input tuple:", err)
			continue
		}

		logger.Println("Decoded tuple:", t)

		// -------- 2. Process tuple based on type --------

		if t.Type == EOS {
			// On receiving EOS, emit counts
			for key, count := range counts {
				out := Tuple{
					ID:    t.ID, // use EOS's ID for aggregates
					Type:  Data,
					Key:   key,
					Value: fmt.Sprintf("%d", count),
				}
				outBytes, err := json.Marshal(out)
				if err != nil {
					logger.Println("Failed to marshal output tuple:", err)
					continue
				}
				fmt.Println(string(outBytes))
			}

			// Then emit EOS downstream
			out := Tuple{
				ID:    t.ID,
				Type:  EOS,
				Key:   "",
				Value: "",
			}
			outBytes, err := json.Marshal(out)
			if err != nil {
				logger.Println("Failed to marshal output tuple:", err)
				continue
			}
			fmt.Println(string(outBytes))
			logger.Println("Emitted EOS tuple, terminating aggregation for this stream.")
			continue
		}
		// DATA input: update aggregate
		logger.Println("Aggregating key:", t.Key)
		counts[t.Key]++
		logger.Println("Current count for key", t.Key, "is", counts[t.Key])

		// Emit DONE for this input (even though we don't emit DATA per-row)
		emitDone(t.ID)
	}

	logger.Println("Program finished.")
}

func emitDone(id TupleID) {
	out := Tuple{
		ID:    id,
		Type:  Done,
		Key:   "",
		Value: "",
	}
	b, err := json.Marshal(out)
	if err != nil {
		logger.Println("Failed to marshal DONE tuple:", err)
		return
	}
	fmt.Println(string(b))
	logger.Println("Emitted DONE tuple:", string(b))
}

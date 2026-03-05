package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
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

func init() {
	// Log file path using current timestamp
	filename := fmt.Sprintf("/home/mp4/grep-wrapper-%s.log", time.Now().Format("20060102-150405"))
	f, err := os.OpenFile(filename,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		// Fall back to stderr ONLY if log file fails
		logger = log.New(os.Stderr, "[grep-wrapper] ", log.LstdFlags)
		logger.Println("Failed to open log file:", err)
		return
	}

	logger = log.New(f, "[grep-wrapper] ", log.LstdFlags)
}

func main() {

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <pattern> \n", os.Args[0])
		os.Exit(1)
	}

	// pattern to search within the CSV line
	pattern := os.Args[1]

	// column index to extract
	// colIndex := atoiOrExit(os.Args[2])

	sc := bufio.NewScanner(os.Stdin)

	logger.Println("Program started. Pattern =", pattern)

	for sc.Scan() {
		line := sc.Text()

		logger.Println("Input line:", line)

		// -------- 1. Decode JSON Tuple --------
		var t Tuple
		if err := json.Unmarshal([]byte(line), &t); err != nil {
			logger.Println("JSON unmarshal failed:", err)
			continue
		}

		logger.Println("Decoded tuple:", t)

		if t.Type == EOS {
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
			logger.Println("Emitted EOS tuple, terminating.")
			continue
		}

		// -------- Non-EOS (DATA) input --------

		logger.Println("t.Value:", t.Value)

		csvLine := string(t.Value)
		logger.Println("csvLine:", csvLine)

		// -------- 2. Perform grep on FULL CSV line --------
		if !strings.Contains(csvLine, pattern) {
			// filtered out, but we still owe a DONE so upstream can ACK this input
			logger.Println("Pattern not found in csvLine; emitting DONE only.")
			emitDone(t.ID)
			continue
		}

		// -------- 3. Build output DATA tuple --------
		out := Tuple{
			ID:    t.ID,
			Type:  Data,    // explicitly mark as DATA
			Key:   t.Key,   // preserve key
			Value: csvLine, // full line as value
		}

		logger.Println("Built output DATA tuple (before Marshal):", out)

		// -------- 4. Emit DATA JSON to stdout --------
		b, err := json.Marshal(out)
		if err != nil {
			logger.Println("Failed to marshal output DATA tuple:", err)
			// still emit DONE so upstream can ACK
			emitDone(t.ID)
			continue
		}

		fmt.Println(string(b))
		logger.Println("Emitted DATA tuple (b):", string(b))

		// -------- 5. Emit DONE for this input tuple --------
		emitDone(t.ID)

	}

	logger.Println("Scanner ended, exiting program.")
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

// func atoiOrExit(s string) int {
// 	var x int
// 	_, err := fmt.Sscanf(s, "%d", &x)
// 	if err != nil {
// 		fmt.Fprintf(os.Stderr, "invalid column index: %s\n", s)
// 		os.Exit(1)
// 	}
// 	return x
// }

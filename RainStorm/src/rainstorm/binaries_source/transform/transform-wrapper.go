package main

import (
	"bufio"
	"encoding/csv"
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
	filename := fmt.Sprintf("/home/mp4/transform-wrapper-%s.log", time.Now().Format("20060102-150405"))
	f, err := os.OpenFile(filename,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		// Fall back to stderr ONLY if log file fails
		logger = log.New(os.Stderr, "[transform-stage2] ", log.LstdFlags)
		logger.Println("Failed to open log file:", err)
		return
	}

	logger = log.New(f, "[transform-wrapper] ", log.LstdFlags)
}

func main() {
	// No command-line args needed for this stage
	logger.Println("Transform started: output first three CSV fields")

	sc := bufio.NewScanner(os.Stdin)

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

		// -------- 2. Handle EOS --------
		if t.Type == EOS {
			out := Tuple{
				ID:    t.ID,
				Type:  EOS,
				Key:   "",
				Value: "",
			}
			outBytes, err := json.Marshal(out)
			if err != nil {
				logger.Println("Failed to marshal EOS output tuple:", err)
				continue
			}
			fmt.Println(string(outBytes))
			logger.Println("Emitted EOS tuple, terminating.")
			continue
		}

		logger.Println("t.Value:", t.Value)

		csvLine := string(t.Value)
		logger.Println("csvLine:", csvLine)

		// -------- 3. Parse CSV --------
		r := csv.NewReader(strings.NewReader(csvLine))
		record, err := r.Read()
		if err != nil {
			logger.Println("CSV parse failed:", err)
			continue
		}

		if len(record) < 3 {
			logger.Println("Record has fewer than 3 fields, skipping. Fields:", len(record))
			continue
		}

		firstThree := record[0:3]
		outValue := strings.Join(firstThree, ",")
		logger.Println("First three fields joined:", outValue)

		// -------- 4. Build output tuple --------
		out := Tuple{
			ID:    t.ID,
			Type:  t.Type,
			Key:   t.Key,    // No specific key for this stage
			Value: outValue, // Only first three fields
		}

		logger.Println("Built output tuple (before Marshal):", out)

		// -------- 5. Emit JSON to stdout --------
		b, err := json.Marshal(out)
		if err != nil {
			logger.Println("Failed to marshal output tuple:", err)
			continue
		}

		fmt.Println(string(b))
		logger.Println("Emitted tuple (b):", string(b))

		// Now emit DONE for this input tuple
		emitDone(t.ID)
	}

	if err := sc.Err(); err != nil {
		logger.Println("Scanner error:", err)
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

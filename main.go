package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
)

const chunkSize = 16

var mu sync.Mutex
var store map[string]int
var counts map[string]int

type Event struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func binWriter(id int, file *os.File, wg *sync.WaitGroup, jobs <-chan Event) {
	defer wg.Done()

	for {
		select {
		case task, ok := <-jobs:
			if ok {
				ukey, err := uuid.FromString(task.Key)
				if err != nil {
					log.Fatal(err)
				}
				uvalue, err := uuid.FromString(task.Value)
				if err != nil {
					log.Fatal(err)
				}
				// write a chunk
				if _, err := file.Write(ukey.Bytes()); err != nil {
					panic(err)
				}
				if _, err := file.Write(uvalue.Bytes()); err != nil {
					panic(err)
				}
			} else {
				log.Println("===> workerFunc got closed jobs channel <===")
				return
			}
		}
	}
}

func search(id int, wg *sync.WaitGroup, jobs <-chan string, results chan<- string) {
	defer wg.Done()
	for {
		select {

		case task, ok := <-jobs:
			if ok {
				// IF WE HAVE THIS UUID IN STORE, COUNT IT IN OUR COUNTS COLLECTION
				if _, ok := store[task]; ok {
					mu.Lock()
					counts[task] = store[task]
					mu.Unlock()
				}
				log.Println("worker", id, "finished job")
				results <- fmt.Sprintf("UUID: %s found %d times \n", task, counts[task])
			} else {
				log.Println("===> workerFunc got closed jobsChan <===")
				return
			}
		}
	}
}

func main() {

	var generate bool
	flag.BoolVar(&generate, "g", false, "Specify if db.bin needs to generated.")

	flag.Usage = func() {
		log.Printf("Usage: \n")
		log.Printf("./main user-provided-uuid-#1 user-provided-uuid-#2 [Space separated list of UUID to search]\n")
		log.Printf("./main -g [Weather to generate db.bin]\n")
		// flag.PrintDefaults() // prints default usage
	}
	flag.Parse()

	// OUR TEMP BUFF IS 16bytes long
	buffer := make([]byte, chunkSize)

	if generate {

		// LETS START MEASUING HOW LONG IT TAKES TO SEARCH FOR UUIDs
		start := time.Now()

		file, err := os.Create("db.bin")

		if err != nil {
			log.Fatal(err)
		}

		jsonFile, err := os.Open("events.json")
		if err != nil {
			log.Fatal(err)
		}

		scanner := bufio.NewScanner(jsonFile)
		var event = Event{}

		var wg0 sync.WaitGroup
		wg0.Add(1)
		jobChannelWriter := make(chan Event, 10)

		go binWriter(1, file, &wg0, jobChannelWriter)

		for scanner.Scan() {
			// read json line as a byte array and unmarshal it into an Event struct
			if err := json.Unmarshal([]byte(scanner.Text()), &event); err != nil {
				log.Fatal(err)
			}
			// PUMP EVENTS INTO A JOB CHANNEL WHERE A CONSUMER WILL BE WRITING THEM TO FILE
			jobChannelWriter <- event
		}
		close(jobChannelWriter)
		wg0.Wait()
		log.Println("===> DONE WRITING DB.BIN <===")
		file.Close()
		jsonFile.Close()

		elapsed := time.Since(start)
		log.Printf("===> writing db.bin took %s <===", elapsed)
	}

	var searchUUIDs []string
	searchUUIDs = flag.Args()

	store = make(map[string]int)
	counts = make(map[string]int)

	// LETS START MEASUING HOW LONG IT TAKES TO SEARCH FOR UUIDs
	start := time.Now()

	f, e := os.Open("db.bin")
	if e != nil {
		panic(e)
	}
	defer f.Close()

	// LETS FIRST READ FILE AND STORE ALL UUIDs IN THE GLOBAL STORE
	log.Println("===> READING BACK <===")
	for {
		bytesread, err := f.Read(buffer)

		if err != nil {
			if err != io.EOF {
				panic(err)
			}
			break
		}
		store[uuid.FromBytesOrNil(buffer[:bytesread]).String()]++
	}

	var wg sync.WaitGroup
	jobChannel := make(chan string, len(searchUUIDs))
	jobResultChannel := make(chan string, len(searchUUIDs))

	// CREATE OUR JOBS, SEND UUID INTO A JOBS CHANNEL TO BE SEARCHED BY WORKERS
	for i := 0; i < len(searchUUIDs); i++ {
		wg.Add(1)
		jobChannel <- searchUUIDs[i]
	}

	// START WORKERS, 1 FOR EACH UUID THAT WE ARE SEARCHING
	for i := 0; i < len(searchUUIDs); i++ {
		go search(i, &wg, jobChannel, jobResultChannel)
	}

	close(jobChannel)
	wg.Wait()
	close(jobResultChannel)

	fmt.Println("========================== RESULTS ============================")
	for a := 1; a <= len(searchUUIDs); a++ {
		fmt.Printf("%s \n", <-jobResultChannel)
	}

	elapsed := time.Since(start)
	log.Printf("All work took %s", elapsed)
}

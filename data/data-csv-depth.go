package data

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/beaquant/utils"
	"log"
	"os"
	"strings"
	"time"

	gbt "github.com/dirkolbrich/gobacktest"
)

// TickerEventFromCSVeData loads the market data from a SQLite database.
// It expands the underlying data struct.
type DepthEventFromCSVeData struct {
	gbt.Data
	FileDir string
}

// Load single data events into a stream ordered by date (latest first).
func (d *DepthEventFromCSVeData) Load(symbols []string) error {
	// check file location
	if len(d.FileDir) == 0 {
		return errors.New("no directory for data provided: ")
	}

	// create a map for holding the file name for each symbol
	files := make(map[string]string)

	// read all files from directory
	if len(symbols) == 0 {
		files, err := fetchFilesFromDir(d.FileDir)
		if err != nil {
			return err
		}
		log.Printf("%v data files found.\n", len(files))
	}

	// construct filenames for provided symbols
	for _, symbol := range symbols {
		file := symbol + ".csv"
		files[symbol] = file
	}
	log.Printf("Loading %v symbol files.\n", len(files))

	// read file for each fileName
	for symbol, file := range files {
		log.Printf("Loading %s file for %s symbol.\n", file, symbol)

		// open file for corresponding symbol
		lines, err := readDepthFromCSVFile(d.FileDir + file)
		if err != nil {
			return err
		}
		log.Printf("%v data lines found.\n", len(lines))

		// for each found record create an event
		for _, line := range lines {
			event, err := createDepthEventFromLine(line, symbol)
			if err != nil {
				log.Println(err)
			}
			d.Data.SetStream(append(d.Data.Stream(), event))
		}
	}

	// sort data stream
	d.Data.SortStream()

	return nil
}

// readOrderbookFromCSVFile opens and reads a csv file line by line
// and returns a slice with a key/value map for each line.
func readDepthFromCSVFile(path string) (lines []map[string]string, err error) {
	log.Printf("Loading from %s.\n", path)
	// open file
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// create scanner on top of file
	reader := csv.NewReader(file)
	// set delimeter
	reader.Comma = ','
	// read first line for keys and fill in array
	keys, err := reader.Read()

	// read each line and create a map of values combined to the keys
	for line, err := reader.Read(); err == nil; line, err = reader.Read() {
		l := make(map[string]string)
		for i, v := range line {
			l[keys[i]] = v
		}
		// put found line as map into stream holder item
		lines = append(lines, l)
	}

	return lines, nil
}

// createTickerEventFromLine takes a key/value map and a string and builds a bar struct.
func createDepthEventFromLine(line map[string]string, symbol string) (dep gbt.DepthEvent, err error) {
	// parse each string in line to corresponding record value
	timestamp := utils.ToInt64(line["t"])
	date := time.Unix(0, timestamp*1000000)
	a := line["a"]
	b := line["b"]

	asks := make([][]float64, 0)
	err = json.Unmarshal([]byte(a), &asks)
	if err != nil {
		fmt.Println(err)
	}
	bids := make([][]float64, 0)
	err = json.Unmarshal([]byte(b), &bids)
	if err != nil {
		fmt.Println(err)
	}
	asklist := make(gbt.DepthList, 0)
	for _, v := range asks {
		asklist = append(asklist, gbt.List{
			Price:    v[0],
			Quantity: v[1],
		})
	}
	bidlist := make(gbt.DepthList, 0)
	for _, v := range bids {
		bidlist = append(bidlist, gbt.List{
			Price:    v[0],
			Quantity: v[1],
		})
	}
	// create and populate new event
	event := &gbt.Event{}
	event.SetTime(date)
	event.SetSymbol(strings.ToUpper(symbol))

	metric := &gbt.Metric{}

	dep = &gbt.Depth{
		Event:  *event,
		Metric: *metric,
		Asks:   asklist,
		Bids:   bidlist,
	}

	return dep, nil
}

package data

import (
	"errors"
	"log"
	"strconv"
	"strings"
	"time"

	gbt "github.com/dirkolbrich/gobacktest"
)

// BarEventFromSQLiteData loads the market data from a SQLite database.
// It expands the underlying data struct.
type BarEventFromSQLiteData struct {
	gbt.Data
	FileDir string
}

// Load single data events into a stream ordered by date (latest first).
func (d *BarEventFromSQLiteData) Load(symbols []string) error {
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
		lines, err := readCSVFile(d.FileDir + file)
		if err != nil {
			return err
		}
		log.Printf("%v data lines found.\n", len(lines))

		// for each found record create an event
		for _, line := range lines {
			event, err := createBarEventFromLine(line, symbol)
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

// createBarEventFromLine takes a key/value map and a string and builds a bar struct.
func createTickerTickerEventFromEntry(line map[string]string, symbol string) (bar gbt.BarEvent, err error) {
	// parse each string in line to corresponding record value
	date, _ := time.Parse("2006-01-02", line["Date"])
	openPrice, _ := strconv.ParseFloat(line["Open"], 64)
	highPrice, _ := strconv.ParseFloat(line["High"], 64)
	lowPrice, _ := strconv.ParseFloat(line["Low"], 64)
	closePrice, _ := strconv.ParseFloat(line["Close"], 64)
	adjClosePrice, _ := strconv.ParseFloat(line["Adj Close"], 64)
	volume, _ := strconv.ParseInt(line["Volume"], 10, 64)

	// create and populate new event
	event := &gbt.Event{}
	event.SetTime(date)
	event.SetSymbol(strings.ToUpper(symbol))

	metric := &gbt.Metric{}

	bar = &gbt.Bar{
		Event:    *event,
		Metric:   *metric,
		Open:     openPrice,
		High:     highPrice,
		Low:      lowPrice,
		Close:    closePrice,
		AdjClose: adjClosePrice,
		Volume:   volume,
	}

	return bar, nil
}

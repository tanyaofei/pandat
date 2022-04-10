package pandat

import (
	"encoding/csv"
	"io"
	"os"
)

type ReadCsvOption struct {
	NoHeader         bool
	AlwaysQuotes     bool
	TrimLeadingSpace bool
	Separator        rune
}

func ReadCsvPath(filepath string, option ReadCsvOption) (*DataFrame[any], error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	return ReadCsv(f, option)
}

func ReadCsv(r io.Reader, option ReadCsvOption) (*DataFrame[any], error) {
	reader := csv.NewReader(r)
	if option.Separator == 0 {
		reader.Comma = ','
	} else {
		reader.Comma = option.Separator
	}

	reader.LazyQuotes = !option.AlwaysQuotes
	reader.TrimLeadingSpace = option.TrimLeadingSpace

	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		// empty csv file
		return NewDataFrame[any](), nil
	}

	data := make([][]string, len(records[0]))
	for _, row := range records {
		for ncol, val := range row {
			data[ncol] = append(data[ncol], val)
		}
	}
	return ReadSlice(data, !option.NoHeader), nil
}

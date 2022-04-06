package pandat

import (
	"encoding/csv"
	"io"
)

type CSVOptions struct {
	HasNoHeader      bool
	AlwaysQuotes     bool
	TrimLeadingSpace bool
	Separator        rune
}

func ReadCSV(r io.Reader, option CSVOptions) (*DataFrame[any], error) {
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

	data := make([][]string, len(records[0]))
	for _, row := range records {
		for ncol, val := range row {
			data[ncol] = append(data[ncol], val)
		}
	}
	return ReadSlice(data, !option.HasNoHeader), nil
}

package pandat

import (
	"github.com/xuri/excelize/v2"
	"io"
	"os"
)

type ReadXlsxOption struct {
	NoHeader     bool
	Sheet        string
	SheetIndex   int
	Password     string
	RawCellValue bool
}

func ReadXlsxPath(filepath string, option ReadXlsxOption) (*DataFrame[any], error) {
	r, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	return ReadXlsx(r, option)
}

func ReadXlsx(r io.Reader, option ReadXlsxOption) (*DataFrame[any], error) {
	f, err := excelize.OpenReader(r, excelize.Options{
		Password:     option.Password,
		RawCellValue: option.RawCellValue,
	})
	if err != nil {
		return nil, err
	}

	sheets := f.GetSheetList()

	if len(sheets) == 0 {
		// empty excel
		return NewDataFrame[any](), nil
	}

	var sheet string
	if option.Sheet != "" {
		sheet = option.Sheet
	} else {
		sheet = sheets[option.SheetIndex]
	}

	records, err := f.GetRows(sheet)
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		// empty sheet
		return NewDataFrame[any](), nil
	}

	data := make([][]string, len(records[0]))
	for _, row := range records {
		for ncol, val := range row {
			data[ncol] = append(data[ncol], val)
		}
	}
	return ReadSlice(data, true), nil
}

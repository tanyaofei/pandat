# pandat

A library like python pandas for golang base on 1.18. Provide DataFrame and Series. Supports read from xlsx, csv,
parquet and export to xlsx, csv, parquet.

## Generate DataFrame

```go


package main

import (
	"fmt"
	"github.com/tanyaofei/pandat"
)

func main() {

	// Create dataframe
	df := pandat.NewDataFrame(
		pandat.NewSeries("a", 1, 2, 3, 4, 5),
		pandat.NewSeries("b", 2, 3, 4, 5, 6),
	)
	fmt.Println(df)

	// read from csv
	dfFromCSV, _ := pandat.ReadCsvPath("example.csv", pandat.ReadCsvOption{})
	fmt.Println(dfFromCSV)

	// read from xlsx
	dfFromXlsx, _ := pandat.ReadXlsxPath("1.xlsx", pandat.ReadXlsxOption{})
	fmt.Println(dfFromXlsx)

	// read from parquet
	dfFromParquet, _ := pandat.ReadParquetPath("1.parquet")
    fmt.Println(dfFromParquet)
}
```

## Export DataFrame

```go
package main

import "github.com/tanyaofei/pandat"

func main() {
	df := pandat.NewDataFrame(
		pandat.NewSeries("a", 1, 2, 3, 4, 5),
		pandat.NewSeries("b", 2, 3, 4, 5, 6),
	)
	df.ToParquetPath("1.parquet")
	df.ToCsvPath("1.csv", pandat.WriteCSVOption{})
	df.ToXlsxPath("1.xlsx", pandat.WriteXlsxOption{})
}
```

## Futures

1. Supports sav, zsav
2. More stats
3. formatted printing for dataframe
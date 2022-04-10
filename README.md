# pandat

A library like python pandas for golang base on 1.18. Provide DataFrame and Series. Supports read from xlsx, csv, parquet and export to xlsx, csv, parquet.



## Generate DataFrame

```go
func main() {
  
  // Create dataframe
	df := pandata.NewDataFrame(
		NewSeries("a", 1, 2, 3, 4, 5),
    NewSeries("b", 2, 3, 4, 5, 6),
	)
  fmt.Println(df)
  
  // read from csv
  dfFromCSV, _ := pandata.ReadCSV("example.csv", pandata.ReadCsvOption{})
  fmt.Println(dfFromCSV)
  
  // read from xlsx
  dfFromXlsx, _ := pandata.ReadXlsx("1.xlsx", pandata.ReadXlsxOption{})
  
  // read from parquet

  fmt.Println(dfFromXlsx)
}
```



## Export DataFrame

```go
func main() {
  df := pandata.NewDataFrame(
		NewSeries("a", 1, 2, 3, 4, 5),
    NewSeries("b", 2, 3, 4, 5, 6),
	)
  df.ToParquet("1.parquet")
  df.ToCsv("1.csv", pandata.WriteCsvOption{})
  df.ToXlsx("1.xlsx", pandata.WriteXlsxOption{})
}
```



## Futures

1. Supports sav, zsav
2. More stats
3. formated printing for dataframe
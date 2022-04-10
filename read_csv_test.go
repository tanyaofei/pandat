package pandat

import (
	"fmt"
	"os"
	"testing"
)

func TestReadCSV(t *testing.T) {
	f, err := os.Open("/Users/tanyaofei/Desktop/测试数据/1.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	df, err := ReadCsv(f, ReadCsvOption{})
	if err != nil {
		panic(err)
	}
	fmt.Println(df)

	out, err := os.Create("1.csv")
	err = df.ToCsv(out)
	if err != nil {
		panic(err)
	}
}

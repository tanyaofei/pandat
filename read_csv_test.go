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

	df, err := ReadCSV(f, CSVOptions{})
	if err != nil {
		panic(err)
	}
	fmt.Println(df)

	out, err := os.Create("1.csv")
	err = df.ToCSV(out)
	if err != nil {
		panic(err)
	}
}

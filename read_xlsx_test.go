package pandat

import (
	"fmt"
	"os"
	"testing"
)

func TestReadExcel(t *testing.T) {
	f, err := os.Open("/Users/tanyaofei/Desktop/测试数据/1.xlsx")
	if err != nil {
		panic(err)
	}
	df, err := ReadXlsx(f, ReadXlsxOption{})
	if err != nil {
		panic(err)
	}
	fmt.Println(df)
	fmt.Println(df.DTypes())
}

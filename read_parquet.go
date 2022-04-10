package pandat

import (
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/source"
	"runtime"
)

func ReadParquetPath(filepath string) (*DataFrame[any], error) {
	f, err := local.NewLocalFileReader(filepath)
	if err != nil {
		return nil, err
	}
	return ReadParquet(f)
}

func ReadParquet(f source.ParquetFile) (*DataFrame[any], error) {
	pr, err := reader.NewParquetReader(f, nil, int64(runtime.NumCPU()))
	if err != nil {
		return nil, err
	}

	defer pr.ReadStop()

	names := parquetColumnNames(pr.SchemaHandler)
	seriess := make([]*Series[any], 0, len(names))
	for i := 0; i < len(names); i++ {
		values, _, _, err := pr.ReadColumnByIndex(int64(i), pr.GetNumRows())
		if err != nil {
			return nil, err
		}

		seriess = append(seriess, NewSeries(names[i], values...))
	}
	return NewDataFrame(seriess...), nil
}

func parquetColumnNames(schema *schema.SchemaHandler) []string {
	names := make([]string, 0, len(schema.SchemaElements)-1)
	for i, tag := range schema.Infos {
		if i == 0 {
			continue
		}
		names = append(names, tag.ExName)
	}
	return names
}

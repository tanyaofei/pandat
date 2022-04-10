package pandat

import (
	"encoding/csv"
	"fmt"
	dynamicstruct "github.com/ompluscator/dynamic-struct"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/writer"
	"github.com/xuri/excelize/v2"
	"io"
	"os"
	"reflect"
	"runtime"
	"strconv"
)

type WriteCSVOption struct {
	Comma   rune
	UseCRLF bool
}
type WriteXlsxOption struct {
	Sheet string
}

func (d *DataFrame[E]) ToCsvPath(filepath string, option WriteCSVOption) error {
	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	return d.ToCsv(f, option)
}

func (d *DataFrame[E]) ToCsv(f io.Writer, option WriteCSVOption) error {
	w := csv.NewWriter(f)
	w.Comma = option.Comma
	w.UseCRLF = option.UseCRLF

	err := w.Write(d.Names())
	if err != nil {
		return err
	}
	for _, series := range d.Transpose().seriess {
		values := make([]string, 0, series.Len())
		for _, val := range series.Slice() {
			values = append(values, fmt.Sprint(val))
		}
		err := w.Write(values)
		if err != nil {
			panic(err)
		}
	}

	w.Flush()
	return nil
}

func (d *DataFrame[E]) ToParquetPath(filepath string) error {
	f, err := os.Create(filepath)
	if err != nil {
		return err
	}

	return d.ToParquet(f)
}

func (d *DataFrame[E]) ToParquet(f io.Writer) error {
	//namer := strings.NewReplacer(
	//    " ", "_",
	//    ",", "",
	//    ";", "",
	//    "{", "",
	//    "}", "",
	//    "(", "",
	//    ")", "",
	//    "=", "",
	//    "\xEF\xBB\xBF", "",
	//)

	schema := dynamicstruct.NewStruct()
	for i, name := range d.Names() {
		fieldName := "C" + strconv.Itoa(i)
		columnName := name
		series := d.seriess[i]
		switch series.DType() {
		case reflect.Int:
			tag := fmt.Sprintf(`parquet:"name=%s, type=INT64, repetitiontype=OPTIONAL"`, columnName)
			schema.AddField(fieldName, (*int)(nil), tag)
		case reflect.Int8:
			tag := fmt.Sprintf(`parquet:"name=%s, type=INT32, repetitiontype=OPTIONAL"`, columnName)
			schema.AddField(fieldName, (*int8)(nil), tag)
		case reflect.Int16:
			tag := fmt.Sprintf(`parquet:"name=%s, type=INT32, repetitiontype=OPTIONAL"`, columnName)
			schema.AddField(fieldName, (*int16)(nil), tag)
		case reflect.Int32:
			tag := fmt.Sprintf(`parquet:"name=%s, type=INT32, repetitiontype=OPTIONAL"`, columnName)
			schema.AddField(fieldName, (*int32)(nil), tag)
		case reflect.Int64:
			tag := fmt.Sprintf(`parquet:"name=%s, type=INT64, repetitiontype=OPTIONAL"`, columnName)
			schema.AddField(fieldName, (*int64)(nil), tag)
		case reflect.Uint:
			tag := fmt.Sprintf(`parquet:"name=%s, type=INT64, repetitiontype=OPTIONAL"`, columnName)
			schema.AddField(fieldName, (*uint)(nil), tag)
		case reflect.Uint8:
			tag := fmt.Sprintf(`parquet:"name=%s, type=INT32, repetitiontype=OPTIONAL"`, columnName)
			schema.AddField(fieldName, (*uint8)(nil), tag)
		case reflect.Uint16:
			tag := fmt.Sprintf(`parquet:"name=%s, type=INT32, repetitiontype=OPTIONAL"`, columnName)
			schema.AddField(fieldName, (*uint16)(nil), tag)
		case reflect.Uint32:
			tag := fmt.Sprintf(`parquet:"name=%s, type=INT64, repetitiontype=OPTIONAL"`, columnName)
			schema.AddField(fieldName, (*uint32)(nil), tag)
		case reflect.Uint64:
			tag := fmt.Sprintf(`parquet:"name=%s, type=INT64, repetitiontype=OPTIONAL"`, columnName)
			schema.AddField(fieldName, (*uint64)(nil), tag)
		case reflect.Float32:
			tag := fmt.Sprintf(`parquet:"name=%s, type=FLOAT, repetitiontype=OPTIONAL"`, columnName)
			schema.AddField(fieldName, (*float32)(nil), tag)
		case reflect.Float64:
			tag := fmt.Sprintf(`parquet:"name=%s, type=DOUBLE, repetitiontype=OPTIONAL"`, columnName)
			schema.AddField(fieldName, (*float64)(nil), tag)
		case reflect.Bool:
			tag := fmt.Sprintf(`parquet:"name=%s, type=BOOL, repetitiontype=OPTIONAL"`, columnName)
			schema.AddField(fieldName, (*bool)(nil), tag)
		default:
			tag := fmt.Sprintf(`parquet:"name=%s, type=UTF-8, encoding=PLAIN_DICTIONARY, repetitiontype=OPTIONAL"`, columnName)
			schema.AddField(fieldName, (*string)(nil), tag)
		}
	}

	class := schema.Build()
	w := writerfile.NewWriterFile(f)
	defer w.Close()

	pw, err := writer.NewParquetWriter(w, class.New(), int64(runtime.NumCPU()))
	if err != nil {
		return err
	}
	for _, row := range d.Transpose().seriess {
		recv := class.New()
		for ncol, val := range row.elements {
			field := reflect.ValueOf(recv).Elem().FieldByName("C" + strconv.Itoa(ncol))
			if !field.IsValid() {
				// should not happen?
				continue
			}
			switch field.Kind() {
			case reflect.String:
				v := fmt.Sprint(val)
				field.Set(reflect.ValueOf(&v))
			default:
				v := reflect.New(reflect.TypeOf(val))
				v.Elem().Set(reflect.ValueOf(val))
				field.Set(v)
			}
		}
		if err := pw.Write(recv); err != nil {
			return err
		}
	}
	if err := pw.WriteStop(); err != nil {
		return err
	}
	return nil
}

func (d *DataFrame[any]) ToXlsxPath(filepath string, option WriteXlsxOption) error {
	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	return d.ToXlsx(f, option)
}

func (d *DataFrame[any]) ToXlsx(f io.Writer, option WriteXlsxOption) error {
	w := excelize.NewFile()

	if option.Sheet == "" {
		option.Sheet = "Sheet1"
		w.NewSheet(option.Sheet)
	} else {
		w.NewSheet(option.Sheet)
	}

	// write header
	header := d.Names()
	err := w.SetSheetRow(option.Sheet, "A1", &header)
	if err != nil {
		return err
	}

	// write data
	for row, values := range d.Values() {
		vals := values.Slice()
		err := w.SetSheetRow(option.Sheet, "A"+strconv.Itoa(row+2), &vals)
		if err != nil {
			return err
		}
	}

	err = w.Write(f)
	return err
}

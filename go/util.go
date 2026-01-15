package main

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Assumes the records contain the affected rows and prints the affected rows.
func PrintAffectedRows(records []arrow.Record) {
	if len(records) == 0 {
		panic("Unexpected empty records")
	}
	defer releaseRecords(records)

	// By Datalayers' design, the affected rows is the value at the first row and the first column.
	affectedRows := records[0].Column(0).(*array.Int64).Value(0)
	fmt.Println("Affected rows: ", affectedRows)
}

// Helper function to print records as a table
func PrintRecords(records []arrow.Record) {
	if len(records) == 0 {
		return
	}
	defer releaseRecords(records)

	// Creates a tabwriter to format output into a table
	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.AlignRight)

	// Gets schema and prints column headers
	schema := records[0].Schema()
	for _, field := range schema.Fields() {
		fmt.Fprintf(writer, "%s\t", field.Name)
	}
	fmt.Fprintln(writer)

	// Prints rows
	for _, record := range records {
		numRows := int(record.NumRows())
		numCols := int(record.NumCols())
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			for colIndex := 0; colIndex < numCols; colIndex++ {
				switch arr := record.Column(colIndex).(type) {
				case *array.Timestamp:
					fmt.Fprintf(writer, "%v\t", arr.Value(rowIndex).ToTime(arrow.Millisecond).Local())
				case *array.Int8:
					fmt.Fprintf(writer, "%d\t", arr.Value(rowIndex))
				case *array.Int32:
					fmt.Fprintf(writer, "%d\t", arr.Value(rowIndex))
				case *array.Float32:
					fmt.Fprintf(writer, "%.2f\t", arr.Value(rowIndex))
				default:
					panic(fmt.Sprintf("Unexpected array type: %v", arr.DataType()))
				}
			}
			fmt.Fprintln(writer)
		}
	}
	writer.Flush()
}

func MakeInsertBinding() arrow.Record {
	// Sets the timezone to UTC+8.
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		panic(fmt.Sprintf("Failed to load location: %v", err))
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "Asia/Shanghai"}, Nullable: false},
		{Name: "sid", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "flag", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
	}, nil)

	memAllocator := memory.NewGoAllocator()
	tsBuilder := array.NewTimestampBuilder(memAllocator, &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "Asia/Shanghai"})
	sidBuilder := array.NewInt32Builder(memAllocator)
	valueBuilder := array.NewFloat32Builder(memAllocator)
	flagBuilder := array.NewInt8Builder(memAllocator)

	tsData := []time.Time{
		time.Date(2024, 9, 2, 10, 0, 0, 0, loc),
		time.Date(2024, 9, 2, 10, 5, 0, 0, loc),
		time.Date(2024, 9, 2, 10, 10, 0, 0, loc),
		time.Date(2024, 9, 2, 10, 15, 0, 0, loc),
		time.Date(2024, 9, 2, 10, 20, 0, 0, loc),
	}
	sidData := []int32{1, 2, 3, 4, 5}
	valueData := []float32{12.5, 15.3, 9.8, 22.1, 30.0}
	flagData := []int8{0, 1, 0, 1, 0}

	for _, ts := range tsData {
		tsBuilder.AppendTime(ts)
	}
	valid := []bool{true, true, true, true, true}
	sidBuilder.AppendValues(sidData, valid)
	valueBuilder.AppendValues(valueData, valid)
	flagBuilder.AppendValues(flagData, valid)

	tsArray := tsBuilder.NewArray()
	sidArray := sidBuilder.NewArray()
	valueArray := valueBuilder.NewArray()
	flagArray := flagBuilder.NewArray()
	record := array.NewRecord(schema, []arrow.Array{tsArray, sidArray, valueArray, flagArray}, int64(len(tsData)))

	tsBuilder.Release()
	sidBuilder.Release()
	valueBuilder.Release()
	flagBuilder.Release()

	return record
}

func MakeQueryBinding(sid int32) arrow.Record {
	sidBuilder := array.NewInt32Builder(memory.NewGoAllocator())
	defer sidBuilder.Release()

	sidBuilder.Append(sid)
	sidArray := sidBuilder.NewArray()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "sid", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	record := array.NewRecord(schema, []arrow.Array{sidArray}, 1)
	return record
}

func MakeMultiBinding(sids []int32) arrow.Record {
	sidBuilder := array.NewInt32Builder(memory.NewGoAllocator())
	defer sidBuilder.Release()
	sidBuilder.Append(1)
	sidArray1 := sidBuilder.NewArray()

	sidBuilder = array.NewInt32Builder(memory.NewGoAllocator())
	defer sidBuilder.Release()
	sidBuilder.Append(2)
	sidArray2 := sidBuilder.NewArray()

	sidBuilder = array.NewInt32Builder(memory.NewGoAllocator())
	defer sidBuilder.Release()
	sidBuilder.Append(3)
	sidArray3 := sidBuilder.NewArray()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "sid", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "sid", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "sid", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	record := array.NewRecord(schema, []arrow.Array{sidArray1, sidArray2, sidArray3}, 1)
	return record
}

func releaseRecords(records []arrow.Record) {
	for _, record := range records {
		record.Release()
	}
}

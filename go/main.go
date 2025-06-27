package main

import (
	"fmt"
	"os"
)

func main() {
	// Read the TLS certificate path from the environment variable.
	var tlsCert *string = nil
	path := os.Getenv("TLS_CERT")
	if path != "" {
		tlsCert = &path
	}

	// Creates a client for executing SQLs on the Datalayers server.
	config := &ClientConfig{
		Host:     "127.0.0.1",
		Port:     8360,
		Username: "admin",
		Password: "public",
		TlsCert:  tlsCert,
	}
	client, err := MakeClient(config)
	if err != nil {
		fmt.Println("Failed to create client: ", err)
		return
	}

	// Creates a database `go`.
	sql := "CREATE DATABASE go;"
	result, err := client.Execute(sql)
	if err != nil {
		fmt.Println("Failed to create database: ", err)
		return
	}
	// The result should be:
	// Affected rows: 0
	PrintAffectedRows(result)

	// Optional: sets the database header for each outgoing request to `go`.
	// The Datalayers server uses this header to identify the associated table of a request.
	// This setting is optional since the following SQLs contain the database context
	// and the server could parse the database context from SQLs.
	client.UseDatabase("go")

	// Creates a table `demo` within the database `go`.
	sql = `
    CREATE TABLE go.demo (
        ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        sid INT32,
        value REAL,
        flag INT8,
        timestamp key(ts)
    )
    PARTITION BY HASH(sid) PARTITIONS 8
    ENGINE=TimeSeries;`
	result, err = client.Execute(sql)
	if err != nil {
		fmt.Println("Failed to create table: ", err)
		return
	}
	// The result should be:
	// Affected rows: 0
	PrintAffectedRows(result)

	// Inserts some data into the `demo` table.
	sql = `
        INSERT INTO go.demo (ts, sid, value, flag) VALUES
            ('2024-09-01T10:00:00+08:00', 1, 12.5, 0),
            ('2024-09-01T10:05:00+08:00', 2, 15.3, 1),
            ('2024-09-01T10:10:00+08:00', 3, 9.8, 0),
            ('2024-09-01T10:15:00+08:00', 4, 22.1, 1),
            ('2024-09-01T10:20:00+08:00', 5, 30.0, 0);`
	result, err = client.Execute(sql)
	if err != nil {
		fmt.Println("Failed to insert data: ", err)
		return
	}
	// The result should be:
	// Affected rows: 5
	PrintAffectedRows(result)

	// Queries the inserted data.
	sql = "SELECT * FROM go.demo"
	result, err = client.Execute(sql)
	if err != nil {
		fmt.Println("Failed to scan data: ", err)
		return
	}
	// The result should be:
	//                               ts   sid   value   flag
	//    2024-09-01 10:15:00 +0800 CST     4   22.10      1
	//    2024-09-01 10:00:00 +0800 CST     1   12.50      0
	//    2024-09-01 10:05:00 +0800 CST     2   15.30      1
	//    2024-09-01 10:10:00 +0800 CST     3    9.80      0
	//    2024-09-01 10:20:00 +0800 CST     5   30.00      0
	PrintRecords(result)

	// Inserts some data into the `demo` table with prepared statement.
	sql = "INSERT INTO go.demo (ts, sid, value, flag) VALUES (?, ?, ?, ?);"
	preparedStmt, err := client.Prepare(sql)
	if err != nil {
		fmt.Println("Failed to create a insert prepared statement: ", err)
		return
	}
	binding := MakeInsertBinding()
	result, err = client.ExecutePrepared(preparedStmt, binding)
	if err != nil {
		fmt.Println("Failed to execute a insert prepared statement: ", err)
		return
	}
	// The result should be:
	// Affected rows: 5
	PrintAffectedRows(result)

	// Queries the inserted data with prepared statement.
	sql = "SELECT * FROM go.demo WHERE sid = ?"
	preparedStmt, err = client.Prepare(sql)
	if err != nil {
		fmt.Println("Failed to create a select prepared statement: ", err)
		return
	}

	// Retrieves all rows with `sid` = 1.
	binding = MakeQueryBinding(1)
	result, err = client.ExecutePrepared(preparedStmt, binding)
	if err != nil {
		fmt.Println("Failed to execute a select prepared statement: ", err)
		return
	}
	// The result should be:
	//                               ts   sid   value   flag
	//    2024-09-01 10:00:00 +0800 CST     1   12.50      0
	//    2024-09-02 10:00:00 +0800 CST     1   12.50      0
	PrintRecords(result)

	// Retrieves all rows with `sid` = 1.
	binding = MakeQueryBinding(2)
	result, err = client.ExecutePrepared(preparedStmt, binding)
	if err != nil {
		fmt.Println("Failed to execute a select prepared statement: ", err)
		return
	}
	// The result should be:
	//                               ts   sid   value   flag
	//    2024-09-01 10:05:00 +0800 CST     2   15.30      1
	//    2024-09-02 10:05:00 +0800 CST     2   15.30      1
	PrintRecords(result)

	// Closes the prepared statement to notify releasing resources on server side.
	if err = client.ClosePrepared(preparedStmt); err != nil {
		fmt.Println("Failed to close a prepared statement: ", err)
		return
	}

	// There provides a dedicated interface `execute_update` for executing DMLs, including Insert, Delete.
	// This interface directly returns the affected rows which might be convenient for some use cases.
	//
	// Note, Datalayers does not support Update and the development for Delete is in progress.
	sql = `
        INSERT INTO go.demo (ts, sid, value, flag) VALUES
            ('2024-09-03T10:00:00+08:00', 1, 4.5, 0),
            ('2024-09-03T10:05:00+08:00', 2, 11.6, 1);`
	affectedRows, err := client.ExecuteUpdate(sql)
	if err != nil {
		fmt.Println("Failed to insert data: ", err)
		return
	}
	// The output should be:
	// Affected rows: 2
	fmt.Println("Affected rows: ", affectedRows)

	// Checks that the data are inserted successfully.
	sql = "SELECT * FROM go.demo where ts >= '2024-09-03T10:00:00+08:00'"
	result, err = client.Execute(sql)
	if err != nil {
		fmt.Println("Failed to scan data: ", err)
		return
	}
	// The result should be:
	// 	                              ts   sid   value   flag
	//    2024-09-03 10:00:00 +0800 CST     1    4.50      0
	//    2024-09-03 10:05:00 +0800 CST     2   11.60      1
	PrintRecords(result)

	fmt.Println("\nFinished, thank you!")
}

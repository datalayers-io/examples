package main

import (
	"fmt"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

/// Bad cases:
/// - Could not use binding on some types, such as INT8
///   db.Exec("INSERT INTO sx1 (sid, value, flag, name) VALUES (?, ?, ?, ?)", 3, 32.5, 10, "root")
/// - Could not use First because gorm will append LIMIT $1 to the SQL
///   var dat Sx1Table
///   result = db.First(&dat, "sid > 1")
func main() {
	fmt.Println("Test postgresql over gorm")

	// Connect to database
	dsn := "host=127.0.0.1 port=5432 user=admin password=public dbname=pg_test sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		fmt.Println("Failed to connect database: ", err)
		return
	}

	// Create a database
	sql := "CREATE DATABASE IF NOT EXISTS pg_test"
	result := db.Exec(sql)
	if result.Error != nil {
		fmt.Println("Failed to create database: ", result.Error)
		return
	}

	// Create a table
	sql = `CREATE TABLE IF NOT EXISTS sx1 (
		ts TIMESTAMP(9) NOT NULL DEFAULT CURRENT_TIMESTAMP,
		sid INT32 NOT NULL,
		value REAL,
		flag INT8,
		name STRING,
		timestamp key(ts)
	)
	PARTITION BY HASH (sid) PARTITIONS 1
	ENGINE=TimeSeries
	with (
		TTL='10d',
		update_mode=overwrite
	);`
	result = db.Exec(sql)
	if result.Error != nil {
		fmt.Println("Failed to create table: ", result.Error)
		return
	}

	// Insert data
	// Insert without binding
	result = db.Exec("INSERT INTO sx1 (sid, value, flag, name) VALUES(1, 10.2, 10, 'test')")
	if result.Error != nil {
		fmt.Println("Failed to insert data: ", result.Error)
		return
	}
	// Insert with binding
	result = db.Exec("INSERT INTO sx1 (sid, value, flag, name) VALUES (?, ?, 10, ?)", 2, 21.3, "root")
	if result.Error != nil {
		fmt.Println("Failed to insert data: ", result.Error)
		return
	}
	result = db.Exec("INSERT INTO sx1 (sid, value, flag, name) VALUES (?, ?, 20, ?)", 3, 32.5, "admin")
	if result.Error != nil {
		fmt.Println("Failed to insert data: ", result.Error)
		return
	}

	// Query data
	// Query without data
	result = db.Exec("SELECT * FROM sx1")
	if result.Error != nil {
		fmt.Println("Failed to query data: ", result.Error)
		return
	}
	fmt.Println("Affected rows: ", result.RowsAffected)

	// Query with scan
	var dats []Sx1Table
	result = db.Raw("SELECT * FROM sx1 WHERE sid > ?", 2).Scan(&dats)
	if result.Error != nil {
		fmt.Println("Failed to query data: ", result.Error)
		return
	}
	fmt.Println("Affected rows: ", len(dats))

	// Query with Where
	result = db.Where("sid > ?", 1).Find(&dats)
	if result.Error != nil {
		fmt.Println("Failed to query data: ", result.Error)
		return
	}
	fmt.Println("Affected rows: ", len(dats))

	// Query with Find
	result = db.Find(&dats, "sid = 1")
	if result.Error != nil {
		fmt.Println("Failed to query data: ", result.Error)
		return
	}
	fmt.Println("Affected rows: ", len(dats))

	// Delete data, table must set update_mode=overwrite
	sql = "DELETE FROM sx1 WHERE sid = 1"
  	result = db.Exec("DELETE FROM sx1 WHERE sid = ?", 2)
  	if result.Error != nil {
  		fmt.Println("Failed to delete data: ", result.Error)
  		return
  	}
	fmt.Println("Affected rows: ", result.RowsAffected)

	// Drop table
	sql = "DROP TABLE sx1"
	result = db.Exec(sql)
	if result.Error != nil {
		fmt.Println("Failed to drop table: ", result.Error)
		return
	}
}

type Sx1Table struct {
  Ts time.Time
  Sid int32
  Value float32
  Flag int8
  Name string
}

func (Sx1Table) TableName() string {
  return "sx1"
}

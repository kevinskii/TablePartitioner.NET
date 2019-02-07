# TablePartitioner.NET
## Overview
Provides a set of .NET utilities for writing data in a Apache Hive-style partitioned table format.

## Description
Consider a multi-terabyte table that contains the following rows:

gender | country | age | weightLBS
--- | --- | --- | ---
male | US | 37 | 172
male | CN | 25 | 196
female | JPN | 49 | 118
... | ... | ... | ...

If this table is stored in a single CSV file and analyzed using a tool like Apache Hive or Spark, the entire file will need to be processed every time a query is run. To avoid this, Hive and Spark (and others) support a partitioning scheme that allows the table to be split on fields that contain enumerated values.

For example, splitting the table on the "gender" and "country" fields will result in the following directory structure:

>
	path
	└── to
	    └── table
	        ├── gender=male
	        │   ├── ...
	        │   │
	        │   ├── country=US
	        │   │   └── data.csv
	        │   ├── country=CN
	        │   │   └── data.csv
	        │   └── ...
	        └── gender=female
	            ├── ...
	            ├── country=JPN
	            │   └── data.csv
	            └── ...

Where each data.csv file only contains the remaining "age" and "weightLBS" fields. This allows a query like 
```SELECT * from SomeTable WHERE gender='male' and country='US';``` to run on only the CSV file(s) that are in the corresponding partitioned directories.

(Please see the Apache Spark Parquet documentation at http://spark.apache.org/docs/latest/sql-programming-guide.html#parquet-files for more details.)

## Building
Open the TablePartitioner.sln file in Visual Studio 2015 or later and build the main project and unit tests.

## How To Use
To write partitioned tables, the entire schema doesn't need to be known ahead of time. Only the partitioning columns need to be defined, and optionally a set of "key" columns. The key columns are written on the left-most side, while all other columns are written in alphabetical order. The columns in each row can vary as long as the partition and key columns are present.

The writer saves memory by run-length encoding each column internally. The Flush() function should be called every *n* rows to write the accumulated data to disk and clear memory. The size of *n* depends largely on the amount of RAM on the system, the amount of variation across rows, and whether or not parallel writer processes are running.

## Example

	using PartitionedTableWriter.Impl;
	using PartitionedTableWriter.Interfaces;

	// Create the writer
	string[] keyColumns = { "ID" };
	string[] partitionColumns = { "gender", "country" };
	var writer = PartitionedTableWriterFactory.CreateWriter(@"\\SERVER\Path", "TableName", partitionColumns, keyColumns);
	
	// Add some rows
	writer.AddRow(new Dictionary<string,string>() {
		{ "ID", "12345" },
		{ "gender", "male" },
		{ "country", "US" },
		{ "age", "37" }
	});
	writer.AddRow(new Dictionary<string,string>() {
		{ "ID", "67890" },
		{ "gender", "female" },
		{ "country", "CN" },
		{ "favorite_color", "red" },
		{ "marital_status", "married" },
	});
	// ...

	// Save the results to disk
	writer.Flush();

## Other Notes
The PartitionedTableCSVWriter class doesn't gaurantee that the CSV output is properly formed. For example, a field value that contains commas will simply have them replaced by spaces, whereas a "proper" CSV writer would keep the value as-is and surround it by quotes.

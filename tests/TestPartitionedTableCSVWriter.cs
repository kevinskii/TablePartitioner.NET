using System;
using System.IO;
using System.IO.Compression;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Diagnostics;
using PartitionedTableWriter.Impl;

namespace PartitionedTableWriter_UnitTests {
    [TestClass]
    public class TestPartitionedTableCSVWriter {
        private const string _rootPath = @"C:\temp\ConvertSessRecDataToHDFSFormat_UnitTests";

        [TestInitialize()]
        public void Init() {
            if (Directory.Exists(_rootPath)) {
                Directory.Delete(_rootPath, true);
                Directory.CreateDirectory(_rootPath);
            }
        }

        [TestCleanup()]
        public void Cleanup() {
            if (Directory.Exists(_rootPath)) {
                Directory.Delete(_rootPath, true);
            }
        }

        [TestMethod]
        public void TestExceptionThrownOnInvalidSchema() {
            // Partitioning columns and key columns must be mutually exclusive.
            try {
                var tableWriter = new PartitionedTableCSVWriter(_rootPath, "TableName", new string[] { "Col1", "Col2", "Col3" }, new string[] { "Key1", "Col3", "Key2"});
                Assert.Fail("Exception was not thrown when initializing with an incorrect schema.");
            }
            catch(ApplicationException) { }
        }

        [TestMethod]
        public void TestExceptionThrownOnMissingKeyValue() {
            try {
                var tableWriter = new PartitionedTableCSVWriter(_rootPath, "TableName", 
                    new string[] { "Partition1", "Partition2" }, new string[] { "Key1", "Key2" });

                // Try adding a row with no value for "Key2"
                tableWriter.AddRow(new Dictionary<string, string>() {
                    { "Partition1", "V1" },
                    { "Partition2", "V2" },
                    { "Key1", "V1" },
                    { "SomeOtherColumn", "V1" },
                });
                Assert.Fail("Exception was not thrown when adding a row with a missing key.");
            }
            catch (Exception) { }
        }


        [TestMethod]
        public void TestExceptionThrownOnMissingPartitionValue() {
            try {
                var tableWriter = new PartitionedTableCSVWriter(_rootPath, "TableName",
                    new string[] { "Partition1", "Partition2" }, new string[] { "Key1", "Key2" });

                // Try adding a row with no value for "Partition1"
                tableWriter.AddRow(new Dictionary<string, string>() {
                    { "SomeColumn", "V1" },
                    { "Partition2", "V2" },
                    { "Key1", "V1" },
                    { "Key2", "V1" },
                });
                Assert.Fail("Exception was not thrown when adding a row with a missing partition.");
            }
            catch (Exception) { }
        }

        [TestMethod]
        public void TestMultiplePartitions() {
            var tablePath = Path.Combine(_rootPath, "Table.table");
            const string INPUT =
                   "TYPE,FEATURE,MODEL,TIMESTAMP,VALUE,UNITS\n" +
                   "Diag,Feature1,3231,t0,DDD,N/A\n" +
                   "Diag,Feature1,3231,t1,DDD,N/A\n" +
                   "Diag,Feature2,3231,t1,123,ms\n" +
                   "Clinical,Feature2,2110,t4,456,ms\n" +
                   "Clinical,Feature3,3231-40,t5,456.78,s\n" +
                   "Clinical,Feature3,3231-40,t6,456.78,s\n";
            // The above input should be partitioned into subdirectories per the following (directory, output):
            var expectedOutput = new Dictionary<string, string>() {
                {
                    Path.Combine(tablePath, "TYPE=Diag", "FEATURE=Feature1"),
                    "TIMESTAMP,MODEL,UNITS,VALUE\n" +
                    "t0,3231,N/A,DDD\n" +
                    "t1,3231,N/A,DDD\n"
                },
                {
                    Path.Combine(tablePath, "TYPE=Diag", "FEATURE=Feature2"),
                    "TIMESTAMP,MODEL,UNITS,VALUE\n" +
                    "t1,3231,ms,123\n"
                },
                {
                    Path.Combine(tablePath, "TYPE=Clinical", "FEATURE=Feature2"),
                    "TIMESTAMP,MODEL,UNITS,VALUE\n" +
                    "t4,2110,ms,456\n"
                },
                {
                    Path.Combine(tablePath, "TYPE=Clinical", "FEATURE=Feature3"),
                    "TIMESTAMP,MODEL,UNITS,VALUE\n" +
                    "t5,3231-40,s,456.78\n" +
                    "t6,3231-40,s,456.78\n"
                },
            };

            var partitionColumns = new string[] { "TYPE", "FEATURE" };
            var keyColumns = new string[] { "TIMESTAMP", "MODEL" };
            var tableWriter = new PartitionedTableCSVWriter(_rootPath, "Table", partitionColumns, keyColumns);
            var rowsToWrite = INPUT.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(row => row.Split(',')).ToArray();
            var columnNames = rowsToWrite[0];

            // Write all of the rows (excluding the header) to the table writer then flush it
            for (int i = 1; i < rowsToWrite.Length; ++i) {
                var rowValues = columnNames.Zip(rowsToWrite[i], (name, val) => new { name, val }).ToDictionary(item => item.name, item => item.val);
                tableWriter.AddRow(rowValues);
            }
            tableWriter.Flush();

            // Make sure that the partitioning directories have been created exactly as expected
            HashSet<string> expectedPartitionDirs = new HashSet<string>(
                new string[] {
                    Path.Combine(tablePath, "TYPE=Diag"),
                    Path.Combine(tablePath, "TYPE=Clinical"),
                    Path.Combine(tablePath, "TYPE=Diag", "FEATURE=Feature1"),
                    Path.Combine(tablePath, "TYPE=Diag", "FEATURE=Feature2"),
                    Path.Combine(tablePath, "TYPE=Clinical", "FEATURE=Feature2"),
                    Path.Combine(tablePath, "TYPE=Clinical", "FEATURE=Feature3"),
                });
            HashSet<string> actualPartitionDirs = new HashSet<string>(Directory.GetDirectories(tablePath, "*", SearchOption.AllDirectories));
            Assert.IsTrue(expectedPartitionDirs.SetEquals(actualPartitionDirs), "The partitioning directories were created as expected");

            // Make sure there's a single file in every directory
            foreach (var partitionDir in actualPartitionDirs.Where(dirName => dirName.Contains("FEATURE"))) {
                if (Directory.GetFiles(partitionDir, "*.csv").Length != 1) {
                    Assert.Fail(partitionDir + " is missing a CSV file.");
                }
            }

            // Make sure each files's contents is correct
            foreach(var kvp in expectedOutput) {
                var outputFile = Directory.GetFiles(kvp.Key, "*.csv").First();
                string fileContents = File.ReadAllText(outputFile);
                Assert.AreEqual(kvp.Value, fileContents, "CSV table writer output matches the input.");
            }
        }


        [TestMethod]
        public void TestSinglePartitionNoKeys() {
             const string INPUT =
                    "TYPE,FEATURE,MODEL,TIMESTAMP,VALUE,UNITS\n" +
                    "Diag,Feature1,3231,t0,DDD,N/A\n" +
                    "Diag,Feature1,3231,t1,DDD,N/A\n" +
                    "Diag,Feature2,3231,t1,123,ms\n" +
                    "Clinical,Feature2,2110,t4,456,ms\n" +
                    "Clinical,Feature3,3231-40,t5,456.78,s\n" +
                    "Clinical,Feature3,3231-40,t6,456.78,s\n";

            // Same as the input, but with the columns in alphabetical order
            const string EXPECTED_OUTPUT =
                    "FEATURE,MODEL,TIMESTAMP,TYPE,UNITS,VALUE\n" +
                    "Feature1,3231,t0,Diag,N/A,DDD\n" +
                    "Feature1,3231,t1,Diag,N/A,DDD\n" +
                    "Feature2,3231,t1,Diag,ms,123\n" +
                    "Feature2,2110,t4,Clinical,ms,456\n" +
                    "Feature3,3231-40,t5,Clinical,s,456.78\n" +
                    "Feature3,3231-40,t6,Clinical,s,456.78\n";
            var partitionColumns = new string[0];
            var keyColumns = new string[0];
            var tableWriter = new PartitionedTableCSVWriter(_rootPath, "Table", partitionColumns, keyColumns);
            var rowsToWrite = INPUT.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(row => row.Split(',')).ToArray();
            var columnNames = rowsToWrite[0];

            // Write all of the rows (excluding the header) to the table writer then flush it
            for (int i = 1; i < rowsToWrite.Length; ++i) {
                var rowValues = columnNames.Zip(rowsToWrite[i], (name, val) => new { name, val }).ToDictionary(item => item.name, item => item.val);
                tableWriter.AddRow(rowValues);
            }
            tableWriter.Flush();

            // Read back the table and compare to the original
            string csv = File.ReadAllText(Directory.GetFiles(Path.Combine(_rootPath, "Table.table"), "*.csv")[0]);
            Assert.AreEqual(EXPECTED_OUTPUT, csv, "CSV table writer output matches the input.");
        }


        [TestMethod]
        public void TestSinglePartitionWithKeys() {
            const string INPUT =
                   "TYPE,FEATURE,MODEL,TIMESTAMP,VALUE,UNITS\n" +
                   "Diag,Feature1,3231,t0,DDD,N/A\n" +
                   "Diag,Feature1,3231,t1,DDD,N/A\n" +
                   "Diag,Feature2,3231,t1,123,ms\n" +
                   "Clinical,Feature2,2110,t4,456,ms\n" +
                   "Clinical,Feature3,3231-40,t5,456.78,s\n" +
                   "Clinical,Feature3,3231-40,t6,456.78,s\n";

            // Same as the input, but starting with the key columns in the specified order, followed by remaining columns in alphabetical order
            const string EXPECTED_OUTPUT =
                "TIMESTAMP,MODEL,FEATURE,TYPE,UNITS,VALUE\n" +
                "t0,3231,Feature1,Diag,N/A,DDD\n" +
                "t1,3231,Feature1,Diag,N/A,DDD\n" +
                "t1,3231,Feature2,Diag,ms,123\n" +
                "t4,2110,Feature2,Clinical,ms,456\n" +
                "t5,3231-40,Feature3,Clinical,s,456.78\n" +
                "t6,3231-40,Feature3,Clinical,s,456.78\n";
            var partitionColumns = new string[0];
            var keyColumns = new string[] { "TIMESTAMP", "MODEL" };
            var tableWriter = new PartitionedTableCSVWriter(_rootPath, "Table", partitionColumns, keyColumns);
            var rowsToWrite = INPUT.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(row => row.Split(',')).ToArray();
            var columnNames = rowsToWrite[0];

            // Write all of the rows (excluding the header) to the table writer then flush it
            for (int i = 1; i < rowsToWrite.Length; ++i) {
                var rowValues = columnNames.Zip(rowsToWrite[i], (name, val) => new { name, val }).ToDictionary(item => item.name, item => item.val);
                tableWriter.AddRow(rowValues);
            }
            tableWriter.Flush();

            // Read back the table and compare to the original
            string csv = File.ReadAllText(Directory.GetFiles(Path.Combine(_rootPath, "Table.table"), "*.csv")[0]);
            Assert.AreEqual(EXPECTED_OUTPUT, csv, "CSV table writer output matches the input.");
        }


        [TestMethod]
        public void TestSinglePartitionWithDynamicColumns() {
            const string INPUT =
                   "TYPE,FEATURE,MODEL,TIMESTAMP,VALUE,UNITS\n" +
                   "Diag,Feature1,3231,t0,DDD,N/A\n" +
                   "Diag,Feature1,3231,t1,DDD,N/A\n" +
                   "Diag,Feature2,3231,t1,123,ms\n" +
                   "Clinical,Feature2,2110,t4,456,ms\n" +
                   "Clinical,Feature3,3231-40,t5,456.78,s\n" +
                   "Clinical,Feature3,3231-40,t6,456.78,s\n";

            // Same as the input, but starting with the key columns in the specified order, followed by remaining columns in alphabetical order
            const string EXPECTED_OUTPUT =
                "TIMESTAMP,MODEL,FEATURE,TYPE,UNITS,VALUE\n" +
                // First two VALUE should be <null> because this column wasn't added until the 3rd row
                "t0,3231,Feature1,Diag,N/A,\n" +
                "t1,3231,Feature1,Diag,N/A,\n" +
                // Row 3 is complete
                "t1,3231,Feature2,Diag,ms,123\n" +
                // Next two FEATURE should be <null> because it wasn't included in these rows
                "t4,2110,,Clinical,ms,456\n" +
                "t5,3231-40,,Clinical,s,456.78\n" +
                // All non-key columns are missing from the last row
                "t6,3231-40,,,,\n";
            var partitionColumns = new string[0];
            var keyColumns = new string[] { "TIMESTAMP", "MODEL" };
            var tableWriter = new PartitionedTableCSVWriter(_rootPath, "Table", partitionColumns, keyColumns);
            var rowsToWrite = INPUT.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(row => row.Split(',')).ToArray();
            var columnNames = rowsToWrite[0];

            // Write all of the rows (excluding the header) to the table writer then flush it
            for (int i = 1; i < rowsToWrite.Length; ++i) {
                var rowValues = columnNames.Zip(rowsToWrite[i], (name, val) => new { name, val }).ToDictionary(item => item.name, item => item.val);

                // Selcetively remove columns based on which row this is
                switch(i) {
                    case 1:
                    case 2:
                        // Omit VALUE from 1st two rows
                        rowValues.Remove("VALUE");
                        break;
                    case 4:
                    case 5:
                        // Omit VALUE from rows 4 and 5
                        rowValues.Remove("FEATURE");
                        break;
                    case 6:
                        // Omit all non-key values from the last row
                        rowValues.Remove("FEATURE");
                        rowValues.Remove("TYPE");
                        rowValues.Remove("UNITS");
                        rowValues.Remove("VALUE");
                        break;
                    default:
                        break;
                }

                tableWriter.AddRow(rowValues);
            }
            tableWriter.Flush();

            // Read back the table and compare to the original
            string csv = File.ReadAllText(Directory.GetFiles(Path.Combine(_rootPath, "Table.table"), "*.csv")[0]);
            Assert.AreEqual(EXPECTED_OUTPUT, csv, "CSV table writer output matches the input.");
        }

        [TestMethod]
        public void TestMultipleFlushes() {
            const string INPUT =
                   "TYPE,FEATURE,MODEL,TIMESTAMP,VALUE,UNITS\n" +
                   "Diag,Feature1,3231,t0,DDD,N/A\n" +
                   "Diag,Feature1,3231,t1,DDD,N/A\n" +
                   "Diag,Feature2,3231,t1,123,ms\n" +
                   "Clinical,Feature2,2110,t4,456,ms\n" +
                   "Clinical,Feature3,3231-40,t5,456.78,s\n" +
                   "Clinical,Feature3,3231-40,t6,456.78,s\n";

            // Same as the input, but with the columns in alphabetical order
            const string EXPECTED_OUTPUT =
                    "FEATURE,MODEL,TIMESTAMP,TYPE,UNITS,VALUE\n" +
                    "Feature1,3231,t0,Diag,N/A,DDD\n" +
                    "Feature1,3231,t1,Diag,N/A,DDD\n" +
                    "Feature2,3231,t1,Diag,ms,123\n" +
                    "Feature2,2110,t4,Clinical,ms,456\n" +
                    "Feature3,3231-40,t5,Clinical,s,456.78\n" +
                    "Feature3,3231-40,t6,Clinical,s,456.78\n";
            var partitionColumns = new string[0];
            var keyColumns = new string[0];
            var tableWriter = new PartitionedTableCSVWriter(_rootPath, "Table", partitionColumns, keyColumns);
            var rowsToWrite = INPUT.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(row => row.Split(',')).ToArray();
            var columnNames = rowsToWrite[0];

            // Write the first couple of rows (excluding the header) to the table writer then flush it
            int i = 1;
            for (; i < 3; ++i) {
                var rowValues = columnNames.Zip(rowsToWrite[i], (name, val) => new { name, val }).ToDictionary(item => item.name, item => item.val);
                tableWriter.AddRow(rowValues);
            }
            tableWriter.Flush();

            // Write the remaining rows to the table writer then flush it
            for (; i < rowsToWrite.Length; ++i) {
                var rowValues = columnNames.Zip(rowsToWrite[i], (name, val) => new { name, val }).ToDictionary(item => item.name, item => item.val);
                tableWriter.AddRow(rowValues);
            }
            tableWriter.Flush();

            // Display a dialog to allow for manual inspection of results (no easy way to read concatenated gzips
            // using the .NET GZipStream class).
            string[] files = Directory.GetFiles(_rootPath, "*.csv", SearchOption.AllDirectories);
            Assert.IsTrue(files.Length == 1, "Only one gzip file should exist across multiple writes");
            string csv = File.ReadAllText(files[0]);
            Assert.AreEqual(EXPECTED_OUTPUT, csv, "CSV table writer output matches the input.");
        }

        [TestMethod]
        public void TestMultipleFlushesOfDifferentSchemas() {
            const string INPUT =
                   "TYPE,FEATURE,MODEL,TIMESTAMP,VALUE,UNITS\n" +
                   "Diag,Feature1,3231,t0,DDD,N/A\n" +
                   "Diag,Feature1,3231,t1,DDD,N/A\n" +
                   "Diag,Feature2,3231,t1,123,ms\n" +
                   "Clinical,Feature2,2110,t4,456,ms\n" +
                   "Clinical,Feature3,3231-40,t5,456.78,s\n" +
                   "Clinical,Feature3,3231-40,t6,456.78,s\n";

            // The first flush will be missing the VALUE column
            const string EXPECTED_OUTPUT_1 =
                    "FEATURE,MODEL,TIMESTAMP,TYPE,UNITS\n" +
                    "Feature1,3231,t0,Diag,N/A\n" +
                    "Feature1,3231,t1,Diag,N/A\n" +
                    "Feature2,3231,t1,Diag,ms\n";
            const string EXPECTED_OUTPUT_2 =
                    "FEATURE,MODEL,TIMESTAMP,TYPE,UNITS,VALUE\n" +
                    "Feature2,2110,t4,Clinical,ms,456\n" +
                    "Feature3,3231-40,t5,Clinical,s,456.78\n" +
                    "Feature3,3231-40,t6,Clinical,s,456.78\n";


            var partitionColumns = new string[0];
            var keyColumns = new string[0];
            var tableWriter = new PartitionedTableCSVWriter(_rootPath, "Table", partitionColumns, keyColumns);
            var rowsToWrite = INPUT.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(row => row.Split(',')).ToArray();
            var columnNames = rowsToWrite[0];

            // Write the first three rows (excluding the header) to the table writer then flush it
            int i = 1;
            for (; i < 4; ++i) {
                var rowValues = columnNames.Zip(rowsToWrite[i], (name, val) => new { name, val }).ToDictionary(item => item.name, item => item.val);
                rowValues.Remove("VALUE"); // Don't include the VALUE column in the first flush
                tableWriter.AddRow(rowValues);
            }
            tableWriter.Flush();

            // Verify the file output
            string file1 = Directory.GetFiles(_rootPath, "*.csv", SearchOption.AllDirectories).Single();
            string csv = File.ReadAllText(file1);
            Assert.AreEqual(EXPECTED_OUTPUT_1, csv, "File1 was written correctly.");

            // Write the remaining rows to the table writer then flush it
            for (; i < rowsToWrite.Length; ++i) {
                var rowValues = columnNames.Zip(rowsToWrite[i], (name, val) => new { name, val }).ToDictionary(item => item.name, item => item.val);
                tableWriter.AddRow(rowValues);
            }
            tableWriter.Flush();

            // Verify the 2nd file output
            string file2 = Directory.GetFiles(_rootPath, "*.csv", SearchOption.AllDirectories)
                .Where(filename => filename != file1).Single();
            csv = File.ReadAllText(file2);
            Assert.AreEqual(EXPECTED_OUTPUT_2, csv, "File2 was written correctly.");
        }
    }
}

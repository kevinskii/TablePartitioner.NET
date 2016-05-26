using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using PartitionedTableWriter.Interfaces;
using System.Security.Cryptography;

namespace PartitionedTableWriter.Impl {
    public class PartitionedTableCSVWriter : IPartitionedTableWriter {
        #region Members
        private const uint NULL_INDEX = 0;
        private const string NULL_STR = "";
        private const string FILE_EXTENSION = ".csv";

        // Represents a single non-partitioned subtable within the file system directory structure
        private class Partition {
            #region Members
            public class PartitionData {
                public string[] ColumnNames { get; set; }
                public IEnumerable<String[]> Rows { get; set; }
            }
            // Each column is run-length encoded to reduce memory overhead
            private class ColumnSegment {
                public uint DistinctValueIndex { get; set; }
                public int RepeatCount { get; set; }
            }
            private Dictionary<string, LinkedList<ColumnSegment>> _columns = new Dictionary<string, LinkedList<ColumnSegment>>();
            private KeyValuePair<string, LinkedList<ColumnSegment>>[] _sortedColumns = null;
            private readonly string[] _keyNames;
            private int _rowCount = 0;
            #endregion


            #region Public
            // Constuctor
            public Partition(string[] keyNames) {
                _keyNames = keyNames;

            }

            // Adds a row to the table
            public void AddRow(IReadOnlyDictionary<string, uint> values) {
                // Make sure that the row contains all of the required key columns
                if(Array.Exists(_keyNames, key => false == values.ContainsKey(key))) {
                    throw new ApplicationException("The row is missing one or more key columns.");
                }

                // For each column:value pair in the input
                foreach(var kvp in values) {

                    // If we haven't added values for this column yet, create a new object to hold its values
                    LinkedList<ColumnSegment> column = null;
                    if(false == _columns.TryGetValue(kvp.Key, out column)) {
                        column = new LinkedList<ColumnSegment>();

                        // If rows have previously been added to the table, set their entries to NULL in this column
                        if(_rowCount > 0) {
                            column.AddLast(new ColumnSegment { DistinctValueIndex = NULL_INDEX, RepeatCount = _rowCount });
                        }
                        _columns.Add(kvp.Key, column);
                    }

                    // If the current value is different than the previous one, or if it's the first value added, then create a new
                    // column segment to hold it
                    if(0 == column.Count || column.Last.Value.DistinctValueIndex != kvp.Value) {
                        column.AddLast(new ColumnSegment { DistinctValueIndex = kvp.Value, RepeatCount = 1 });
                    }
                    else {
                        column.Last.Value.RepeatCount++; // Else this value is the same as the previous one
                    }
                } // End foreach

                // For each pre-existing column that is not in this new row, add a null entry as a placeholder
                var preExistingColumns = _columns.Where(col => false == values.ContainsKey(col.Key));
                foreach(var kvp in preExistingColumns) {
                    if(NULL_INDEX == kvp.Value.Last.Value.DistinctValueIndex) {
                        kvp.Value.Last.Value.RepeatCount++;
                    }
                    else {
                        kvp.Value.AddLast(new ColumnSegment { DistinctValueIndex = NULL_INDEX, RepeatCount = 1 });
                    }
                }
                _rowCount++;
            }

            // Gets the table column names and rows, removing data from the table as it is returned.
            public PartitionData Flush(string[] distinctValues) {
                // Put the columns in order of { Key0, Key1, ..., KeyN, Col1, Col2, ..., ColN } where the key columns are in the order
                // specified, and the non-key columns follow in alphabetical order
                _sortedColumns = new KeyValuePair<string,LinkedList<ColumnSegment>>[_columns.Count];
                for(int i = 0; i < _keyNames.Length; ++i) {
                    _sortedColumns[i] = new KeyValuePair<string, LinkedList<ColumnSegment>>(_keyNames[i], _columns[_keyNames[i]]);
                    _columns.Remove(_keyNames[i]);
                }
                Array.Copy(_columns.ToArray(), 0, _sortedColumns, _keyNames.Length, _sortedColumns.Length - _keyNames.Length);
                _columns.Clear(); // No longer need
                var sortingFunc = Comparer<KeyValuePair<string, LinkedList<ColumnSegment>>>.Create((x, y) => x.Key.CompareTo(y.Key));
                Array.Sort(_sortedColumns, _keyNames.Length, _sortedColumns.Length - _keyNames.Length, sortingFunc); // Sort non-key columns

                var result = new PartitionData {
                    ColumnNames = _sortedColumns.Select(col => col.Key).ToArray(),
                    Rows = get_rows(distinctValues)
                };
                return result;
            }
            #endregion


            #region private
            // Gets and returns rows from the table, removing them in the process
            private IEnumerable<string[]> get_rows(string[] distinctValues) {
                // While there are still rows left to process
                while (_rowCount > 0) {
                    var currentRow = new string[_sortedColumns.Length];
                    for (int i = 0; i < _sortedColumns.Length; ++i) {
                        var currentSegment = _sortedColumns[i].Value.First;
                        currentRow[i] = distinctValues[currentSegment.Value.DistinctValueIndex];

                        // If we have returned all of the values for this segment 
                        if (0 == --currentSegment.Value.RepeatCount) {
                            _sortedColumns[i].Value.RemoveFirst();
                        }
                    }
                    _rowCount--;
                    yield return currentRow;
                }

                // Sanity check: Make sure all segments have been processed
                if (Array.Exists(_sortedColumns, column => column.Value.Count > 0)) {
                    // Should never happen
                    throw new ApplicationException("Design Error: Unprocessed column segments remain after processing all rows.");
                }

            }
            #endregion
        }
        private Dictionary<string, Partition> _partitions = new Dictionary<string, Partition>();
        private readonly Dictionary<string, int> _partitionOrderingTable = new Dictionary<string, int>();
        private readonly Dictionary<string, uint> _distinctValueIndexes = new Dictionary<string, uint>();
        private readonly string[] _keyNames;
        private readonly string _tablePath;
        #endregion


        #region Public
        public PartitionedTableCSVWriter(string path, string tableName, string[] partitioningColumns, string[] keyColumns) {
            _keyNames = keyColumns;

            // Build tables that we can later use to keep track of which order the partitioning keys should be in.
            Array.ForEach(partitioningColumns, columnName => _partitionOrderingTable.Add(columnName, _partitionOrderingTable.Count));

            // Make sure that the partitioning and key columns are mutually exclusive.
            if (Array.Exists(_keyNames, name => _partitionOrderingTable.ContainsKey(name))) {
                throw new ApplicationException("Partitioning column names and key column names must be mutually exlusive.");
            }

            // The table will get its own directory. Use the name plus a ".table" suffix if the user hasn't already added it.
            tableName = tableName.Replace("/", "_").Replace("\\", "_").Replace(" ", "_");
            _tablePath = Path.Combine(
                path,   
                tableName.EndsWith(".table") ? tableName : tableName + ".table");
            _distinctValueIndexes.Add(NULL_STR, NULL_INDEX);
        }
        

        public void AddRow(IReadOnlyDictionary<string, string> columnNamesAndValues) {
            // Copy the column names and values into an array of partition keys, and dictionary of non-partition KeyValuePairs
            var partitionKeyComponents = new string[_partitionOrderingTable.Count];
            var nonPartitioningColumns = new Dictionary<string, uint>(columnNamesAndValues.Count - _partitionOrderingTable.Count);
            foreach(var kvp in columnNamesAndValues) {
                // If this is a partitioning column, copy its "Name=Value" pair into the array of partitioning keys
                int partitionOrderingNum;
                if(_partitionOrderingTable.TryGetValue(kvp.Key, out partitionOrderingNum)) {
                    var sanitizedValue = kvp.Value.Replace(' ', '_').Replace('/', '_').Replace('\\', '_')
                        .Replace("™", String.Empty).Replace("®", string.Empty);
                    partitionKeyComponents[partitionOrderingNum] = kvp.Key + "=" + sanitizedValue;
                }
                // Else this is a regular column
                else {
                    // If we have not yet added the value to the distinct values table, do so
                    uint distinctValIndex;
                    if(false == _distinctValueIndexes.TryGetValue(kvp.Value, out distinctValIndex)) {
                        _distinctValueIndexes.Add(kvp.Value, distinctValIndex = (uint)_distinctValueIndexes.Count);
                    }
                    nonPartitioningColumns[kvp.Key] = distinctValIndex;
                }
            }

            // Get the subtable for this partition key, creating if necessary
            var partitionKey =
                String.Join(Path.DirectorySeparatorChar.ToString(),
                    partitionKeyComponents.Select(component => component.Replace(Path.DirectorySeparatorChar, '_'))); // Remove any slashes from the path components
            Partition table = null;
            if(false == _partitions.TryGetValue(partitionKey, out table)) {
                _partitions.Add(partitionKey, table = new Partition(_keyNames));
            }
            table.AddRow(nonPartitioningColumns);
        }


        public void Flush() {
            var hasher = MD5.Create();
            var encoder = System.Text.Encoding.UTF8;

            // Convert the distinct values index table into an array for fast access
            var distinctValues = new string[_distinctValueIndexes.Count];
            foreach(var kvp in _distinctValueIndexes) {
                distinctValues[kvp.Value] = kvp.Key.Replace(',','_').Replace('\n', '_'); // Sanitize each value
            }
            _distinctValueIndexes.Clear(); // No longer need
            _distinctValueIndexes[NULL_STR] = NULL_INDEX;

            // For each partitioned subtable
            foreach(var kvp in _partitions) {

                // If the partition path doesn't yet exist, create it
                var subTablePath = Path.Combine(_tablePath, kvp.Key);
                if(false == Directory.Exists(subTablePath)) {
                    Directory.CreateDirectory(subTablePath);
                }

                // Get the data from this subtable and compute a unique hash of the column headers. This hash will be used to
                // differentiate between subtables which may lie on the same partition path, but which have different
                // schemas (i.e. the columns are different). Although a subtable's schema will almost always remain
                // constant across Flushes, we can't assume this because columns are dynamically added.
                var tableData = kvp.Value.Flush(distinctValues);
                var sanitizedColumnNames = tableData.ColumnNames.Select(name => name.Replace(',', '_').Replace('\n', '_').Replace(' ', '_').Replace("\"", ""));
                var headerStr = String.Join(",", sanitizedColumnNames);
                var hashBytes = hasher.ComputeHash(encoder.GetBytes(headerStr));
                var hash = BitConverter.ToString(hashBytes).Replace("-", string.Empty);
                var subTableName = Path.Combine(subTablePath, hash + FILE_EXTENSION);
                bool tableAlreadyExists = File.Exists(subTableName);

                // Write the data to the file system
                using (var fileStream = File.Open(subTableName, FileMode.Append)) {
                    // If the table didn't previously exist, write the header
                    if(false == tableAlreadyExists) {
                        var headerBytes = encoder.GetBytes(headerStr + "\n");
                        fileStream.Write(headerBytes, 0, headerBytes.Length);
                    }
                    // Write each row to the file system
                    foreach (var row in tableData.Rows) {
                        var rowBytes = encoder.GetBytes(String.Join(",", row) + "\n");
                        fileStream.Write(rowBytes, 0, rowBytes.Length);
                    }

                    // Flush and close the streams
                    fileStream.Flush();
                    fileStream.Close();
                }
            } // end foreach subtable

            _partitions.Clear();
        }
        #endregion
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PartitionedTableWriter.Interfaces {
    
    /// <summary>
    /// Describes methods that write data into a partitioned table. A partitioned table is one where some of the columns
    /// have been moved out of the table itself and into the directory structure above it, which reduces the table's size
    /// and can improve query performance.
    /// 
    /// For example, given a table which contains the following:
    /// Model,      Serial, Timestamp,      Value
    /// 3231-36,    12345,  04-Jan-2014,    DDD
    /// 3231-36,    24680,  01-Feb-2014,    DDI
    /// 2110,       54321,  01-Mar-2015,    DDI
    /// 2110,       12345,  07-Jul-2012,    DDD
    /// 
    /// This single block of data can be subdivided into small files in a directory structure of key=value directory names
    /// to reduce the size and improve performance:
    /// /path/to/table
    ///     /Model=3231-36
    ///         /Value=DDD
    ///             Serial, Timestamp
    ///             12345,  04-Jan-2014
    ///         /Value=DDI
    ///             Serial, Timestamp
    ///             24680,  01-Feb-2014
    ///     /Model=2110
    ///         /...
    /// </summary>
    public interface IPartitionedTableWriter {

        /// <summary>
        /// Adds a row to the table. 
        /// </summary>
        void AddRow(IReadOnlyDictionary<string,string> columnNamesAndValues);

        /// <summary>
        /// Writes the accumulated rows to the file system in partitioned table format.
        /// </summary>
        void Flush();
    }
}

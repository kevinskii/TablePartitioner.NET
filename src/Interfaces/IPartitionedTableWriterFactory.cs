using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PartitionedTableWriter.Interfaces {
    /// <summary>
    /// Abstracts the creation of Partitioned table writers to allow for easier unit testing.
    /// </summary>
    public interface IPartitionedTableWriterFactory {
        /// <summary>
        /// Creates an instance of an IPartitionedTableWriter.
        /// </summary>
        /// <param name="rootDir">See IPartitionedTableWriter</param>
        /// <param name="fieldNames">See IPartitionedTableWriter</param>
        /// <returns>See IPartitionedTableWriter</returns>
        IPartitionedTableWriter CreateCSVWriter(string path, string tableName, string[] partitioningColumns, string[] keyColumns);
    }
}

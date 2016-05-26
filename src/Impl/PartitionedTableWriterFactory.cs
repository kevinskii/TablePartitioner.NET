using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PartitionedTableWriter.Interfaces;
namespace PartitionedTableWriter.Impl {
    public class PartitionedTableWriterFactory : IPartitionedTableWriterFactory {
        public IPartitionedTableWriter CreateCSVWriter(string path, string tableName, string[] partitioningColumns, string[] keyColumns) {
            return new PartitionedTableCSVWriter(path, tableName, partitioningColumns, keyColumns);
        }
    }
}

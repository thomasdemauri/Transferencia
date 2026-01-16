using Microsoft.Data.SqlClient;
using System.Data;
using static Server.LineProcessor;

namespace Server
{
    internal class BatchDatabaseProcessor
    {
        private static string GetConnectionString()
            => "Server=localhost;Database=TRANSFER_DB;User Id=sa;Password=admin;Encrypt=True;TrustServerCertificate=True;";

        private const int BATCH_SIZE = 50000;

        public SqlConnection Connection;
        public SqlBulkCopy BulkCopy;

        public BatchDatabaseProcessor()
        {
            Connection = new SqlConnection(GetConnectionString());
            BulkCopy = new SqlBulkCopy(Connection);

            BulkCopy.DestinationTableName = "AndroidLogs";
            BulkCopy.BatchSize = BATCH_SIZE;
            BulkCopy.BulkCopyTimeout = 60;

            BulkCopy.ColumnMappings.Add("LogDate", "LogDate");
            BulkCopy.ColumnMappings.Add("Pid", "Pid");
            BulkCopy.ColumnMappings.Add("Tid", "Tid");
            BulkCopy.ColumnMappings.Add("Level", "Level");
            BulkCopy.ColumnMappings.Add("Component", "Component");
            BulkCopy.ColumnMappings.Add("Content", "Content");

            Connection.Open();
        }

        public Task BulkInsert(Entry[] entries, int entriesIndex)
        {
            var dataTable = ConvertToDataTable(entries, entriesIndex);
            return BulkCopy.WriteToServerAsync(dataTable);
        }

        private static DataTable ConvertToDataTable(Entry[] entries, int entriesIndex)
        {
            var dataTable = CreateTable();

            for (int i = 0; i < entriesIndex; i++)
            {
                dataTable.Rows.Add(entries[i].LogDate, entries[i].Pid, entries[i].Tid, entries[i].Level, entries[i].Component, entries[i].Content);
            }

            return dataTable;
        }

        static DataTable CreateTable()
        {
            var table = new DataTable();

            table.Columns.Add("LogDate", typeof(string));
            table.Columns.Add("Pid", typeof(Int16));
            table.Columns.Add("Tid", typeof(Int16));
            table.Columns.Add("Level", typeof(char));
            table.Columns.Add("Component", typeof(string));
            table.Columns.Add("Content", typeof(string));

            return table;
        }
    }
}

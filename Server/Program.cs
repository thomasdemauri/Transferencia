using Microsoft.Data.SqlClient;
using System.Data;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Server
{
    internal class Program
    {
        public static int BATCH_SIZE = 10_0000;
        const int BUFFER_SIZE = 4_096;
        const int MAX_LINE_BUFFER_SIZE = 1024 * 64;

        static async Task Main(string[] args)
        {
            const int port = 8888;
            var listener = new TcpListener(IPAddress.Any, port);

            Console.WriteLine($"Starting TCP Server on port {port}...");
            listener.Start();

            while (true)
            {
                Console.WriteLine("Waiting for a connection...");
                var client = listener.AcceptTcpClient();
                Console.WriteLine("Client connected!");

                var stream = client.GetStream();
                var linesProcessed = 0;

                using (var connection = new SqlConnection(GetConnectionString()))
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null)
                {
                    DestinationTableName = "AndroidLogs",
                    BatchSize = BATCH_SIZE,
                    BulkCopyTimeout = 60
                })
                {
                    bulkCopy.ColumnMappings.Add("LogDate", "LogDate");
                    bulkCopy.ColumnMappings.Add("Pid", "Pid");
                    bulkCopy.ColumnMappings.Add("Tid", "Tid");
                    bulkCopy.ColumnMappings.Add("Level", "Level");
                    bulkCopy.ColumnMappings.Add("Component", "Component");
                    bulkCopy.ColumnMappings.Add("Content", "Content");
                    await connection.OpenAsync();

                    var currentBuffer = new byte[BUFFER_SIZE];
                    var accumulator = new StringBuilder();
                    var bytesRead = 0;

                    var lineBuffer = new char[MAX_LINE_BUFFER_SIZE];
                    var entries = new Entry[BATCH_SIZE];
                    var entriesIndex = 0;


                    while ((bytesRead = await stream.ReadAsync(currentBuffer)) != 0)
                    {
                        var chunkString = Encoding.UTF8.GetString(currentBuffer, 0, bytesRead);
                        accumulator.Append(chunkString);

                        var cursor = 0;
                        var newLineAt = 0;
                        var iterator = 0;

                        while (iterator < accumulator.Length)
                        {
                            if (accumulator[iterator] == '\n' || accumulator[iterator] == '\r')
                            {
                                newLineAt = iterator;
                                int length = newLineAt - cursor;

                                if (length > 0)
                                {
                                    accumulator.CopyTo(cursor, lineBuffer, 0, length);
                                    var lineSpan = new Span<char>(lineBuffer, 0, length);

                                    if (ProcessMessage(lineSpan) is Entry entry)
                                    {
                                        entries[entriesIndex++] = entry;

                                        if (entriesIndex >= BATCH_SIZE)
                                        {
                                            await bulkCopy.WriteToServerAsync(ConvertToDataTable(entries, entriesIndex));
                                            entriesIndex = 0;
                                        }
                                    }

                                }

                                cursor = newLineAt + 1;
                            }

                            iterator++;
                        }

                        if (cursor > 0)
                        {
                            accumulator.Remove(0, cursor);
                        }

                    }

                    if (accumulator.Length > 0)
                    {

                        accumulator.CopyTo(0, lineBuffer, 0, accumulator.Length);

                        var lineSpan = new Span<char>(lineBuffer, 0, accumulator.Length);

                        if (ProcessMessage(lineSpan) is Entry entry)
                        {
                            entries[entriesIndex++] = entry;
                        }
                    }

                    if (entriesIndex > 0)
                    {
                        await bulkCopy.WriteToServerAsync(ConvertToDataTable(entries, entriesIndex));
                        entriesIndex = 0;
                    }

                    await connection.CloseAsync();


                }

                client.Close();

                Console.WriteLine(linesProcessed);
                Console.WriteLine("Client disconnected.");
            }
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

        static Entry? ProcessMessage(Span<char> message)
        {
            //var watch = System.Diagnostics.Stopwatch.StartNew();

            if (message.IsWhiteSpace()) return null;

            int startIndex = -1;

            for (int i = 0; i < message.Length; i++)
            {
                if (char.IsDigit(message[i]))
                {
                    startIndex = i;
                    break;
                }
            }

            if (startIndex == -1)
            {
                return null;
            }

            if (message.Length < 33)
            {
                return null;
            }

            var cleanMessage = message.Slice(startIndex);

            if (cleanMessage.Length < 33)
            {
                return null;
            }

            var level = message[31];

            var logDate = cleanMessage.Slice(0, 18);
            var pid = cleanMessage.Slice(19, 5).TrimStart();
            var tid = cleanMessage.Slice(25, 5).TrimStart();

            var colonTerminator = new ReadOnlySpan<char>(new char[] { ':' });
            var greaterThanTerminator = new ReadOnlySpan<char>(new char[] { '>' });
            var relativeSpan = cleanMessage.Slice(33);

            var endRelative = relativeSpan.IndexOf(colonTerminator);
            if (endRelative == -1)
            {
                endRelative = relativeSpan.IndexOf(greaterThanTerminator);
            }
            if (endRelative == -1)
            {
                throw new Exception();
            }

            var component = relativeSpan.Slice(0, endRelative);
            var content = relativeSpan.Slice(component.Length + 1);

            //watch.Stop();
            //Console.WriteLine($"Time taken: {watch.ElapsedMilliseconds} miliseconds");

            return new Entry(logDate.ToString(), Int16.Parse(pid), Int16.Parse(tid), level, component.ToString(), content.ToString());
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

        static string GetConnectionString()
            => "Server=localhost;Database=TRANSFER_DB;User Id=sa;Password=admin;Encrypt=True;TrustServerCertificate=True;";
    }

    public struct Entry(string logDate, Int16 pid, Int16 tid, char level, string component, string content)
    {
        public string LogDate { get; } = logDate;
        public Int16 Pid { get; } = pid;
        public Int16 Tid { get; } = tid;
        public char Level { get; } = level;
        public string Component { get; } = component;
        public string Content { get; } = content;
    }
}
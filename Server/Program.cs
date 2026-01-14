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

                using (var reader = new StreamReader(stream, Encoding.UTF8))
                using (var connection = new SqlConnection(GetConnectionString()))
                using (var bulkCopy = new SqlBulkCopy(connection)
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

                    string? line;

                    var entries = new Entry[BATCH_SIZE];
                    var index = 0;

                    while ((line = await reader.ReadLineAsync()) != null)
                    {
                        var entry = ProcessMessage(line);

                        if (entry is null)
                        {
                            continue;
                        }

                        entries[index++] = (Entry)entry;

                        linesProcessed++;

                        if (index == BATCH_SIZE)
                        {
                            using (var dataTable = ConvertToDataTable(entries))
                            {
                                await bulkCopy.WriteToServerAsync(dataTable);
                            }

                            Array.Clear(entries);
                            index = 0;
                        }
                    }

                    if (index > 0)
                    {
                        linesProcessed++;
                        using (var dataTable = ConvertToDataTable(entries))
                        {
                            await bulkCopy.WriteToServerAsync(dataTable);
                        }
                    }

                    await connection.CloseAsync();
                }

                client.Close();

                Console.WriteLine(linesProcessed);
                Console.WriteLine("Client disconnected.");
            }
        }

        private static DataTable ConvertToDataTable(Entry[] entries)
        {
            var dataTable = CreateTable();

            foreach (var entry in entries)
            {
                dataTable.Rows.Add(entry.LogDate, entry.Pid, entry.Tid, entry.Level, entry.Component, entry.Content);
            }

            return dataTable;
        }

        static Entry? ProcessMessage(string message)
        {
            //if (message.Length < 18)
            //{
            //    return null;
            //}

            //var startMessage = false;
            //var index = 0;

            //while (!startMessage && index < message.Length)
            //{
            //    startMessage = Char.IsDigit(message[index]);
            //    index++;
            //}

            //if (startMessage == false)
            //{
            //    return null;
            //}

            if (string.IsNullOrWhiteSpace(message)) return null;

            int startIndex = -1;

            // O loop 'for' garante que 'i' nunca vai estourar o tamanho da mensagem
            for (int i = 0; i < message.Length; i++)
            {
                if (char.IsDigit(message[i]))
                {
                    startIndex = i;
                    break; // Encontramos! Para o loop agora.
                }
            }

            // Se o loop terminou e startIndex continua -1, significa que não tinha nenhum número.
            // Retorna null para ignorar essa linha de lixo.
            if (startIndex == -1)
            {
                return null;
            }

            // Verifica se, a partir do número encontrado, tem tamanho suficiente
            if (message.Length - startIndex < 18)
            {
                return null;
            }

            // Pega a mensagem válida a partir do índice encontrado
            // Não precisa de Trim aqui porque o IsDigit já pulou os espaços e lixos
            //var validPart = message.Substring(startIndex);

            var cleanMessage = message.Substring(startIndex);

            var logDate = cleanMessage.Substring(0, 18);
            var pid = cleanMessage.Substring(19, 5).TrimStart();
            var tid = cleanMessage.Substring(25, 5).TrimStart();
            var level = cleanMessage[31];

            var endRelative = cleanMessage.IndexOf(':', 33);
            if (endRelative == -1)
            {
                endRelative = cleanMessage.IndexOf('>', 33);
            }
            var endAbsolute = endRelative - 33;

            var component = cleanMessage.Substring(33, endAbsolute);
            var content = cleanMessage.Substring(33 + 2 + endAbsolute);

            return new Entry(logDate, Int16.Parse(pid) , Int16.Parse(tid), level, component, content);
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
        public Int16 Tid { get; }  = tid;
        public char Level { get; } = level;
        public string Component { get; } = component;
        public string Content { get; } = content;
    }
}

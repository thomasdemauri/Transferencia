using Microsoft.Data.SqlClient;
using System.Data;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;

namespace Server
{
    internal class Program
    {
        const int BATCH_SIZE = 10_0000;
        const int NETWORK_BUFFER_SIZE = 4_096;
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
                Console.WriteLine("Client connected! Processing 3GB Stream...");

                // Cronômetro para você ver a diferença
                var watch = System.Diagnostics.Stopwatch.StartNew();

                await ProcessClientAsync(client);

                watch.Stop();
                Console.WriteLine($"Processamento finalizado em: {watch.Elapsed}");
            }
        }

        static async Task ProcessClientAsync(TcpClient client)
        {
            var stream = client.GetStream();

            // 1. CRIAR O CANAL (A Esteira entre as threads)
            // Bounded(5) significa: Se tiver 5 lotes (250k linhas) esperando o banco, 
            // a leitura da rede PAUSA para não estourar a memória RAM (Backpressure).
            var channel = Channel.CreateBounded<BatchData>(new BoundedChannelOptions(5)
            {
                SingleReader = true,
                SingleWriter = true
            });

            // 2. INICIAR O CONSUMIDOR (Thread do Banco de Dados)
            // Ele roda em paralelo (Task.Run)
            var dbTask = Task.Run(async () =>
            {
                using (var connection = new SqlConnection(GetConnectionString()))
                {
                    await connection.OpenAsync();

                    // Opção TableLock: MONSTRUOSO ganho de performance para cargas grandes (3GB),
                    // mas trava a tabela inteira enquanto insere. Use se puder.
                    var bulkOptions = SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction;

                    // Loop que fica consumindo da esteira
                    await foreach (var batch in channel.Reader.ReadAllAsync())
                    {
                        using (var bulkCopy = new SqlBulkCopy(connection, bulkOptions, null))
                        {
                            bulkCopy.DestinationTableName = "AndroidLogs";
                            bulkCopy.BatchSize = batch.Count;
                            bulkCopy.BulkCopyTimeout = 0; // Infinito (importante para 3GB)

                            // Mapeamentos (igual ao seu)
                            bulkCopy.ColumnMappings.Add("LogDate", "LogDate");
                            bulkCopy.ColumnMappings.Add("Pid", "Pid");
                            bulkCopy.ColumnMappings.Add("Tid", "Tid");
                            bulkCopy.ColumnMappings.Add("Level", "Level");
                            bulkCopy.ColumnMappings.Add("Component", "Component");
                            bulkCopy.ColumnMappings.Add("Content", "Content");

                            // Grava no banco usando o Count exato deste lote
                            await bulkCopy.WriteToServerAsync(ConvertToDataTable(batch.Entries, batch.Count));
                        }
                    }
                }
            });

            // 3. O PRODUTOR (Thread da Rede/CPU - Seu código antigo)
            // Esta thread só se preocupa em ler e processar o mais rápido possível
            try
            {
                var currentBuffer = new byte[NETWORK_BUFFER_SIZE];
                var accumulator = new StringBuilder();
                var lineBuffer = new char[MAX_LINE_BUFFER_SIZE];

                // Precisamos alocar arrays novos para cada envio ao canal
                var entries = new Entry[BATCH_SIZE];
                var entriesIndex = 0;
                int bytesRead;

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
                                if (length > lineBuffer.Length)
                                    lineBuffer = new char[length + 1024];

                                accumulator.CopyTo(cursor, lineBuffer, 0, length);
                                var lineSpan = new ReadOnlySpan<char>(lineBuffer, 0, length);

                                if (ProcessMessage(lineSpan) is Entry entry)
                                {
                                    entries[entriesIndex++] = entry;

                                    if (entriesIndex >= BATCH_SIZE)
                                    {
                                        // ENVIA PARA A ESTEIRA
                                        // Cria um objeto simples para passar o array e a contagem
                                        await channel.Writer.WriteAsync(new BatchData(entries, entriesIndex));

                                        // ALOCA UM NOVO BALDE LIMPO
                                        // "Mas isso não gasta memória?" 
                                        // Para 3GB (aprox 30M linhas) / 50k lote = 600 alocações. É nada pro GC.
                                        entries = new Entry[BATCH_SIZE];
                                        entriesIndex = 0;
                                    }
                                }
                            }
                            cursor = newLineAt + 1;
                        }
                        iterator++;
                    }

                    if (cursor > 0) accumulator.Remove(0, cursor);
                }

                // FLUSH FINAL (Sobras)
                if (accumulator.Length > 0)
                {
                    accumulator.CopyTo(0, lineBuffer, 0, accumulator.Length);
                    var lineSpan = new ReadOnlySpan<char>(lineBuffer, 0, accumulator.Length);
                    if (ProcessMessage(lineSpan) is Entry entry)
                        entries[entriesIndex++] = entry;
                }

                if (entriesIndex > 0)
                {
                    // Envia o restinho
                    await channel.Writer.WriteAsync(new BatchData(entries, entriesIndex));
                }
            }
            finally
            {
                // AVISA O BANCO QUE ACABOU A REDE
                channel.Writer.Complete();
                client.Close();
            }

            // 4. ESPERA O BANCO TERMINAR DE TRABALHAR
            await dbTask;
            Console.WriteLine("Client disconnected.");
        }
        static Entry? ProcessMessage(ReadOnlySpan<char> fullLineSpan)
        {
            if (fullLineSpan.IsWhiteSpace())
            {
                return null;
            }

            int startIndex = -1;
            for (int i = 0; i < fullLineSpan.Length; i++)
            {
                if (char.IsDigit(fullLineSpan[i]))
                {
                    startIndex = i;
                    break;
                }
            }

            if (startIndex == -1)
            {
                return null;
            }

            var line = fullLineSpan.Slice(startIndex);

            if (line.Length < 33)
            {
                return null;
            }

            try
            {
                var logDate = line.Slice(0, 18).ToString();
                var pidSpan = line.Slice(19, 5).Trim();
                var tidSpan = line.Slice(25, 5).Trim();

                short pid = short.Parse(pidSpan);
                short tid = short.Parse(tidSpan);

                var level = line[31];

                var bodyLine = line.Slice(33);

                var separatorComponent = bodyLine.IndexOf(':');
                if (separatorComponent == -1)
                {
                    separatorComponent = bodyLine.IndexOf('>');
                }
                if (separatorComponent == -1)
                {
                    return null;
                }

                var component = bodyLine.Slice(0, separatorComponent).ToString();
                var content = bodyLine.Slice(separatorComponent + 1).ToString().Trim();


                return new Entry(logDate, pid, tid, level, component, content);
            }
            catch (Exception)
            {
                return null;
            }
        }

        static DataTable ConvertToDataTable(Entry[] entries, int count)
        {
            var dataTable = CreateTable();
            for (int i = 0; i < count; i++)
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

    record BatchData(Entry[] Entries, int Count);
}

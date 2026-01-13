using System.Net.Sockets;
using System.Text;

namespace Client
{
    internal class Program
    {
        const string SERVER = "127.0.0.1";
        const int    PORT   = 8888;

        static async Task Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Please provide a directory path as an argument.");
                return;
            }

            var files = GetAllFilesToProcess(args[0]);

            await Processor(files);

            Console.WriteLine("All files sent sucessfully...");
        }

        static async Task Processor(List<string> files)
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();

            using (var client = new TcpClient(SERVER, PORT))
            {
                var stream = client.GetStream();

                var filesCount = files.Count;

                for (int i = 0; i < filesCount; i++)
                {
                    await ReadFile(files[i], stream);
                }

                watch.Stop();
                Console.WriteLine($"Time taken: {watch.ElapsedMilliseconds / 1000} seconds");
            }
        }

        static async Task ReadFile(string file, NetworkStream stream)
        {
            var buffer = new byte[32_000];

            using (var ms = new MemoryStream(buffer))
            using (var reader = new StreamReader(file, Encoding.UTF8))
            {
                string? line;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    var lineInBytes = Encoding.UTF8.GetBytes(line + Environment.NewLine);

                    if (ms.Length + lineInBytes.Length > buffer.Length)
                    {
                        await stream.WriteAsync(buffer, 0, (int)ms.Length);
                        ms.Position = 0;
                        ms.SetLength(0);
                    }

                    ms.Write(lineInBytes, 0, lineInBytes.Length);
                }

                if (ms.Length > 0)
                {
                    await stream.WriteAsync(buffer, 0, (int)ms.Length);
                }
            }
        }

        static List<string> GetAllFilesToProcess(string folderPath)
        {
            var filesToProcess = new List<string>();

            var directory = Path.Combine(Directory.GetCurrentDirectory(), folderPath);

            if (!Directory.Exists(directory))
            {
                Console.WriteLine($"Directory not found: {directory}");
                return filesToProcess;
            }

            var folders = Directory.GetDirectories(directory);

            foreach (var folder in folders)
            {
                Console.WriteLine(folder);

                var subFolders = Directory.GetDirectories(folder);

                foreach (var subFolder in subFolders)
                {
                    var files = Directory.GetFiles(subFolder);
                    foreach (var file in files)
                    {
                        if (file.EndsWith(".txt"))
                        {
                            continue;
                        }
                        filesToProcess.Add(file);
                    }
                }

            }
            return filesToProcess;
        }
    }
}

using System.Net.Sockets;
using System.Text;

namespace Client
{
    internal class Program
    {
        const string SERVER = "127.0.0.1";
        const int    PORT   = 8888;
        const int    BUFFER_SIZE = 4_096;


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

            var globalCounter = 0;

            using (var client = new TcpClient(SERVER, PORT))
            {
                var stream = client.GetStream();

                var filesCount = files.Count;

                for (int i = 0; i < filesCount; i++)
                {
                    globalCounter += await ReadFile(files[i], stream);
                }

                watch.Stop();
                Console.WriteLine($"Time taken: {watch.ElapsedMilliseconds / 1000} seconds");
                Console.WriteLine($"Lines read: {globalCounter}");
            }
        }

        static async Task<int> ReadFile(string file, NetworkStream stream)
        {
            Console.WriteLine($"Starting process file: {file}");

            var counter = 0;
            var currentBuffer = new byte[BUFFER_SIZE];
            var bytesRead = 0;

            using (var reader = new FileStream(file, FileMode.Open))
            {
                while ((bytesRead = await reader.ReadAsync(currentBuffer, 0, currentBuffer.Length)) != 0)
                {
                    await stream.WriteAsync(currentBuffer, 0, bytesRead);
                }
            }

            Console.WriteLine($"Process finished");
            return counter;
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

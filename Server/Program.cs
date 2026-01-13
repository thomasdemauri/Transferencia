using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Server
{
    internal class Program
    {
        static void Main(string[] args)
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
                var buffer = new byte[32_000];
                int bytesRead;

                var counter = 0;
                var remainingCharacters = string.Empty;

                while ((bytesRead = stream.Read(buffer, 0, buffer.Length)) != 0)
                {
                    var chunck = remainingCharacters + Encoding.UTF8.GetString(buffer, 0, bytesRead);

                    var messages = chunck.Split(Environment.NewLine);

                    for (int i = 0; i < messages.Length - 1; i++)
                    {
                        counter++;
                    }

                    remainingCharacters = messages[messages.Length - 1];
                }

                if (!string.IsNullOrEmpty(remainingCharacters))
                {
                    counter++;
                }

                Console.WriteLine(counter);

                client.Close();
                Console.WriteLine("Client disconnected.");
            }
        }
    }
}

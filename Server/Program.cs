using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;

namespace Server
{
    internal class Program
    {
        static Regex TagPattern = new("(?<=\\s[IWEFDV]\\s)[^:]+");
        static Regex MessagePattern = new("(?<=:\\s).*$");

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
                        ProcessMessage(messages[i]);
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

        private static void ProcessMessage(string message)
        {
            if (message.Length < 32)
            {
                return;
            }

            var date = message.Substring(0, 18);
            var pid = message.Substring(19, 5).TrimStart();
            var tid = message.Substring(25, 5).TrimStart();
            var log = message.Substring(31, 1);
            var tag = TagPattern.Match(message);
            var messageLog = MessagePattern.Match(message);
        }
    }
}

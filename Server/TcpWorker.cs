using Microsoft.Extensions.Hosting;
using System.Net;
using System.Net.Sockets;

namespace Server
{
    internal class TcpWorker : BackgroundService
    {
        public static int BATCH_SIZE = 10_0000;
        public static int port = 8888;

        private readonly PipelineProcessor _processor;

        public TcpWorker(PipelineProcessor processor)
        {
            _processor = processor;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var listener = new TcpListener(IPAddress.Any, port);

            Console.WriteLine($"Starting TCP Server on port {port}...");
            listener.Start();

            var client = listener.AcceptTcpClient();

            var stream = client.GetStream();


            while (true)
            {
                await _processor.ProcessLinesAsync(stream);
            }

        }
    }
}

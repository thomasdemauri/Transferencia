using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Threading.Channels;
using static Server.LineProcessor;

namespace Server
{
    internal class Program
    {

        static async Task Main(string[] args)
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices(services =>
                {
                    services.AddSingleton(Channel.CreateUnbounded<Entry>());
                    services.AddSingleton<PipelineProcessor>();
                    services.AddSingleton<JobMonitor>();

                    services.AddHostedService<TcpWorker>();
                    services.AddHostedService<ChannelProcessor>();
                })
                .Build();

            host.Run();
        }
    }
}
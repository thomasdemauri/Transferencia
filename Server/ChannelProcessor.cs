using Microsoft.Extensions.Hosting;
using System.Threading.Channels;
using static Server.LineProcessor;

namespace Server
{
    internal class ChannelProcessor : BackgroundService
    {
        private readonly BatchDatabaseProcessor _batchDatabaseProcessor = new BatchDatabaseProcessor();
        private readonly Channel<Entry> _channel;
        private readonly JobMonitor _monitor;
        public ChannelProcessor(Channel<Entry> channel, JobMonitor monitor)
        {
            _channel = channel;
            _monitor = monitor;
        }

        public ChannelWriter<Entry> Writer => _channel.Writer;
        public ChannelReader<Entry> Reader => _channel.Reader;

        private const int BATCH_SIZE = 50000;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var entriesBatch = new Entry[BATCH_SIZE];
            var currentIndex = 0;

            await foreach (var entry in Reader.ReadAllAsync(stoppingToken))
            {
                if (currentIndex >= BATCH_SIZE)
                {
                    await _batchDatabaseProcessor.BulkInsert(entriesBatch, currentIndex);
                    currentIndex = 0;
                    entriesBatch = new Entry[BATCH_SIZE];
                    _monitor.BatchProcessed(currentIndex);
                }

                entriesBatch[currentIndex++] = entry;
            }
            if (currentIndex > 0)
            {
                await _batchDatabaseProcessor.BulkInsert(entriesBatch, currentIndex);
                _monitor.BatchProcessed(currentIndex);
            }
        }
    }
}
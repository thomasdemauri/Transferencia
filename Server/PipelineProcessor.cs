using Microsoft.Data.SqlClient;
using System.Buffers;
using System.Data;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using static Server.LineProcessor;

namespace Server
{
    internal class PipelineProcessor
    {

        private readonly LineProcessor ProcessorLine = new LineProcessor();
        private readonly Channel<Entry> _channel;
        private readonly JobMonitor _monitor;

        public PipelineProcessor(Channel<Entry> channel, JobMonitor monitor)
        {
            _channel = channel;
            _monitor = monitor;
        }

        public async Task ProcessLinesAsync(NetworkStream socket)
        {
            var pipe = new Pipe();
            Task writing = FillPipeAsync(socket, pipe.Writer);
            Task reading = ReadPipeAsync(pipe.Reader);

            await Task.WhenAll(reading, writing);
        }

        async Task FillPipeAsync(NetworkStream socket, PipeWriter writer)
        {
            const int minimumBufferSize = 512;

            while (true)
            {
                Memory<byte> memory = writer.GetMemory(minimumBufferSize);
                try
                {
                    int bytesRead = await socket.ReadAsync(memory);
                    if (bytesRead == 0)
                    {
                        break;
                    }
                    writer.Advance(bytesRead);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    break;
                }

                FlushResult result = await writer.FlushAsync();

                if (result.IsCompleted)
                {
                    break;
                }
            }

            writer.Complete();
        }

        async Task ReadPipeAsync(PipeReader reader)
        {
            while (true)
            {
                ReadResult result = await reader.ReadAsync();

                ReadOnlySequence<byte> buffer = result.Buffer;
                SequencePosition? position = null;

                do
                {
                    position = buffer.PositionOf((byte)'\n');

                    if (position != null)
                    {

                        if (ProcessorLine.Process(buffer.Slice(0, position.Value)) is Entry entry)
                        {
                            await _channel.Writer.WriteAsync(entry);
                            _monitor.ItemProduced();
                        }

                        buffer = buffer.Slice(buffer.GetPosition(1, position.Value));
                    }
                }
                while (position != null);

                reader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }

            reader.Complete();

            _monitor.EndProduction();
        }
    }
}

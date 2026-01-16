using System.Buffers;
using System.Text;

namespace Server
{
    internal partial class LineProcessor
    {
        public Entry? Process(ReadOnlySequence<byte> buffer)
        {
            var stringLine = GetUTF8String(buffer);
            return ProcessMessage(stringLine);
        }

        string GetUTF8String(ReadOnlySequence<byte> buffer)
        {
            if (buffer.IsSingleSegment)
            {
                return Encoding.UTF8.GetString(buffer.First.Span);
            }

            return string.Create((int)buffer.Length, buffer, (span, sequence) =>
            {
                foreach (var segment in sequence)
                {
                    Encoding.UTF8.GetChars(segment.Span, span);

                    span = span.Slice(segment.Length);
                }
            });
        }

        static Entry? ProcessMessage(string message)
        {
            if (message.IsWhiteSpace())
            {
                return null;
            }

            int startIndex = -1;
            for (int i = 0; i < message.Length; i++)
            {
                if (char.IsDigit(message[i]))
                {
                    startIndex = i;
                    break;
                }
            }

            if (startIndex == -1)
            {
                return null;
            }

            var line = message.Substring(startIndex);

            if (line.Length < 33)
            {
                return null;
            }

            try
            {
                var logDate = line.Substring(0, 18).ToString();
                var pidSpan = line.Substring(19, 5).Trim();
                var tidSpan = line.Substring(25, 5).Trim();

                short pid = short.Parse(pidSpan);
                short tid = short.Parse(tidSpan);

                var level = line[31];

                var bodyLine = line.Substring(33);

                var separatorComponent = bodyLine.IndexOf(':');
                if (separatorComponent == -1)
                {
                    separatorComponent = bodyLine.IndexOf('>');
                }
                if (separatorComponent == -1)
                {
                    return null;
                }

                var component = bodyLine.Substring(0, separatorComponent).ToString();
                var content = bodyLine.Substring(separatorComponent + 1).ToString().Trim();


                return new Entry(logDate, pid, tid, level, component, content);
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}

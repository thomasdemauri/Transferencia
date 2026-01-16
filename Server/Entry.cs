namespace Server
{
    internal partial class LineProcessor
    {
        public struct Entry(string logDate, Int16 pid, Int16 tid, char level, string component, string content)
        {
            public string LogDate { get; } = logDate;
            public Int16 Pid { get; } = pid;
            public Int16 Tid { get; } = tid;
            public char Level { get; } = level;
            public string Component { get; } = component;
            public string Content { get; } = content;
        }
    }
}

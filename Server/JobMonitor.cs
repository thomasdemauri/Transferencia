using System.Diagnostics;

public class JobMonitor
{
    private long _producedCount = 0;
    private long _processedCount = 0;
    private bool _productionEnded = false;
    private readonly Stopwatch _watch = new Stopwatch();

    public void StartJob()
    {
        _producedCount = 0;
        _processedCount = 0;
        _productionEnded = false;
        _watch.Restart();
        Console.WriteLine("--- JOB STARTED ---");
    }

    public void ItemProduced()
    {
        Interlocked.Increment(ref _producedCount);
    }

    public void BatchProcessed(int count)
    {
        Interlocked.Add(ref _processedCount, count);
        CheckCompletion();
    }

    public void EndProduction()
    {
        _productionEnded = true;
        CheckCompletion();
    }

    private void CheckCompletion()
    {
        if (_productionEnded && _producedCount >= 27000000)
        {
            _watch.Stop();
            Console.WriteLine("--- JOB FINISHED ---");
            Console.WriteLine($"Total Time: {_watch.Elapsed.TotalSeconds:F2} seconds");
            Console.WriteLine($"Total Items: {_processedCount}");
            Console.ReadLine();
        }
    }
}
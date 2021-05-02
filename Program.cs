using System;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;

namespace azure_amqp_benchmarks
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        }
    }

    class Config : ManualConfig
    {
        public Config()
        {
            AddJob(Job.ShortRun.WithRuntime(CoreRuntime.Core50));
            AddExporter(DefaultExporters.Markdown, DefaultExporters.JsonFull).KeepBenchmarkFiles(true).DontOverwriteResults(true);
            AddDiagnoser(new DisassemblyDiagnoser(new DisassemblyDiagnoserConfig()), MemoryDiagnoser.Default);
        }
    }
}

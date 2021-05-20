using azure_amqp_benchmarks;
using BenchmarkDotNet.Attributes;
using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Encoding;
using System;
using System.Runtime.InteropServices;

[Config(typeof(Config))]
[BenchmarkCategory("Encoding")]
public class ArrayEncoding
{
    public static byte[] RandomBytes1MB = new byte[1024 * 1024];
    public static int[] RandomInt32Array1M = new int[1024 * 1024];
    public static int[] RandomInt32Array1K = new int[1024];
    public static long[] RandomInt64Array1M = new long[1024 * 1024];
    public static long[] RandomInt64Array1K = new long[1024];
    public static uint[] RandomUInt32Array1M = new uint[1024 * 1024];
    public static uint[] RandomUInt32Array1K = new uint[1024];
    public static ulong[] RandomUInt64Array1M = new ulong[1024 * 1024];
    public static ulong[] RandomUInt64Array1K = new ulong[1024];
    public static decimal[] RandomDecimalArray1M = new decimal[1024 * 1024];
    public static decimal[] RandomDecimalArray1K = new decimal[1024];

    public static AmqpSymbol[] RandomAmqpSymbolArray100K = new AmqpSymbol[1024 * 10];
    public static AmqpSymbol[] RandomAmqpSymbolArray1K = new AmqpSymbol[1024];

    public static bool[] RandomBoolArray1M = new bool[1024 * 1024];
    public static bool[] RandomBoolArray1K = new bool[1024];

    // buffers for 1M values
    public static ByteBuffer ScratchByteBuffer = new ByteBuffer(new byte[1024 * 1024 * 32], autoGrow: false);

    public static byte[] EncodedBytes1MB;
    public static ByteBuffer EncodedBytes1MBBuffer;

    public static byte[] EncodedInt32Array1M;
    public static byte[] EncodedInt32Array1K;
    public static byte[] EncodedUInt32Array1M;
    public static byte[] EncodedUInt32Array1K;
    public static byte[] EncodedInt64Array1M;
    public static byte[] EncodedInt64Array1K;
    public static byte[] EncodedUInt64Array1M;
    public static byte[] EncodedUInt64Array1K;
    public static byte[] EncodedBoolArray1M;
    public static byte[] EncodedBoolArray1K;
    public static byte[] EncodedDecimalArray1M;
    public static byte[] EncodedDecimalArray1K;
    public static ByteBuffer EncodedInt32Array1MBuffer;
    public static ByteBuffer EncodedInt32Array1KBuffer;
    public static ByteBuffer EncodedUInt32Array1MBuffer;
    public static ByteBuffer EncodedUInt32Array1KBuffer;
    public static ByteBuffer EncodedInt64Array1MBuffer;
    public static ByteBuffer EncodedInt64Array1KBuffer;
    public static ByteBuffer EncodedUInt64Array1MBuffer;
    public static ByteBuffer EncodedUInt64Array1KBuffer;

    public static ByteBuffer EncodedBoolArray1MBuffer;
    public static ByteBuffer EncodedBoolArray1KBuffer;
    public static ByteBuffer EncodedDecimalArray1MBuffer;
    public static ByteBuffer EncodedDecimalArray1KBuffer;

    public static ByteBuffer EncodedAmqpSymbolArray1MBuffer;
    public static ByteBuffer EncodedAmqpSymbolArray1KBuffer;

    [GlobalSetup]
    public void GlobalSetup()
    {
        Random rng = new Random(0);
        rng.NextBytes(RandomBytes1MB);
        rng.NextBytes(MemoryMarshal.AsBytes(RandomInt32Array1M.AsSpan()));
        rng.NextBytes(MemoryMarshal.AsBytes(RandomInt32Array1K.AsSpan()));
        rng.NextBytes(MemoryMarshal.AsBytes(RandomUInt32Array1M.AsSpan()));
        rng.NextBytes(MemoryMarshal.AsBytes(RandomUInt32Array1K.AsSpan()));
        rng.NextBytes(MemoryMarshal.AsBytes(RandomInt64Array1M.AsSpan()));
        rng.NextBytes(MemoryMarshal.AsBytes(RandomInt64Array1K.AsSpan()));
        rng.NextBytes(MemoryMarshal.AsBytes(RandomUInt64Array1M.AsSpan()));
        rng.NextBytes(MemoryMarshal.AsBytes(RandomUInt64Array1K.AsSpan()));

        FillSymbolArray(rng, RandomAmqpSymbolArray100K);
        FillSymbolArray(rng, RandomAmqpSymbolArray1K);
        FillBoolArray(rng, RandomBoolArray1K);
        FillBoolArray(rng, RandomBoolArray1M);
        FillDecimalArray(rng, RandomDecimalArray1K);
        FillDecimalArray(rng, RandomDecimalArray1M);

        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeBinary(RandomBytes1MB, ScratchByteBuffer);
        EncodedBytes1MB = ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos).ToArray();
        EncodedBytes1MBBuffer = new ByteBuffer(EncodedBytes1MB, 0, EncodedBytes1MB.Length);

        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomInt32Array1M, ScratchByteBuffer);
        EncodedInt32Array1M = ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos).ToArray();
        EncodedInt32Array1MBuffer = new ByteBuffer(EncodedInt32Array1M, 0, EncodedInt32Array1M.Length);

        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomInt32Array1K, ScratchByteBuffer);
        EncodedInt32Array1K = ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos).ToArray();
        EncodedInt32Array1KBuffer = new ByteBuffer(EncodedInt32Array1K, 0, EncodedInt32Array1K.Length);

        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomUInt32Array1M, ScratchByteBuffer);
        EncodedUInt32Array1M = ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos).ToArray();
        EncodedUInt32Array1MBuffer = new ByteBuffer(EncodedUInt32Array1M, 0, EncodedUInt32Array1M.Length);

        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomUInt32Array1K, ScratchByteBuffer);
        EncodedUInt32Array1K = ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos).ToArray();
        EncodedUInt32Array1KBuffer = new ByteBuffer(EncodedUInt32Array1K, 0, EncodedUInt32Array1K.Length);

        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomInt64Array1M, ScratchByteBuffer);
        EncodedInt64Array1M = ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos).ToArray();
        EncodedInt64Array1MBuffer = new ByteBuffer(EncodedInt64Array1M, 0, EncodedInt64Array1M.Length);

        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomInt64Array1K, ScratchByteBuffer);
        EncodedInt64Array1K = ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos).ToArray();
        EncodedInt64Array1KBuffer = new ByteBuffer(EncodedInt64Array1K, 0, EncodedInt64Array1K.Length);

        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomUInt64Array1M, ScratchByteBuffer);
        EncodedUInt64Array1M = ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos).ToArray();
        EncodedUInt64Array1MBuffer = new ByteBuffer(EncodedUInt64Array1M, 0, EncodedUInt64Array1M.Length);

        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomUInt64Array1K, ScratchByteBuffer);
        EncodedUInt64Array1K = ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos).ToArray();
        EncodedUInt64Array1KBuffer = new ByteBuffer(EncodedUInt64Array1K, 0, EncodedUInt64Array1K.Length);

        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomBoolArray1M, ScratchByteBuffer);
        EncodedBoolArray1M = ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos).ToArray();
        EncodedBoolArray1MBuffer = new ByteBuffer(EncodedBoolArray1M, 0, EncodedBoolArray1M.Length);

        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomBoolArray1K, ScratchByteBuffer);
        EncodedBoolArray1K = ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos).ToArray();
        EncodedBoolArray1KBuffer = new ByteBuffer(EncodedBoolArray1K, 0, EncodedBoolArray1K.Length);

        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomDecimalArray1M, ScratchByteBuffer);
        EncodedDecimalArray1M = ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos).ToArray();
        EncodedDecimalArray1MBuffer = new ByteBuffer(EncodedDecimalArray1M, 0, EncodedDecimalArray1M.Length);

        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomDecimalArray1K, ScratchByteBuffer);
        EncodedDecimalArray1K = ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos).ToArray();
        EncodedDecimalArray1KBuffer = new ByteBuffer(EncodedDecimalArray1K, 0, EncodedDecimalArray1K.Length);

        EncodedAmqpSymbolArray1MBuffer = new ByteBuffer(new byte[1024 * 1024 * 10], autoGrow: true);
        AmqpCodec.EncodeArray(RandomAmqpSymbolArray100K, EncodedAmqpSymbolArray1MBuffer);

        EncodedAmqpSymbolArray1KBuffer = new ByteBuffer(new byte[1024 * 1024 * 10], autoGrow: true);
        AmqpCodec.EncodeArray(RandomAmqpSymbolArray1K, EncodedAmqpSymbolArray1KBuffer);
    }

    private static void FillBoolArray(Random rng, bool[] boolArray)
    {
        for (int i = 0; i < boolArray.Length; i++)
        {
            boolArray[i] = rng.Next(1) == 1;
        }
    }

    private static void FillDecimalArray(Random rng, decimal[] decimalArray)
    {
        for (int i = 0; i < decimalArray.Length; i++)
        {
            decimalArray[i] = new decimal(rng.Next(10), rng.Next(10), rng.Next(10), isNegative: false, 0);
        }
    }

    private static void FillSymbolArray(Random rng, AmqpSymbol[] symbolArray)
    {
        for (int i = 0; i < symbolArray.Length; i++)
        {
            int length = rng.Next(10);

            symbolArray[i] = new AmqpSymbol(string.Create(length, rng, (s, innerRng) =>
            {
                char c = (char)innerRng.Next('a', 'z');
                s.Fill(c);
            }));
        }
    }

    [Benchmark]
    public int ArrayAmqpSymbolDecode_1M_MAA()
    {
        EncodedAmqpSymbolArray1MBuffer.Seek(0);
        var result = AmqpCodec.DecodeArray<AmqpSymbol>(EncodedAmqpSymbolArray1MBuffer);
        return result.Length;
    }

    [Benchmark]
    public int ArrayAmqpSymbolDecode_1K_MAA()
    {
        EncodedAmqpSymbolArray1KBuffer.Seek(0);
        var result = AmqpCodec.DecodeArray<AmqpSymbol>(EncodedAmqpSymbolArray1KBuffer);
        return result.Length;
    }

    [Benchmark]
    public void ArrayAmqpSymbolEncode_100K_MAA()
    {
        EncodedAmqpSymbolArray1MBuffer.Reset();
        AmqpCodec.EncodeArray(RandomAmqpSymbolArray100K, EncodedAmqpSymbolArray1MBuffer);
    }

    [Benchmark]
    public void ArrayAmqpSymbolEncode_1K_MAA()
    {
        EncodedAmqpSymbolArray1KBuffer.Reset();
        AmqpCodec.EncodeArray(RandomAmqpSymbolArray1K, EncodedAmqpSymbolArray1KBuffer);
    }

    [Benchmark]
    public ReadOnlyMemory<byte> Bytes_Encode_MAA()
    {
        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeBinary(RandomBytes1MB, ScratchByteBuffer);
        return ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos);
    }

    [Benchmark]
    public ArraySegment<byte> Bytes_Decode_MAA()
    {
        EncodedBytes1MBBuffer.Seek(0);
        var result = AmqpCodec.DecodeBinary(EncodedBytes1MBBuffer);
        return result;
    }

    [Benchmark]
    public ReadOnlyMemory<byte> ArrayInt32Encode_MAA_1M()
    {
        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomInt32Array1M, ScratchByteBuffer);
        return ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos);
    }

    [Benchmark]
    public ReadOnlyMemory<byte> ArrayInt32Encode_MAA_1K()
    {
        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomInt32Array1K, ScratchByteBuffer);
        return ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos);
    }

    [Benchmark]
    public int ArrayInt32Decode_1M_MAA()
    {
        EncodedInt32Array1MBuffer.Seek(0);
        var result = AmqpCodec.DecodeArray<int>(EncodedInt32Array1MBuffer);
        return result.Length;
    }

    [Benchmark]
    public int ArrayInt32Decode_1K_MAA()
    {
        EncodedInt32Array1KBuffer.Seek(0);
        var result = AmqpCodec.DecodeArray<int>(EncodedInt32Array1KBuffer);
        return result.Length;
    }

    [Benchmark]
    public ReadOnlyMemory<byte> ArrayUInt32Encode_MAA_1M()
    {
        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomUInt32Array1M, ScratchByteBuffer);
        return ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos);
    }

    [Benchmark]
    public ReadOnlyMemory<byte> ArrayUInt32Encode_MAA_1K()
    {
        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomUInt32Array1K, ScratchByteBuffer);
        return ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos);
    }

    [Benchmark]
    public int ArrayUInt32Decode_1M_MAA()
    {
        EncodedUInt32Array1MBuffer.Seek(0);
        var result = AmqpCodec.DecodeArray<uint>(EncodedUInt32Array1MBuffer);
        return result.Length;
    }

    [Benchmark]
    public int ArrayUInt32Decode_1K_MAA()
    {
        EncodedUInt32Array1KBuffer.Seek(0);
        var result = AmqpCodec.DecodeArray<uint>(EncodedUInt32Array1KBuffer);
        return result.Length;
    }

    [Benchmark]
    public ReadOnlyMemory<byte> ArrayInt64Encode_MAA_1M()
    {
        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomInt64Array1M, ScratchByteBuffer);
        return ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos);
    }

    [Benchmark]
    public ReadOnlyMemory<byte> ArrayInt64Encode_MAA_1K()
    {
        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomInt64Array1K, ScratchByteBuffer);
        return ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos);
    }

    [Benchmark]
    public int ArrayInt64Decode_1M_MAA()
    {
        EncodedInt64Array1MBuffer.Seek(0);
        var result = AmqpCodec.DecodeArray<long>(EncodedInt64Array1MBuffer);
        return result.Length;
    }

    [Benchmark]
    public int ArrayInt64Decode_1K_MAA()
    {
        EncodedInt64Array1KBuffer.Seek(0);
        var result = AmqpCodec.DecodeArray<long>(EncodedInt64Array1KBuffer);
        return result.Length;
    }

    [Benchmark]
    public ReadOnlyMemory<byte> ArrayUInt64Encode_MAA_1M()
    {
        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomUInt64Array1M, ScratchByteBuffer);
        return ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos);
    }

    [Benchmark]
    public ReadOnlyMemory<byte> ArrayUInt64Encode_MAA_1K()
    {
        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomUInt64Array1K, ScratchByteBuffer);
        return ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos);
    }

    [Benchmark]
    public int ArrayUInt64Decode_1M_MAA()
    {
        EncodedUInt64Array1MBuffer.Seek(0);
        var result = AmqpCodec.DecodeArray<ulong>(EncodedUInt64Array1MBuffer);
        return result.Length;
    }

    [Benchmark]
    public int ArrayUInt64Decode_1K_MAA()
    {
        EncodedUInt64Array1KBuffer.Seek(0);
        var result = AmqpCodec.DecodeArray<long>(EncodedUInt64Array1KBuffer);
        return result.Length;
    }

    [Benchmark]
    public ReadOnlyMemory<byte> ArrayBoolEncode_MAA_1M()
    {
        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomBoolArray1M, ScratchByteBuffer);
        return ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos);
    }

    [Benchmark]
    public ReadOnlyMemory<byte> ArrayBoolEncode_MAA_1K()
    {
        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomBoolArray1K, ScratchByteBuffer);
        return ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos);
    }

    [Benchmark]
    public int ArrayBoolDecode_1M_MAA()
    {
        EncodedBoolArray1MBuffer.Seek(0);
        var result = AmqpCodec.DecodeArray<bool>(EncodedBoolArray1MBuffer);
        return result.Length;
    }

    [Benchmark]
    public int ArrayBoolDecode_1K_MAA()
    {
        EncodedBoolArray1KBuffer.Seek(0);
        var result = AmqpCodec.DecodeArray<bool>(EncodedBoolArray1KBuffer);
        return result.Length;
    }

    [Benchmark]
    public ReadOnlyMemory<byte> ArrayDecimalEncode_MAA_1M()
    {
        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomDecimalArray1M, ScratchByteBuffer);
        return ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos);
    }

    [Benchmark]
    public ReadOnlyMemory<byte> ArrayDecimalEncode_MAA_1K()
    {
        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomDecimalArray1K, ScratchByteBuffer);
        return ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos);
    }

    [Benchmark]
    public int ArrayDecimalDecode_1M_MAA()
    {
        EncodedDecimalArray1MBuffer.Seek(0);
        var result = AmqpCodec.DecodeArray<decimal>(EncodedDecimalArray1MBuffer);
        return result.Length;
    }

    [Benchmark]
    public int ArrayDecimalDecode_1K_MAA()
    {
        EncodedDecimalArray1KBuffer.Seek(0);
        var result = AmqpCodec.DecodeArray<decimal>(EncodedDecimalArray1KBuffer);
        return result.Length;
    }
}


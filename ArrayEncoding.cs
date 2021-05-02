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

    public static AmqpSymbol[] RandomAmqpSymbolArray100K = new AmqpSymbol[1024 * 10];
    public static AmqpSymbol[] RandomAmqpSymbolArray1K = new AmqpSymbol[1024];

    public static bool[] RandomBoolArray1M = new bool[1024 * 1024];
    public static bool[] RandomBoolArray1K = new bool[1024];

    // buffers for 1M values
    public static ByteBuffer ScratchByteBuffer = new ByteBuffer(new byte[1024 * 1024 * 10], autoGrow: false);
    public static byte[] ScratchArray = new byte[1024 * 1024 * 10];

    public static int[] ScratchInt32Array = new int[1024 * 1024 * 2];

    public static byte[] EncodedBytes1MB;
    public static ByteBuffer EncodedBytes1MBBuffer;

    public static byte[] EncodedInt32Array1M;
    public static byte[] EncodedInt32Array1K;
    public static byte[] EncodedBoolArray1M;
    public static byte[] EncodedBoolArray1K;
    public static ByteBuffer EncodedInt32Array1MBuffer;
    public static ByteBuffer EncodedInt32Array1KBuffer;

    public static ByteBuffer EncodedBoolArray1MBuffer;
    public static ByteBuffer EncodedBoolArray1KBuffer;

    public static ByteBuffer EncodedAmqpSymbolArray1MBuffer;
    public static ByteBuffer EncodedAmqpSymbolArray1KBuffer;

    [GlobalSetup]
    public void GlobalSetup()
    {
        Random rng = new Random(0);
        rng.NextBytes(RandomBytes1MB);
        rng.NextBytes(MemoryMarshal.AsBytes(RandomInt32Array1M.AsSpan()));
        rng.NextBytes(MemoryMarshal.AsBytes(RandomInt32Array1K.AsSpan()));

        FillSymbolArray(rng, RandomAmqpSymbolArray100K);
        FillSymbolArray(rng, RandomAmqpSymbolArray1K);
        FillBoolArray(rng, RandomBoolArray1K);
        FillBoolArray(rng, RandomBoolArray1M);

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
        AmqpCodec.EncodeArray(RandomBoolArray1M, ScratchByteBuffer);
        EncodedBoolArray1M = ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos).ToArray();
        EncodedBoolArray1MBuffer = new ByteBuffer(EncodedBoolArray1M, 0, EncodedBoolArray1M.Length);

        ScratchByteBuffer.Reset();
        AmqpCodec.EncodeArray(RandomBoolArray1K, ScratchByteBuffer);
        EncodedBoolArray1K = ScratchByteBuffer.Buffer.AsMemory(0, ScratchByteBuffer.WritePos).ToArray();
        EncodedBoolArray1KBuffer = new ByteBuffer(EncodedBoolArray1K, 0, EncodedBoolArray1K.Length);

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
}


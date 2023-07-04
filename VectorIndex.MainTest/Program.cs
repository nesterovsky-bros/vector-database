using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

using FASTER.core;

using HDF.PInvoke;

using HDF5CSharp;

using NesterovskyBros.VectorIndex;

using static System.Formats.Asn1.AsnWriter;

var randomInput = GetRandomDataset((int)DateTime.Now.Ticks, 10000, 1536);

// Test memory
//{
//  await using var points = new MemoryStore<long, Memory<float>>();
//  await using var ranges = new MemoryStore<long, RangeValue>();
//  await using var pointRanges = new MemoryStore<long, long>();
//  await using var stats = new MemoryStore<StatsKey, Memory<Stats>>();

//  await Test(randomInput, points, ranges, pointRanges, stats);
//}

// Test FasterKV
//{
//var root = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

//await using var points = CreateStore<long, Memory<float>>($"{root}.points");
//await using var ranges = CreateStore<long, RangeValue>($"{root}.ranges");
//await using var pointRanges = CreateStore<long, long>($"{root}.pointRanges");
//await using var stats = CreateStore<StatsKey, Memory<Stats>>($"{root}.stats");

//await Test(randomInput, points, ranges, pointRanges, stats);
//}


// Test2 memory
if (false)
{
  var stopwatch = new Stopwatch();

  stopwatch.Start();

  var input = randomInput.ToList();

  stopwatch.Stop();

  Console.WriteLine($"Load points: {stopwatch.Elapsed}");

  stopwatch.Restart();

  var count = 0L;

  await foreach(var (rangeId, range) in Test2(
    input.ToAsyncEnumerable(),
    (_, _) => new MemoryRangeStore()))
  {
    ++count;
  }

  stopwatch.Stop();

  Console.WriteLine($"Build index: {stopwatch.Elapsed}");
}

// Crafted set.
if (true)
{
  var stopwatch = new Stopwatch();

  stopwatch.Start();

  IEnumerable<(long, Memory<float>)> input()
  {
    var dimensions = 1536;

    for(var i = 0L; i < dimensions; ++i)
    {
      var vector = new float[dimensions];

      vector[i] = 1;

      yield return (i, vector);
    }
  }

  stopwatch.Stop();

  Console.WriteLine($"Load points: {stopwatch.Elapsed}");

  stopwatch.Restart();

  var index = await Test2(
    input().ToAsyncEnumerable(),
    (_, _) => new MemoryRangeStore()).
    ToDictionaryAsync(item => item.rangeId, item => item.range);

  stopwatch.Stop();

  Console.WriteLine($"Build index: {stopwatch.Elapsed}");
}

// Test deep-image-96-angular.hdf5
{
  var fileName = args.Length > 0 ? args[0] : null;

  if(fileName != null)
  {
    var (size, dimension) = GetHdf5DatasetSize(fileName, "/train");
    var datasetInput = GetHdf5Dataset(fileName, "/train", size, dimension);
    using var store = new MemoryMappedIndexTempStore(size, dimension);


    // /train, /test

    var stopwatch = new Stopwatch();

    //stopwatch.Start();

    //var input = datasetInput.ToList();

    //stopwatch.Stop();

    //Console.WriteLine($"Load points: {stopwatch.Elapsed}");

    stopwatch.Restart();

    var index = await Test2(
      datasetInput.ToAsyncEnumerable(),
      //(_, _) => new MemoryRangeStore()).
      store.NextStore).
      ToDictionaryAsync(item => item.rangeId, item => item.range);

    stopwatch.Stop();

    Console.WriteLine($"Build index: {stopwatch.Elapsed}, ranges: {index.Count}");
  }
}

async ValueTask Test(
  IEnumerable<(long id, Memory<float> vector)> input,
  IStore<long, Memory<float>> points,
  IStore<long, RangeValue> ranges,
  IStore<long, long> pointRanges,
  IStore<StatsKey, Memory<Stats>> stats)
{
  var stopwatch = new Stopwatch();

  stopwatch.Start();

  foreach(var item in input)
  {
    await points.Set(item.id, item.vector);
  }

  stopwatch.Stop();

  Console.WriteLine($"Load points: {stopwatch.Elapsed}");

  stopwatch.Restart();

  await IndexBuilder.Build(points, ranges, pointRanges, stats);

  stopwatch.Stop();

  //await foreach(var value in ranges.GetItems())
  //{
  //  var range = IndexBuilder.GetRange(value.id, value.value);

  //  Console.WriteLine(range);
  //}

  Console.WriteLine($"Build index: {stopwatch.Elapsed}");
}

IAsyncEnumerable<(long rangeId, RangeValue range)> Test2(
  IAsyncEnumerable<(long id, Memory<float> vector)> input,
  Func<long, long, IRangeStore> storeFactory) =>
  IndexBuilder.Build(input, storeFactory);

FasterStore<K, V> CreateStore<K, V>(string path)
  where K: unmanaged
{
  var settings = new FasterKVSettings<K, V>(path, true);

  try
  {
    settings.MemorySize = 1L << 30;
    settings.PageSize = 1L << 25;
    settings.IndexSize = 1L << 26;

    return new FasterStore<K, V>(settings);
  }
  catch
  {
    settings.Dispose();

    throw;
  }
}

IEnumerable<(long id, Memory<float> vector)> GetRandomDataset(
  int seed, 
  long count, 
  short dimensions)
{
  var random = new Random(seed);

  for(var i = 0L; i < count; ++i)
  {
    var vector = new float[dimensions];

    for(var j = 0; j < vector.Length; ++j)
    {
      vector[j] = random.NextSingle() * 2 - 1;
    }

    yield return (i, vector);
  }
}

(long count, short dimensions) GetHdf5DatasetSize(
  string fileName, 
  string datasetName)
{
  var fileId = Hdf5.OpenFile(fileName, true);
  var datasetId = H5D.open(fileId, Hdf5Utils.NormalizedName(datasetName));

  try
  {
    var spaceId = H5D.get_space(datasetId);

    try
    {
      int rank = H5S.get_simple_extent_ndims(spaceId);

      if(rank != 2)
      {
        throw new InvalidOperationException("Invalid rank.");
      }

      ulong[] maxDims = new ulong[rank];
      ulong[] dims = new ulong[rank];

      H5S.get_simple_extent_dims(spaceId, dims, maxDims);

      return (checked((long)maxDims[0]), checked((short)maxDims[1]));
    }
    finally
    {
      H5S.close(spaceId);
    }
  }
  finally
  {
    H5D.close(datasetId);
  }
} 

IEnumerable<(long id, Memory<float> vector)> GetHdf5Dataset(
  string fileName, 
  string datasetName,
  long size,
  short dimension)
{
  var index = 0L;
  var step = 10000;
  var fileId = Hdf5.OpenFile(fileName, true);

  try
  { 
    while(index < size)
    {
      var rows = Hdf5.ReadDataset<float>(
        fileId,
        datasetName,
        checked((ulong)index),
        Math.Min(checked((ulong)(index + step - 1)), checked((ulong)(size - 1))));

      var count = rows.GetLength(0);

      for(var i = 0; i < count; i++)
      {
        var row = new float[dimension];

        for(var j = 0; j < row.Length; ++j)
        {
          row[j] = rows[i, j];
        }

        yield return (index++, row);
      }
    }
  }
  finally
  {
    Hdf5.CloseFile(fileId);
  }
}

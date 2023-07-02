using System.Diagnostics;

using FASTER.core;

using HDF.PInvoke;

using HDF5CSharp;

using NesterovskyBros.VectorIndex;

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
    () => new MemoryRangeStore()))
  {
    ++count;
  }

  stopwatch.Stop();

  Console.WriteLine($"Build index: {stopwatch.Elapsed}");
}

// Test deep-image-96-angular.hdf5
{
  var fileName = args.Length > 0 ? args[0] : null;

  if(fileName != null)
  {
    var datasetInput = GetHdf5Dataset(fileName, "/train");

    // /train, /test
    //var flat = Hdf5.ReadFlatFileStructure(fileName);

    var stopwatch = new Stopwatch();

    stopwatch.Start();

    var input = datasetInput.ToList();

    stopwatch.Stop();

    Console.WriteLine($"Load points: {stopwatch.Elapsed}");

    stopwatch.Restart();

    var count = 0L;

    await foreach(var (rangeId, range) in Test2(
      input.ToAsyncEnumerable(),
      () => new MemoryRangeStore()))
    {
      ++count;

      if(count < 10 ||
        count < 100 && count % 10 == 0 ||
        count < 1000 && count % 100 == 0 ||
        count < 10000 && count % 1000 == 0 ||
        count % 100000 == 0)
      {
        Console.WriteLine($"Processed {count} ranges.");
      }
    }

    stopwatch.Stop();

    Console.WriteLine($"Build index: {stopwatch.Elapsed}");
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
  Func<IRangeStore> storeFactory) =>
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

IEnumerable<(long id, Memory<float> vector)> GetHdf5Dataset(
string fileName, 
  string datasetName)
{
  var fileId = Hdf5.OpenFile(fileName, true);
  var index = 0UL;
  var step = 1000U;
  var size = 0UL;
  var dimension = (short)0;

  try
  {
    var datasetId = H5D.open(fileId, Hdf5Utils.NormalizedName(datasetName));

    try
    {
      var spaceId = H5D.get_space(datasetId);

      try
      {
        int rank = H5S.get_simple_extent_ndims(spaceId);

        if (rank != 2)
        {
          throw new InvalidOperationException("Invalid rank.");
        }

        ulong[] maxDims = new ulong[rank];
        ulong[] dims = new ulong[rank];
        
        H5S.get_simple_extent_dims(spaceId, dims, maxDims);

        size = maxDims[0];
        dimension = checked((short)maxDims[1]);
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

    while(index < size)
    {
      var rows = Hdf5.ReadDataset<float>(
        fileId,
        datasetName,
        index,
        Math.Min(index + step - 1, size));

      var count = rows.GetLength(0);

      for(var i = 0; i < count; i++)
      {
        var row = new float[dimension];

        for(var j = 0; j < row.Length; ++j)
        {
          row[j] = rows[i, j];
        }

        yield return ((long)index++, row);
      }
    }
  }
  finally
  {
    Hdf5.CloseFile(fileId);
  }
}

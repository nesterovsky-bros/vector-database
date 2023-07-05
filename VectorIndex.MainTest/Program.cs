using System.Diagnostics;

using HDF.PInvoke;

using HDF5CSharp;

using NesterovskyBros.VectorIndex;

var randomInput = GetRandomDataset((int)DateTime.Now.Ticks, 10000, 1536);

// Test2 memory
if (true)
{
  var stopwatch = new Stopwatch();

  stopwatch.Start();

  var index = new Dictionary<long, RangeValue>();

  await foreach(var (rangeId, range) in 
    Test(
      randomInput,
      (_, _) => new MemoryRangeStore()))
  {
    index.Add(rangeId, range);
  }

  stopwatch.Stop();

  Console.WriteLine($"Build index: {stopwatch.Elapsed}");
}

// Crafted set.
if (true)
{
  var stopwatch = new Stopwatch();

  stopwatch.Start();

  var index = new Dictionary<long, RangeValue>();

  await foreach(var (rangeId, range) in
    Test(
      input(),
      (_, _) => new MemoryRangeStore()))
  {
    index.Add(rangeId, range);
  }

  stopwatch.Stop();

  Console.WriteLine($"Build index: {stopwatch.Elapsed}");

  async IAsyncEnumerable<(long, Memory<float>)> input()
  {
    var dimensions = 1536;

    for(var i = 0L; i < dimensions; ++i)
    {
      var vector = new float[dimensions];

      vector[i] = 1;

      yield return (i, vector);
    }
  }
}

// Test deep-image-96-angular.hdf5
if (true)
{
  var fileName = args.Length > 0 ? args[0] : null;

  if(fileName != null)
  {
    using var outputWriter = args.Length > 1 ? File.CreateText(args[1]) : null;

    if (outputWriter != null)
    {
      await outputWriter.WriteLineAsync("RangeID,Dimension,Mid,ID");
    }

    // /train, /test
    var (size, dimension) = GetHdf5DatasetSize(fileName, "/train");
    var datasetInput = GetHdf5Dataset(fileName, "/train", size, dimension);
    using var store = new FileRangeStore(size, dimension, 10000);

    var stopwatch = new Stopwatch();

    if(args.Length > 2)
    {
      var count = 0L;
      using var trainWriter = File.CreateText(args[2]);

      await trainWriter.WriteLineAsync("ID|Vector");

      await foreach(var (id, vector) in datasetInput)
      {
        await trainWriter.WriteLineAsync($"{id}|{string.Join(',', vector.ToArray())}");

        ++count;

        if(count % 100000 == 0)
        {
          Console.WriteLine($"Processed {count} records.");
        }
      }
    }

    if (args.Length > 3)
    {
      var count = 0L;
      var (testSize, testDimension) = GetHdf5DatasetSize(fileName, "/test");
      var testDataset = GetHdf5Dataset(fileName, "/test", testSize, testDimension);
     
      using var testWriter = File.CreateText(args[3]);

      await testWriter.WriteLineAsync("ID,Vector");

      await foreach(var (id, vector) in testDataset)
      {
        await testWriter.WriteLineAsync($"{id}|{string.Join(',', vector.ToArray())}");

        ++count;

        if(count % 100000 == 0)
        {
          Console.WriteLine($"Processed {count} records.");
        }
      }
    }

    stopwatch.Start();

    var index = new Dictionary<long, RangeValue>();

    await foreach(var (rangeId, range) in
      Test(
        datasetInput,
        //(_, _) => new MemoryRangeStore()).
        store.NextStore))
    {
      index.Add(rangeId, range);

      if (outputWriter != null)
      {
        await outputWriter.WriteLineAsync(
          $"{rangeId},{range.Dimension},{range.Mid},{range.Id}");
      }
    }

    stopwatch.Stop();

    Console.WriteLine($"Build index: {stopwatch.Elapsed}, ranges: {index.Count}");
  }
}

IAsyncEnumerable<(long rangeId, RangeValue range)> Test(
  IAsyncEnumerable<(long id, Memory<float> vector)> input,
  Func<long, long, IRangeStore> storeFactory) =>
  IndexBuilder.Build(input, storeFactory);

async IAsyncEnumerable<(long id, Memory<float> vector)> GetRandomDataset(
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

async IAsyncEnumerable<(long id, Memory<float> vector)> GetHdf5Dataset(
  string fileName, 
  string datasetName,
  long size,
  short dimension)
{
  var index = 0L;
  var step = 100000;
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

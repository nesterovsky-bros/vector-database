using System.Diagnostics;
//using System.Text.Json;

using FASTER.core;

using NesterovskyBros.VectorIndex;

//using Range = NesterovskyBros.VectorIndex.Range;

//var json = await File.ReadAllTextAsync(
//    "C:\\Temp\\Chatbot\\melinda\\melinda.ocr.en.json");
//using var document = JsonDocument.Parse(json);

//var embeddings = document.RootElement.
//    GetProperty("DocumentEmbeddings").
//    EnumerateArray().
//    SelectMany(item => item.GetProperty("Value").EnumerateArray()).
//    Select((item, index) =>
//    new
//    {
//      index,
//      text = item.GetProperty("Metadata").GetProperty("text").GetString(),
//      vector = item.
//        GetProperty("Embedding").
//        GetProperty("vector").
//        EnumerateArray().
//        Select(item => (float)item.GetDouble()).
//        ToArray()
//    }).
//    ToList();

await using var points = new MemoryStore<long, float[]>();
await using var ranges = new MemoryStore<long, IndexRange>();
await using var pointRanges = new MemoryStore<long, long>();
await using var stats = new MemoryStore<StatsKey, Stats[]>();

//var root = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

//await using var points = CreateStore<long, float[]>($"{root}.points");
//await using var ranges = CreateStore<long, IndexRange>($"{root}.ranges");
//await using var pointRanges = CreateStore<long, long>($"{root}.pointRanges");
//await using var stats = CreateStore<StatsKey, Stats[]>($"{root}.stats");

var stopwatch = new Stopwatch();

stopwatch.Start();

var random = new Random();

//foreach (var item in embeddings)
//{
//    await points.Set(item.index, item.vector);
//}

for(var i = 0; i < 100000; ++i)
{
  var vector = new float[1536];

  for(var j = 0; j < vector.Length; ++j)
  {
    vector[j] = random.NextSingle() * 2 - 1;
  }

  await points.Set(i, vector);
}

stopwatch.Stop();

Console.WriteLine($"Load points: {stopwatch.Elapsed}");

stopwatch.Restart();

await IndexBuilder.Build(points, ranges, pointRanges, stats);

var result = await ranges.GetItems().ToArrayAsync();

stopwatch.Stop();

Console.WriteLine($"Build index: {stopwatch.Elapsed}");

//var embeddingJson = JsonSerializer.Serialize(embeddings, new JsonSerializerOptions() { WriteIndented = true });

//await File.WriteAllTextAsync("C:\\Temp\\Chatbot\\melinda\\melinda.embedding.json", embeddingJson);

//await foreach(var range in ranges.GetItems())
//{
//  Console.WriteLine(range);
//}

FasterStore<K, V> CreateStore<K, V>(string path)
  where K: struct
{
  var settings = new FasterKVSettings<K, V>(path, true);

  try
  {
    settings.MemorySize = 1L << 20;
    settings.PageSize = 1L << 16;
    settings.IndexSize = 1L << 16;

    return new FasterStore<K, V>(settings);
  }
  catch
  {
    settings.Dispose();

    throw;
  }
}

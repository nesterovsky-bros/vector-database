using ArffTools;

namespace NesterovskyBros.VectorIndex;

[TestClass]
public class MemoryVectorIndexTests
{
  const string datasets = "https://raw.githubusercontent.com/nesterovsky-bros/clustering-benchmark/master/src/main/resources/datasets/";

  [TestMethod]
  public void Test_3_3()
  {
    List<Record> records = [];

    for(var i = 0; i < 3; ++i)
    {
      for(var j = 0; j < 3; ++j)
      {
        records.Add(new() 
        { 
          id = records.Count, 
          tag = $"{i},{j}", 
          vector = [i - 1, j - 1] 
        });
      }
    }

    Test("Test_3_3", records, [.5f, .9f], .6f);
  }

  [TestMethod]
  public void Test_10_10()
  {
    List<Record> records = [];

    for(var i = 0; i < 10; ++i)
    { 
      for(var j = 0; j < 10; ++j)
      {
        records.Add(new()
        {
          id = records.Count,
          tag = $"{i},{j}",
          vector = [(i - 4.5f) / 5, (j - 4.5f) / 5]
        });
      }
    }

    Test("Test_10_10", records, [.3f, .3f], .3f);
  }

  [TestMethod]
  public void Test_100_100()
  {
    List<Record> records = [];

    for(var i = 0; i < 100; ++i)
    {
      for(var j = 0; j < 100; ++j)
      {
        records.Add(new()
        {
          id = records.Count,
          tag = $"{i},{j}",
          vector = [(i - 49.5f) / 50, (j - 49.5f) / 50]
        });
      }
    }

    Test("Test_100_100", records, [.3f, .3f], .1f);
  }

  [TestMethod]
  public void Test_1000_1000()
  {
    List<Record> records = [];

    for(var i = 0; i < 1000; ++i)
    {
      for(var j = 0; j < 1000; ++j)
      {
        records.Add(new()
        {
          id = records.Count,
          tag = $"{i},{j}",
          vector = [(i - 499.5f) / 500, (j - 499.5f) / 500]
        });
      }
    }

    Test("Test_1000_1000", records, [.3f, .3f], .05f);
  }

  [TestMethod]
  public void Test_100_100_NotNormalizedVectors()
  {
    List<Record> records = [];

    for(var i = 0; i < 100; ++i)
    {
      for(var j = 0; j < 100; ++j)
      {
        records.Add(new()
        {
          id = records.Count,
          tag = $"{i},{j}",
          vector = [i - 1, j - 1]
        });
      }
    }

    Test("Test_100_100_NotNormalizedVectors", records, [.3f, .3f], .3f);
  }

  [TestMethod]
  public async Task Test_2d_10c()
  {
    var dataset = await Dataset.Read("artificial/2d-10c.arff");
    float[] point = [(73 - dataset.offsetX) / dataset.scale, (70 - dataset.offsetX) / dataset.scale];
    var distance = 10f / dataset.scale;

    var match = Test("Test_2d_10c", dataset.records, point, distance);

    var view = $"X, Y\n{string.Join(
      '\n', 
      match.Select(record =>
      {
        var vector = dataset.Scale(record.vector);

        return $"{vector[0]}, {vector[1]}";
      }))}";

    Console.WriteLine(view);
  }

  private static List<Record> Test(
    string name,
    List<Record> records,
    float[] point,
    float distance)
  {
    var index = new MemoryVectorIndex<Record>(records, record => record.vector);

    //var view = System.Text.Json.JsonSerializer.Serialize(
    //  index.IndexHierarchy.Select(item => new
    //  {
    //    item.index,
    //    item.parent,
    //    center = item.center.ToArray(),
    //    records = item.records?.Select(item => item.vector).ToArray()
    //  }),
    //  new System.Text.Json.JsonSerializerOptions()
    //  {
    //    WriteIndented = true
    //  });

    //Console.WriteLine(view);

    Assert.AreEqual(index.Count, records.Count);

    var plainMatch = records.
      Where(record => Distance(record.vector, point) <= distance).
      ToList();

    var testCount = 0;

    var match = index.
      Find(
        point,
        distance,
        (record, vector) =>
        {
          ++testCount;

          return Distance(record.vector, vector.Span) <= distance;
        }).
      ToList();

    var unmatch = records.
      ExceptBy(match.Select(record => record.id), record => record.id).
      ToList();

    var invalidMatch = match.
      Where(record => Distance(record.vector, point) > distance).
      ToList();

    var invalidUnmatch = unmatch.
      Where(record => Distance(record.vector, point) <= distance).
      ToList();

    Console.WriteLine($"{name
      }:\n  records: {records.Count }, distance: {distance
      }\n  matched: {match.Count} - {
      (float)match.Count / records.Count:P1}\n  predicate calls: {
      testCount} - {(float)testCount / records.Count:P1}\n  predicates per match: {
      (float)testCount / match.Count:N1}.");

    Assert.AreEqual(invalidMatch.Count, 0);
    Assert.AreEqual(invalidUnmatch.Count, 0);
    Assert.AreEqual(match.Count, plainMatch.Count);
    
    Assert.IsTrue(!match.
      ExceptBy(plainMatch.Select(record => record.id), record => record.id).
      Any());

    return match;
  }

  private static float Distance(
    ReadOnlySpan<float> a,
    ReadOnlySpan<float> b)
  {
    var x = a[0] - b[0];
    var y = a[1] - b[1];

    return MathF.Sqrt(x * x + y * y);
  }

  public record struct Record
  {
    public float X => vector?[0] ?? 0;
    public float Y => vector?[1] ?? 0;

    public int id;
    public string? tag;
    public float[] vector;
  }

  public record Dataset
  {
    public List<Record> records = null!;
    public float offsetX;
    public float offsetY;
    public float scale;

    public Dataset() { }

    public Dataset(List<Record> records, bool normalize = true)
    {
      this.records = records;

      if (!normalize)
      {
        scale = 1;

        return;
      }

      var minX = float.PositiveInfinity;
      var maxX = float.NegativeInfinity;
      var minY = float.PositiveInfinity;
      var maxY = float.NegativeInfinity;

      foreach(var record in records)
      {
        var x = record.vector[0];
        var y = record.vector[1];

        minX = Math.Min(minX, x);
        maxX = Math.Max(maxX, x);
        minY = Math.Min(minY, y);
        maxY = Math.Max(maxY, y);
      }

      if (minX >= -1 && maxX <= 1 && minY >= -1 && maxY <= 1)
      {
        scale = 1;

        return;
      }

      if (maxX - minX <= 2 && maxY - minY <= 2)
      {
        scale = 1;
        offsetX = minX >= -1 && maxX <= 1 ? 0 : (minX + maxX) / 2;
        offsetY = minY >= -1 && maxY <= 1 ? 0 : (minY + maxY) / 2;

        foreach(var record in records)
        {
          var vector = record.vector;

          vector[0] -= offsetX;
          vector[1] -= offsetY;
        }

        return;
      }
      else
      {
        scale = Math.Max(maxX - minX, maxY - minY) / 2;
        offsetX = minX >= -1 && maxX <= 1 ? 0 : (minX + maxX) / 2;
        offsetY = minY >= -1 && maxY <= 1 ? 0 : (minY + maxY) / 2;

        foreach(var record in records)
        {
          var vector = record.vector;

          vector[0] = (vector[0] - offsetX) / scale;
          vector[1] = (vector[01] - offsetY) / scale;
        }

        return;
      }
    }

    public static async Task<Dataset> Read(string path, bool normalize = true)
    {
      List<Record> records = [];

      {
        using var client = new HttpClient();
        using var reader =
          new ArffReader(await client.GetStreamAsync($"{datasets}/{path}"));
        var header = reader.ReadHeader();

        while(true)
        {
          var row = reader.ReadInstance();

          if (row == null)
          {
            break;
          }

          var x = Convert.ToSingle(row[0]);
          var y = Convert.ToSingle(row[1]);
          var tag = Convert.ToString(row[2]);

          records.Add(new()
          {
            id = records.Count,
            tag = tag,
            vector = [x, y]
          });
        }
      }

      return new Dataset(records, normalize);
    }

    public float[] Scale(float[] vector) =>
      [vector[0] * scale + offsetX, vector[1] * scale + offsetY];
  }
}
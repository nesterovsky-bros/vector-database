# vector-database
Turn SQL Server into vector database

# Turn Akinator into vector database!
Several years ago we have shown how to turn SQL Server into Akinator like engine. See [KB](https://github.com/nesterovsky-bros/KB) repository.

At that time we did not know about vector databases.
We just implemented a binary search index to identify an object from a set of objects by a set of properties inherent to that object.

## Briefly
Assume you have a set of objects.  
Assume you have a set of boolean properties.  
We hava a matix of `[objects x properties]` with `true` or `false` in cells.  

If we present all properties as a vectors: `[p1, p2, p3, ..., pn]` then we turn original task to a task of creating an index of objects by a vector of booleans.

## Present time vector database
It is only a half a step to extend vector of booleans to vector of floats. It is enough to say that float can be represented as a set of bits (booleans), so all ideas of KB database apply to a vector database.

## Vector database
  ### Let's formulate the idea.
1. We have a set of vectors.  
2. We want to build an index that allows us to efficiently find vectors in some vicinity of a given vector.  
3. To achieve the goal we use "divide and conquer" method.

3.1. Split whole vector space in two parts.  
  There are multiple ways to do this but we selected one of the simplest and available in the SQL.  
  We calculate a mean `avg()` and a standard deviation `stdev()` of all vectors for each dimension.  
  For the split we select a dimension with highest standard deviation, and split in the mean point.  
  This gives us two subsets of vectors of similar cardinality.  
  
3.2. Repeat step 3.1 for each subset, unless it contains exactly one vector.    

The height of the tree that we build this way is proportional to `Log2(N)`, where `N` is number of vectors in the set.  
Estimation gives that for a set of `N` vectors the number of operations required to build such binary index is proportional to `N*Log2(N)`.  
Obviously, compexity of algorithm is propotional to a dimension of vectors.

  ### SQL Server
SQL Server lets us to store float vectors as JSON. Not the best storage type, but we go for it.
Here is our vector table:

```SQL
create table dbo.Text
(
  TextID bigint not null primary key,
  Text nvarchar(max) null,
  Vector varchar(max) null
);
```

Please note that this table is used to bind `TextID` to `Vector` and to build the search index, but not for a search itself.

Here is a structure of the binary index:

```SQL
create table dbo.TextIndex
(
  RangeID bigint not null primary key,
  Dimension smallint null,
  Mid real null,
  LowRangeID bigint null,
  HighRangeID bigint null,
  TextID bigint null
);
```

The search starts from a given `vector` and a `proximity`.  
We start from the root `RangeID = 0`, and compare `Dimension` of input `vector ± proximity` against `Mid`.  
Depending on the outcome we proceed to low (`LowRangeID`), high (`HighRangeID`), or to both ranges.  
We repeat previous step with next ranges until we locate all matched vectors.

Estimation tells that we shall complete the searh at most in `Log2(N)` steps.

## Implementation
An implementation may worth more than theories.
So, you're welcome to see it in [DDL.sql](./DDL.sql)

## Use
1. Create a document: insert something in `dbo.Document`.
2. Populate vectors: insert something into `dbo.Text`. Note that `dbo.Text.Vector` should be a JSON array of floats.
3. Index the document: call the stored procedure `dbo.IndexDocument`.
4. Do the search: call the table valued function `dbo.Search`.

That's all.
Thank you for you attention.

**P.S.:** In addition we have implemented similar [index builder algorithm](./VectorIndex/IndexBuilder.cs) in C#. Though it has the same asymptotical complexity `N*Log2(N)`, it works faster. So, in more complex setup index builder may be implemented in the C#, and search is in the pure SQL.

**P.P.S.:** By the same token we can implement efficient vector index in any SQL database that supports recursive CTE (like SQLite), or in CosmosDB as a function.

 ## C#

It turned out that our initial parallel C# implementation is not scalable for relatively big datasets like deep-image-96-angular, containing ~10M vectors.
Though it is parallel and has `O(N*Log2(N))` complexity, it runs wildly against Process/CPU data locality, and producess enormous number of Page Faults.
Alternative data storage like FasterKV turns out to be too slow.

So, we went and refactored the code from parallel tree level processor into sequential tree walker.
It virtually follows steps 3.1, and 3.2 sequentially for one range at time. See: https://github.com/nesterovsky-bros/vector-database/blob/deea9da842cb12e4edcde4e03a1e68014754d15b/VectorIndex/IndexBuilder.cs#L488.

In such mode we are able to build an index on a laptop just in 3 minutes.

Right now we want to implement benchmarks like in https://qdrant.tech/benchmarks/, though we're not going to implement Client-Server protocol right now. 
Yet, we think we can get the idea where we stand comparing to other vector engines.


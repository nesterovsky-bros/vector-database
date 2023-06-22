# vector-database
Turn SQL Server into vector database

# Turn Akinator into vector database!

Several years ago we have shown how to turn SQL Server into Akinator like engine. See [KB](https://github.com/nesterovsky-bros/KB) repository.

At that time we did not know about vector databases.
We just implemented a binary search index to identify an object from a set of objects by a set of properties inherent to that object.

Briefly
-------

Assume you have a set of objects.  
Assume you have a set of boolean properties.  
We hava a matix of (objects, properties) with true or false in cells.  

If we present all properties as a vector: [p1, p2, p3, ..., pn] then we turn original task to a task of creating an index of object by vector of booleans.

Present time vector database
----------------------------

It is only a half step to extend vector of booleans to vector of floats. It is enough to say that float is represented as a set of bits (booleans), so all ideas of KB database apply to a vector database.

Vector database
---------------

Let's formulate the idea.

1. We have a set of vectors.  
2. We want to build an index that allows us to efficiently find vectors in some vicinity of a given vector.  
3. To achieve the goal we select divide and conquer method.

3.1. Lets split whole vector space in two parts.  
  There are multiple ways to do this but we selected one of the simplest and awailable in the SQL.  
  For each dimension we calculated a mean `avg()` and standard deviation `stdev()`.  
  For the split we select a dimension with highest standard deviation, and split in the mean point.  
  This gives us two subsets of vectors of similar cardinality.  
  
3.2. Repeat step 3.1 for each subset until it contains exactly one vector.  

The height of the tree is proportional to `Log2(N)`, where `N` is number of vectors in the set.  
Estimation gives that for a set of `N` vectors the number of operations required to build such binary index is proportional to `N*Log2(N)`.
Obviously, compexity of algorithm is propotional to a dimension of vectors.

SQL Server lets us to store float vectors as JSON. Not the best storage type, but we go for it.
Here is our vector table:

```SQL
CREATE TABLE dbo.Text
(
  DocID bigint NOT NULL,
  TextID bigint NOT NULL,
  Text nvarchar(max) NULL,
  Vector varchar(max) NULL,
  PRIMARY KEY CLUSTERED(DocID, TextID)
);
```

Please note that this table is used to bind ID to vector, and to build the search index but not for a search itself.

Here is a structure of binary index:

```SQL
CREATE TABLE dbo.TextIndex
(
  DocID bigint NOT NULL,
  RangeID bigint NOT NULL,
  Dimension smallint NULL,
  Mid real NULL,
  LowRangeID bigint NULL,
  HighRangeID bigint NULL,
  TextID bigint NULL,
  PRIMARY KEY(RangeID, DocID) 
  UNIQUE(DocID, RangeID)
);
```

The search starts from a given vector and its proximity.  
We start from the root `RangeID = 0`, and compare `Dimension` of input vector against `Mid`.  
Depending on outcome we proceed to low (`LowRangeID`), high (`HighRangeID`), or to both ranges.  
We repeat previous step with next ranges until we locate all matched vectors.

Again, estimation tells that we shall complete the searh at most in `Log2(N)` steps.

Implementation
--------------
An implementation may worth more than theories.
So, you're welcome to see it at [DDL.sql](./DDL.sql)

Use
---
1. Create a document: insert something in `dbo.Document`.
2. Populate vectors: insert something into `dbo.Text`. Note that `dbo.Text.Vector` should be a JSON array of floats.
3. Index the document: call the stored procedure `dbo.IndexDocument`.
4. Do the search: call the table valued function `dbo.Search`.

That's all.
Thank you for you attention.

**P.S.:** In addition we have implemented similar index builder algorithms in C#. Though it has the same asympthotical complexity `N*Log2(N)`, it works faster. So, in more complex setup index builder may be implemented in the C#, and search is in the pure SQL.

**P.P.S.:** By the same token we can implement efficient vector index in any SQL database that supports recursive CTE, line SQLite, or in CosmosDB as a function.

# vector-database
Turn SQL Server into vector database

#Turn Akinator into vector database!

Several year ago we have shown how to turn SQL Server into Akinator like engine. See [KB](https://github.com/nesterovsky-bros/KB) repository.

At that time we did not know about vector databases at that time.
We just implemented a binary search index to identify an object from a set of objects by a set of properties inherent to that object.

Briefly
-------

Assume you have a set of objects. 
Assume you have a set of boolean properties. 
We hava a matix of objects x properties with true or false in cells. 

If we list all properties as a list then we have a vector: [p1, p2, p3, ..., pn]. 
So, we created an index of object by vector of booleans.

Present time vector database
----------------------------

It is only a half step to extend vector of booleans to vector of floats. It is just enough to say that float is represented as a set of bit fields, which are booleans, so all ideas of KB database fully apply to vector database.







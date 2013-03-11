# Data-Intensive Computing with MapReduce (Spring 2013)
## Assignment 3
### Question 1
The size of my compressed index is 9.1 MB

Grading
=======

Everything looks fine---good job!

As a side note, though, using a Pair object to represent the postings
list makes the index larger than it needs to be: you can actually fit
the *df* in the `BytesWritable` also.

Score: 35/35

-Jimmy

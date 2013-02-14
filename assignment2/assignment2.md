# MapReduce Assignment 2

## Question 0

For the Pairs implementation, I used two MapReduce jobs: The mapper of the first job has output type of (`PairOfStrings`, `IntWritable`). It emits a value ONE for each pair and special pairs (`TERM`, `'\1'`), where the latter is used to compute the marginal. The reducer then counts the pairs and the marginal, and computes the relative frequency of each pair. The reducer output type is (`PairOfStrings`, `FloatWritable`). Marginal counts are also preserved in the output file (in the form of "((`'\1'`, `'TERM'`), `marginal`)"). In the second MapReduce job the mappers do essentially nothing but to parse the itermediate file and emit records for re-grouping. Its output type is (`MyPairOfStrings`, `FloatWritable`). Here `MyPairOfStrings` is a modification to `PairOfStrings` to make the second term the primary order index. At last the reducers get the re-grouped records and marginal counts and compute the PMI (output type is (`MyPairOfStrings`, `FloatWritable`)). Combiners comes to help only for the first MR job, with both input and output pairs as of (`PairOfStrings`, `IntWritable`). The combiner's job is to aggregate the counts of pairs. 

For the Stripes implementation, I also used two MR jobs. The mapper of the first job emits a map: (`Text`, `HMapSIW`), where the value of each present map entry is 1. The reducer simply combines the map lists and write (`Text`, `HMapSIW`). The mapper of the second job does two things: search for the marginal count term in the map, and then compute the relative frequency of each pair. The mapper emits (`MyPairOfStrings`, `FloatWritable`). Then the second stage reducer does exactly same thing as for the Pairs implementation. Combiner comes to help only for the first MR job, with both input and output pairs as of (`Text`, `HMapSIW`). It simply does intermediate combination of `HMapSIW`s.

## Question 1
The running time for the complete Pairs implementation is 328.762 seconds. The running time for the Stripes implementation is 180.355 seconds.

## Question 2
After disabling combiner, the running time for Pairs is 355.05 seconds and the running time for the Stripes implementation is 186.93 secongs. It seems that using combiner does not give us much improvement in this case...

## Question 3
Total number of distinct pairs: 116759

## Question 4 
The pair with highest frequency is (`meshach`, `abednego`), with PMI of `9.319931`. Meshach and Abednego, together with Shadrach are people recorded in the book of Daniel Chapters 1–3, known for their exclusive devotion to God. Actually  the pairs (`meshach`, `abednego`), (`meshach`, `shadrach`) and (`abednego`, `shadrach`) all have the tied PMI of `9.319931`. Therefore it's resonable to guess that these three names always appear together in the Bible.

## Question 5
The words having highest PMI with "cloud" are: `tabernacle` (PMI: 4.153025), `glory` (PMI: 3.3988752), `fire` (PMI: 3.2354724);

The words having highest PMI with "love" are: `hate` (PMI: 2.5755355), `hermia` (PMI: 2.0289917), `commandments` (PMI: 1.9395468)

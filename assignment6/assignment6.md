a = load 'user/shared/tweets2011/tweets2011.txt' as (id: chararray, time: chararray, tweet: chararray);
b = group a by (time);
c = foreach b generate FORMAT_DT('M/dd HH', DATE_TIME(b, "MMM dd HH:mm:ss")), COUNT(*) as count;
store c into 'qiwang321-all'
dump c;



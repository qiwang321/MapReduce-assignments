a = load '/user/shared/tweets2011/tweets2011.txt' as (id: chararray, time: chararray, user: chararray, tweet: chararray);
b = foreach a generate ToDate(time, 'EEE MMM dd HH:mm:ss Z yyyy') as time;
c = filter b by SecondsBetween(time, ToDate('Sun Jan 23 00:00:00 +0000 2011', 'EEE MMM dd HH:mm:ss Z yyyy')) >= 0L and SecondsBetween(time, ToDate('Tue Feb 08 23:59:59 +0000 2011', 'EEE MMM dd HH:mm:ss Z yyyy')) <= 0L;
d = group c by time;
e = foreach d generate group as time, COUNT(c) as count;
f = foreach e generate CONCAT(CONCAT((chararray)GetMonth(time), '/'), (chararray)GetDay(time)) as time, count;
store f into 'qiwang321-all-pig';



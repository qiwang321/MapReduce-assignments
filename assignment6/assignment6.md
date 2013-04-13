a = load '/user/shared/tweets2011/tweets2011.txt' as (id: chararray, time: datetime, user: chararray, tweet: chararray);
b = group a by (time);
c = foreach b generate group as time, COUNT(a) as count;
d = foreach c generate ToDate(time, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') as time;
e = filter d by SecondsBetween(time, ToDate('SUN JAN 23 00:00:00')) >= 0L and SecondsBetween(time, ToDate('SAT FEB 08 23:59:59 +0000 2011')) <= 0L

store c into 'qiwang321-all';
dump c;



DEFINE toToDate(userstring, format)

a = load '/user/shared/tweets2011/tweets2011.txt' as (id: chararray, time: chararray, user: chararray, tweet: chararray);
b = group a by (time);
c = foreach b generate group as time, COUNT(a) as count;
d = foreach c generate ToDate(time, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') as time, count as count;
e = filter d by SecondsBetween(time, ToDate('SUN JAN 23 00:00:00', 'EEE MMM dd HH:mm:ss ZZZZZ yyyy')) >= 0L and SecondsBetween(time, ToDate('SAT FEB 08 23:59:59 +0000 2011', 'EEE MMM dd HH:mm:ss ZZZZZ yyyy')) <= 0L;
f = foreach d generate CONCAT(CONCAT((chararray)GetMonth(time), '/'), (chararray)GetDay(time)) as time, count as count;


store f into 'qiwang321-pig-all';



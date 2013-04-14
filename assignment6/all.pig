a = load '/user/shared/tweets2011/tweets2011.txt' as (id: chararray, time: chararray, user: chararray, tweet: chararray);
b = foreach a generate SUBSTRING(time, 4, 7) as month, SUBSTRING(time, 8, 10) as date, SUBSTRING(time, 11, 13) as hour, SUBSTRING(time, 26, 30) as year;
c = filter b by (month == 'Jan' and date >= '23') or (month == 'Feb' and date <= '08');
d = filter c by year == '2011';
e = group d by (month, date, hour);
f = foreach e generate FLATTEN(group), COUNT(d) as count;
store f into 'qiwang321-all-pig';


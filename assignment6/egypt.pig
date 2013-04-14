a = load '/user/shared/tweets2011/tweets2011.txt' as (id: chararray, time: chararray, user: chararray, tweet: chararray);
b = filter a by (tweet matches '.*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*');
c = foreach b generate SUBSTRING(time, 4, 7) as month, SUBSTRING(time, 8, 10) as date, SUBSTRING(time, 11, 13) as hour, SUBSTRING(time, 26, 30) as year;
d = filter c by (month == 'Jan' and date >= '23') or (month == 'Feb' and date <= '08');
e = filter d by year == '2011';
f = group e by (month, date, hour);
g = foreach f generate FLATTEN(group), COUNT(e) as count;
store g into 'qiwang321-egypt-pig';


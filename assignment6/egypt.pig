-- Counting tweets that contain 'egypt'

a = load '/user/shared/tweets2011/tweets2011.txt' as (id: chararray, time: chararray, user: chararray, tweet: chararray);
b = foreach a generate SUBSTRING(time, 4, 7) as month, SUBSTRING(time, 8, 10) as date, SUBSTRING(time, 11, 13) as hour, SUBSTRING(time, 26, 30) as year, tweet;
c = filter b by (year == '2011') and (tweet matches '.*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*');
jan = filter c by month == 'Jan' and date >= '23';
feb = filter c by month == 'Feb' and date <= '08';
jang = group jan by (date, hour);
febg = group feb by (date, hour);
janc = foreach jang generate CONCAT('1/', group.date) as date, group.hour as hour, COUNT(jan) as count;
febc = foreach febg generate CONCAT('2/', group.date) as date, group.hour as hour, COUNT(feb) as count;
final = union janc, febc;
store final into 'qiwang321-egypt-pig';


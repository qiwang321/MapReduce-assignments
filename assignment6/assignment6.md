-- Counting all tweets

a = load '/user/shared/tweets2011/tweets2011.txt' as (id: chararray, time: chararray, user: chararray, tweet: chararray); <br>
b = foreach a generate SUBSTRING(time, 4, 7) as month, SUBSTRING(time, 8, 10) as date, SUBSTRING(time, 11, 13) as hour, SUBSTRING(time, 26, 30) as year; <br>
c = filter b by year == '2011'; <br>
jan = filter c by month == 'Jan' and date >= '23'; <br>
feb = filter c by month == 'Feb' and date <= '08'; <br>
jang = group jan by (date, hour); <br>
febg = group feb by (date, hour); <br>
janc = foreach jang generate CONCAT('1/', group.date) as date, group.hour as hour, COUNT(jan) as count; <br>
febc = foreach febg generate CONCAT('2/', group.date) as date, group.hour as hour, COUNT(feb) as count; <br>
final = union janc, febc; <br>

store final into 'qiwang321-all-pig'; <br>

-- Counting tweets that contain Egypt or Cairo 

a = load '/user/shared/tweets2011/tweets2011.txt' as (id: chararray, time: chararray, user: chararray, tweet: chararray); <br>
b = foreach a generate SUBSTRING(time, 4, 7) as month, SUBSTRING(time, 8, 10) as date, SUBSTRING(time, 11, 13) as hour, SUBSTRING(time, 26, 30) as year, tweet; <br>
c = filter b by (year == '2011') and (tweet matches '.*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*'); <br>
jan = filter c by month == 'Jan' and date >= '23'; <br>
feb = filter c by month == 'Feb' and date <= '08'; <br>
jang = group jan by (date, hour); <br>
febg = group feb by (date, hour); <br>
janc = foreach jang generate CONCAT('1/', group.date) as date, group.hour as hour, COUNT(jan) as count; <br>
febc = foreach febg generate CONCAT('2/', group.date) as date, group.hour as hour, COUNT(feb) as count; <br>
final = union janc, febc; <br>

store final into 'qiwang321-egypt-pig';



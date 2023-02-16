/*
QUESTIONS

1. What are the most watched TV shows and Movies. Sorted by total watch time, seperated by type.
2. Which viewer IDs watched the most amount of video content by total watch time and total unique videos watched.
3. Which country's users (use country name) have the highest total watch time across all videos?
4. Does the total number of unique viewers impact the total watch time of a video?
5. What is the average watch time for each video?
6. What are the most popular genres by watch time? And total number of videos listed in each genre?

*/





--#1 What are the most watched TV shows and Movies. Sorted by total watch time, seperated by type.

with watch_time as (
	select video_id,
	sum(watch_time_minutes) as total_watch_time
	from views
	group by video_id
), top_videos_per_type as (
	select v.type, v.title, w.total_watch_time,
	rank() over (partition by v.type order by w.total_watch_time desc) as rn
	from videos v
	join watch_time w on w.video_id = v.video_id
)

select type,title,total_watch_time 
from top_videos_per_type 
where rn <= 10

--Here are the top 10 movies and top 10 TV shows by total watch time. 
--The top watched movie is Truckbhar Swapna with 269 minutes of total watch time, and the top watched TV show is The Legend of White Snake with 227 minutes of total watch time.



--#2 Which viewer IDs watched the most amount of video content by total watch time and total unique videos watched.

select top 10 viewer_id,
sum(watch_time_minutes) as total_watch_time,
count(distinct video_id) total_unique_videos_watched
from views 
group by viewer_id
order by total_watch_time desc

select top 10 viewer_id,
sum(watch_time_minutes) as total_watch_time,
count(distinct video_id) total_unique_videos_watched
from views 
group by viewer_id
order by total_unique_videos_watched desc

--The first query tells us the viewers with the most amount of total watch time, and the second tells us teh viewers with the most unique videos watched. 
--The answer is viewer id 890 with 803 minutes of total watch time, and viewer id 295 with 29 unique videos watched, respectively.



--#3 Which country's users (use country name) have the highest total watch time across all videos?

select top 10 c.country,
sum(v.watch_time_minutes) as total_watch_time
from views v
join viewers vr on vr.viewer_id = v.viewer_id
join countries c on c.country_id = vr.country_id
group by c.country
order by total_watch_time desc

--Here are the top 10 countries with most watch time from their viewers, with Cameroon being the top with 6,319 minutes of total watch time.



--#4 Does the total number of unique viewers impact the total watch time of a video?

with unique_viewers_per_video as (
	select d.video_id,
	count(distinct v.viewer_id) as num_unique_viewers,
	sum(v.watch_time_minutes) as total_watch_time
	from videos d
	join views v on v.video_id = d.video_id
	group by d.video_id
)

select num_unique_viewers,
avg(total_watch_time) as average_total_watch_time
from unique_viewers_per_video
group by num_unique_viewers
order by num_unique_viewers desc


--Yes, we can see there is a positive linear correlation between the number of unique viewers and the average total watch time.



--#5 What is the average watch time for each video?

select d.video_id,
avg(v.watch_time_minutes) as average_watch_time
from views v
join videos d on d.video_id = v.video_id
group by d.video_id
order by average_watch_time desc

select avg(watch_time_minutes) as average_watch_time
from views

--The first query provides a list of all videos with their average watch time, which ranges from 0 to 50 minutes.
--The second query tells us that the average watch time for *any* video is 24 minutes.


--#6 What are the most popular genres by watch time? And total number of videos listed in each genre?

with watch_time as (
	select video_id,
	sum(watch_time_minutes) as total_watch_time
	from views
	group by video_id
)

select top 10 trim(ss.value) as genre,
sum(w.total_watch_time) as total_watch_time
from videos d
join watch_time w on w.video_id = d.video_id
cross apply string_split (d.genre,',') ss
group by trim(ss.value)
order by total_watch_time desc

select trim(ss.value) as genre,
count(distinct d.video_id) num_videos
from videos d
cross apply string_split (d.genre,',') ss
group by trim(ss.value)
order by num_videos desc

--The first query provides the top 10 most popular genres by total watch time. The top genre watched is International Movies with 125,630 minutes of watch time.
--The second query provides the number of videos listed in each genre.
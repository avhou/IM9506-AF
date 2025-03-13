cat reddit*json | jq --sort-keys '.[] | . as $nested_objects | $nested_objects' > reddit.json
# create table reddit as select distinct * from read_json_auto('reddit.json', maximum_object_size=1000000000);
# drop table if exists expanded_comments;
# create table expanded_comments as select id, comment->>'body' as body from (select id, unnest(comments) as comment from reddit);
# delete from expanded_comments where body = '[deleted]' or body = '[removed]';
# drop table if exists aggregated_comments;
# create table aggregated_comments as select id, regexp_replace(string_agg(body, ' '), '\\s+', ' ') as comments from expanded_comments group by id;
# drop table if exists texts;
# create table texts as select r.id, r.subreddit, to_timestamp(created_utc) as created, case when r.title in ('[deleted]', '[removed]') then '' else regexp_replace(r.title, '\\s+', ' ') end as title, case when r.selftext in ('[deleted]', '[removed]') then '' else regexp_replace(r.selftext, '\\s+', ' ') end as selftext, c.comments from reddit r left outer join aggregated_comments c on r.id = c.id;
# copy (select id, subreddit, created, trim(coalesce(title, '') || ' ' || coalesce(selftext, '') || ' ' || coalesce(comments, '')) as text from texts) to 'reddit.csv';
# attach 'reddit.sqlite' as output (TYPE sqlite);
# create table output.reddit_hits as select id, subreddit, created, trim(coalesce(title, '') || ' ' || coalesce(selftext, '') || ' ' || coalesce(comments, '')) as text from texts;

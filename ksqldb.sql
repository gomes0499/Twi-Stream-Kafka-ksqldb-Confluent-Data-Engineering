-- Create Stream
CREATE STREAM tweet_stream
  (username VARCHAR, tweet VARCHAR, date DATE)
  WITH (KAFKA_TOPIC='kafka-stream-tweettweets', VALUE_FORMAT='JSON');


SELECT * FROM tweet_stream EMIT CHANGES;

-- Create Table 
CREATE TABLE tweet_counts AS
SELECT username, COUNT(*) 
FROM tweet_stream
GROUP BY username 
EMIT CHANGES;


SELECT * FROM tweet_counts EMIT CHANGES;

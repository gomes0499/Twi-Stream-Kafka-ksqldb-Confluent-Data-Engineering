-- Create Stream table
CREATE STREAM tweet_stream
  (username VARCHAR, tweet VARCHAR, date DATE)
  WITH (KAFKA_TOPIC='kafka-stream-tweettweets', VALUE_FORMAT='JSON');

-- Create new table and topic for sink using sql aggregation functions
CREATE TABLE TWEET_COUNTS_TABLE WITH (KAFKA_TOPIC='pksqlc-x80jkTWEET_COUNTS_TABLE', PARTITIONS=1, REPLICAS=3) AS 
SELECT 
    USERNAME, 
    COUNT(*) 
FROM 
    TWEET_STREAM 
GROUP BY 
    USERNAME 
EMIT CHANGES;

SELECT * FROM TWEET_COUNTS_TABLE EMIT CHANGES;
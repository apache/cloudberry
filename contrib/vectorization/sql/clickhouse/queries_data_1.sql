-- set on vectorization

SET vector.enable_vectorization=on;

SET vector.max_batch_size=128;

-- query 1 - 10

SELECT count() FROM hits;

SELECT count() FROM hits WHERE AdvEngineID != 0::smallint;

SELECT sum(AdvEngineID), count() FROM hits;

SELECT sum(UserID) FROM hits;

SELECT count(distinct UserID) FROM hits;

SELECT count(distinct SearchPhrase) FROM hits;

SELECT min(EventDate), max(EventDate) FROM hits;

SELECT AdvEngineID, count() FROM hits WHERE AdvEngineID != 0::smallint GROUP BY AdvEngineID ORDER BY count();

SELECT RegionID, count(distinct UserID) AS u FROM hits GROUP BY RegionID ORDER BY regionid, u LIMIT 10;

SELECT RegionID, sum(AdvEngineID), count() AS c, count(distinct UserID) FROM hits GROUP BY RegionID ORDER BY regionid, c LIMIT 10;
SELECT 1 FROM oag.venues

--Q1
--Q2 
--Q3 
--Q4 
--Q5
SELECT DEDUP * FROM oag.venues WHERE DisplayName LIKE '%National%' OR DisplayName LIKE '%International%'
SELECT DEDUP * FROM oag.venues WHERE DisplayName LIKE '%Journal%'
SELECT DEDUP * FROM oag.venues WHERE DisplayName LIKE '%Journal%' OR DisplayName LIKE '%International%' OR DisplayName LIKE '%National%'
SELECT DEDUP * FROM oag.venues WHERE DisplayName LIKE '%Journal%' OR DisplayName LIKE '%International%' OR DisplayName LIKE '%National%' OR DisplayName LIKE '%Proceedings%' OR DisplayName LIKE '%computational%'  OR DisplayName LIKE '%Health%'  OR DisplayName LIKE '%Studies%' OR DisplayName LIKE '%Review%' OR DisplayName LIKE '%European%'
SELECT DEDUP * FROM oag.venues WHERE DisplayName LIKE '%Journal%' OR DisplayName LIKE '%International%' OR DisplayName LIKE '%National%' OR DisplayName LIKE '%Proceedings%' OR DisplayName LIKE '%of%' OR DisplayName LIKE '%and%' 

--Q6b
SELECT DEDUP * FROM oag.papers2m INNER JOIN oag.venues ON venue_id = venues.orig_id WHERE year > 2012

--Q7b
SELECT DEDUP * FROM oag.papers2m INNER JOIN oag.venues ON venue_id = venues.orig_id WHERE year > 1995

--Q8b
SELECT DEDUP * FROM oag.papers200k INNER JOIN oag.venues ON venue_id = venues.orig_id WHERE year > 2008
SELECT DEDUP * FROM oag.papers500k INNER JOIN oag.venues ON venue_id = venues.orig_id WHERE year > 2008
SELECT DEDUP * FROM oag.papers1m INNER JOIN oag.venues ON venue_id = venues.orig_id WHERE year > 2008
SELECT DEDUP * FROM oag.papers1m500k INNER JOIN oag.venues ON venue_id = venues.orig_id WHERE year > 2008
SELECT DEDUP * FROM oag.papers2m INNER JOIN oag.venues ON venue_id = venues.orig_id WHERE year > 2008


-- SELECT DEDUP * FROM oag.papers2m INNER JOIN oag.venues ON venue_id = venues.orig_id  WHERE year > 2012
-- SELECT DEDUP * FROM oag.papers2m INNER JOIN oag.venues ON venue_id = venues.orig_id  WHERE year > 2008
-- SELECT DEDUP * FROM oag.papers2m INNER JOIN oag.venues ON venue_id = venues.orig_id  WHERE year > 2003
-- SELECT DEDUP * FROM oag.papers2m INNER JOIN oag.venues ON venue_id = venues.orig_id  WHERE year > 1995
-- SELECT DEDUP * FROM oag.papers2m INNER JOIN oag.venues ON venue_id = venues.orig_id  WHERE year > 1990



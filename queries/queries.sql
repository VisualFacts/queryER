
-- QMOD
SELECT DEDUP * FROM oag.papers200k  WHERE MOD(index, 5) < 1
SELECT DEDUP * FROM oag.papers500k  WHERE MOD(index, 10) < 1
SELECT DEDUP * FROM oag.papers1m WHERE MOD(index, 20) < 1
SELECT DEDUP * FROM oag.papers1m500k WHERE MOD(id, 30) < 1
SELECT DEDUP * FROM oag.papers2m WHERE MOD(id, 40) < 1


-- LI
SELECT DEDUP * FROM oag.papers2m WHERE year > 2008 AND year < 2012
SELECT DEDUP * FROM oag.papers2m WHERE year > 2004 AND year < 2012
SELECT DEDUP * FROM oag.papers2m WHERE year > 2000 AND year < 2012
SELECT DEDUP * FROM oag.papers2m WHERE year > 1994 AND year < 2012




-- PPL200K-2M >< OAO
SELECT DEDUP * FROM q.people200k INNER JOIN q.organisations ON name = organisation WHERE state = 'vic' 
SELECT DEDUP * FROM q.people500k INNER JOIN q.organisations ON name = organisation WHERE state = 'vic' 
SELECT DEDUP * FROM q.people1m INNER JOIN q.organisations ON name = organisation WHERE state = 'vic' 
SELECT DEDUP * FROM q.people1m5k INNER JOIN q.organisations ON name = organisation WHERE state = 'vic' 
SELECT DEDUP * FROM q.people2m INNER JOIN q.organisations ON name = organisation WHERE state = 'vic' 

-- OAGP200K-2M >< OAGV
SELECT DEDUP * FROM q.papers200k INNER JOIN q.venues ON venue_id = venues.orig_id WHERE year > 2008
SELECT DEDUP * FROM q.papers500k INNER JOIN q.venues ON venue_id = venues.orig_id WHERE year > 2008
SELECT DEDUP * FROM q.papers1m INNER JOIN q.venues ON venue_id = venues.orig_id WHERE year > 2008
SELECT DEDUP * FROM q.papers1m500k INNER JOIN q.venues ON venue_id = venues.orig_id WHERE year > 2008
SELECT DEDUP * FROM q.papers2m INNER JOIN q.venues ON venue_id = venues.orig_id WHERE year > 2008

SELECT 1 FROM oag.papers200k

--Q1 
--Q2 
--Q3 
--Q4 
--Q5 


SELECT DEDUP * FROM oag.papers200k WHERE year > 2012
SELECT DEDUP * FROM oag.papers200k WHERE year > 2008
SELECT DEDUP * FROM oag.papers200k WHERE year > 2003
SELECT DEDUP * FROM oag.papers200k WHERE year > 1995
SELECT DEDUP * FROM oag.papers200k WHERE year > 1990

SELECT DEDUP * FROM oag.papers500k WHERE year > 2012
SELECT DEDUP * FROM oag.papers500k WHERE year > 2008
SELECT DEDUP * FROM oag.papers500k WHERE year > 2003
SELECT DEDUP * FROM oag.papers500k WHERE year > 1995
SELECT DEDUP * FROM oag.papers500k WHERE year > 1990

SELECT DEDUP * FROM oag.papers1m WHERE year > 2012
SELECT DEDUP * FROM oag.papers1m WHERE year > 2008
SELECT DEDUP * FROM oag.papers1m WHERE year > 2003
SELECT DEDUP * FROM oag.papers1m WHERE year > 1995
SELECT DEDUP * FROM oag.papers1m WHERE year > 1990

SELECT DEDUP * FROM oag.papers1m500k WHERE year > 2012
SELECT DEDUP * FROM oag.papers1m500k WHERE year > 2008
SELECT DEDUP * FROM oag.papers1m500k WHERE year > 2003
SELECT DEDUP * FROM oag.papers1m500k WHERE year > 1995
SELECT DEDUP * FROM oag.papers1m500k WHERE year > 1990

SELECT DEDUP * FROM oag.papers2m WHERE year > 2012
SELECT DEDUP * FROM oag.papers2m WHERE year > 2008
SELECT DEDUP * FROM oag.papers2m WHERE year > 2003
SELECT DEDUP * FROM oag.papers2m WHERE year > 1995
SELECT DEDUP * FROM oag.papers2m WHERE year > 1990

SELECT DEDUP * FROM oag.papers5m WHERE year > 2012
SELECT DEDUP * FROM oag.papers5m WHERE year > 2008
SELECT DEDUP * FROM oag.papers5m WHERE year > 2003
SELECT DEDUP * FROM oag.papers5m WHERE year > 1995
SELECT DEDUP * FROM oag.papers5m WHERE year > 1990

--Q9 fixed selectivity, increasing dataset size

SELECT DEDUP * FROM oag.papers200k WHERE MOD(id, 4) < 1
SELECT DEDUP * FROM oag.papers200k WHERE MOD(id, 10) < 1
SELECT DEDUP * FROM oag.papers1m WHERE MOD(id, 20) < 1
SELECT DEDUP * FROM oag.papers1m500k WHERE MOD(id, 30) < 1
SELECT DEDUP * FROM oag.papers2m WHERE MOD(id, 40) < 1


--Q10-Q13 Effects of LI, run this with links enabled, 

SELECT DEDUP * FROM oag.papers2m WHERE year > 2008 AND year < 2012
SELECT DEDUP * FROM oag.papers2m WHERE year > 2004 AND year < 2012
SELECT DEDUP * FROM oag.papers2m WHERE year > 2000 AND year < 2012
SELECT DEDUP * FROM oag.papers2m WHERE year > 1994 AND year < 2012



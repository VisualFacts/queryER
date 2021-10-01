SELECT 1 FROM dsd.publications

--Q1
--Q2 
--Q3 
--Q4 
--Q5

SELECT DEDUP * FROM dsd.publications WHERE venue = 'VLDB'
SELECT DEDUP * FROM dsd.publications WHERE authors LIKE 'A%'
SELECT DEDUP * FROM dsd.publications WHERE authors LIKE 'A%' OR venue LIKE 'P%'
SELECT DEDUP * FROM dsd.publications WHERE year > 1980 OR title LIKE 'A%' OR title LIKE 'P%' OR title LIKE 'T%' 
SELECT DEDUP * FROM dsd.publications WHERE authors LIKE '%a%'
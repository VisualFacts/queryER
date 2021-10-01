SELECT 1 FROM synthetic.people200k

--Q1
--Q2 
--Q3 
--Q4 
--Q5
SELECT DEDUP * FROM synthetic.projects WHERE funder LIKE '%European%'
SELECT DEDUP * FROM synthetic.projects WHERE fundingLevel1 LIKE '%Behavioral%' OR title LIKE '%Collaborative%'
SELECT DEDUP * FROM synthetic.projects WHERE fundingLevel0 LIKE '%Engineering%' OR fundingLevel0 LIKE '%Biological%' OR fundingLevel0 LIKE '%Education'
SELECT DEDUP * FROM synthetic.projects WHERE fundingLevel0 LIKE '%Directorate%'
SELECT DEDUP * FROM synthetic.projects WHERE funder LIKE '%National%' 

--Q1
--Q2 
--Q3 
--Q4 
--Q5
SELECT DEDUP * FROM synthetic.people200k WHERE state = 'vic'
SELECT DEDUP * FROM synthetic.people200k WHERE state = 'nsw'
SELECT DEDUP * FROM synthetic.people200k WHERE state = 'vic' OR state = 'nsw'
SELECT DEDUP * FROM synthetic.people200k WHERE age > 27
SELECT DEDUP * FROM synthetic.people200k WHERE age > 19
SELECT DEDUP * FROM synthetic.people200k 
SELECT DEDUP * FROM synthetic.people500k WHERE state = 'vic'
SELECT DEDUP * FROM synthetic.people500k WHERE state = 'nsw'
SELECT DEDUP * FROM synthetic.people500k WHERE state = 'vic' OR state = 'nsw'
SELECT DEDUP * FROM synthetic.people500k WHERE age > 27
SELECT DEDUP * FROM synthetic.people500k WHERE age > 19
SELECT DEDUP * FROM synthetic.people500k
SELECT DEDUP * FROM synthetic.people1m WHERE state = 'vic'
SELECT DEDUP * FROM synthetic.people1m WHERE state = 'nsw'
SELECT DEDUP * FROM synthetic.people1m WHERE state = 'vic' OR state = 'nsw'
SELECT DEDUP * FROM synthetic.people1m WHERE age > 27
SELECT DEDUP * FROM synthetic.people1m WHERE age > 19
SELECT DEDUP * FROM synthetic.people1m
SELECT DEDUP * FROM synthetic.people1m5k WHERE state = 'vic'
SELECT DEDUP * FROM synthetic.people1m5k WHERE state = 'nsw'
SELECT DEDUP * FROM synthetic.people1m5k WHERE state = 'vic' OR state = 'nsw'
SELECT DEDUP * FROM synthetic.people1m5k WHERE age > 27
SELECT DEDUP * FROM synthetic.people1m5k WHERE age > 19
SELECT DEDUP * FROM synthetic.people1m5k 
SELECT DEDUP * FROM synthetic.people2m WHERE state = 'vic'
SELECT DEDUP * FROM synthetic.people2m WHERE state = 'nsw'
SELECT DEDUP * FROM synthetic.people2m WHERE state = 'vic' OR state = 'nsw'
SELECT DEDUP * FROM synthetic.people2m WHERE age > 27
SELECT DEDUP * FROM synthetic.people2m WHERE age > 19
SELECT DEDUP * FROM synthetic.people2m 

-- Q6a
SELECT DEDUP * FROM synthetic.people200k INNER JOIN synthetic.organisations ON name = organisation WHERE state = 'vic' AND organisations.name LIKE '%European%'

-- Q7a
SELECT DEDUP * FROM synthetic.projects INNER JOIN synthetic.organisations ON name = organisation WHERE fundingLevel0 LIKE '%Directorate%'

-- Q8a
SELECT DEDUP * FROM synthetic.people200k INNER JOIN synthetic.organisations ON name = organisation WHERE state = 'vic' 
SELECT DEDUP * FROM synthetic.people500k INNER JOIN synthetic.organisations ON name = organisation WHERE state = 'vic' 
SELECT DEDUP * FROM synthetic.people1m INNER JOIN synthetic.organisations ON name = organisation WHERE state = 'vic' 
SELECT DEDUP * FROM synthetic.people1m5k INNER JOIN synthetic.organisations ON name = organisation WHERE state = 'vic' 
SELECT DEDUP * FROM synthetic.people2m INNER JOIN synthetic.organisations ON name = organisation WHERE state = 'vic' 

--Q9 fixed selectivity, increasing dataset size
SELECT DEDUP * FROM synthetic.people200k WHERE MOD(rec_id, 10) < 1 
SELECT DEDUP * FROM synthetic.people500k WHERE MOD(rec_id, 10) < 1 
SELECT DEDUP * FROM synthetic.people1m WHERE MOD(rec_id, 10) < 1 
SELECT DEDUP * FROM synthetic.peopl1m5k WHERE MOD(rec_id, 10) < 1 
SELECT DEDUP * FROM synthetic.people2m WHERE MOD(rec_id, 10) < 1 


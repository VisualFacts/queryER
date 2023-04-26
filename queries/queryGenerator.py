import random

schema_name = "q"
tables = ["publications", "projects", "organisations", "people200k",  "people500k",  "people1m",  "people1m500k", "people2m",
	"papers200k", "papers500k", "papers1m", "papers1m500k", "papers2m"]


joins = [["projects", "organisations"], ["people200k", "organisations"],["people500k", "organisations"], ["people1m", "organisations"], ["people1m500k", "organisations"], ["people2m", "organisations"], 
 ["papers200k", "venues"],  ["papers500k", "venues"],  ["papers1m", "venues"],  ["papers1m500k", "venues"],  ["papers2m", "venues"]]

years = ['1992','1993','1994','1995','1996','1997','1998','1999','2000','2001','2002','2003','2004','2005','2006','2007','2008', '2009','2010','2011','2012','2013']
venues = ['%Proceedings%', '%proceedings%', '%acm%', '%ACM%', '%international%', '%International%', '%Conference%']

states = ['vic', 'nsw', 'qld', 'now', 'wa']
organisations = ["%Foundation%", "%Fundacia%", "%TECNLOGIA%", "%Technological%", "%University%", '%Company%', '%Universitat%', '%University%', '%Department%']

funders = ['%National%', '%European%', '%Science%', '%Foundation%', '%Comission%']
funders = ['%National%', '%European%', '%Science%', '%Foundation%', '%Comission%']

funding_levels = ['%Fellowship%', '%National%', '%Directorate%', '%Foundation%', '%Information%', '%Educational%', '%Division%', '%Undergraduate%']

countries = ["%France%", '%Spain%', "%Netherlands%", "%Israel%", "%Sweded%", "%Spain%", "%IE", "%Greece%", "%Germany"]

ages = [20, 30, 40, 50, 60]

def get_sp_query(table, opeartor = "OR"):
	if table == "publications":
		venue = random.choice(venues)
		year = random.choice(years)
		return f"SELECT DEDUP * FROM {schema_name}.{table} WHERE year = {int(year)} {opeartor} venue LIKE '{venue}'"
	elif table == "projects":
		funder = random.choice(funders)
		funding_level = random.choice(funding_levels)
		return f"SELECT DEDUP * FROM {schema_name}.{table} WHERE funder LIKE '{funder}' {opeartor} fundingLevel0 LIKE '{funding_level}'"
	if table == "organisations":
		name = random.choice(organisations)
		country = random.choice(countries)
		return f"SELECT DEDUP * FROM {schema_name}.{table} WHERE name LIKE '{name}' {opeartor} country LIKE '{country}'"
	if "people" in table:
		age = random.choice(ages)
		state = random.choice(states)
		return f"SELECT DEDUP * FROM {schema_name}.{table} WHERE age > {age} {opeartor} state LIKE '{state}'"
	if "papers" in table:
		venue = random.choice(venues)
		year = random.choice(years)
		return f"SELECT DEDUP * FROM {schema_name}.{table} WHERE year = {int(year)} {opeartor} venue_name LIKE '{venue}'"

def get_spj_query(join, opeartor = "OR"):
	table1 = join[0]
	table2 = join[1]
	if table1 == "projects":
		funder = random.choice(funders)
		funding_level = random.choice(funding_levels)
		return f"SELECT DEDUP * FROM {schema_name}.{table1} INNER JOIN {schema_name}.{table2} ON {table1}.funder = {table2}.name WHERE funder LIKE '{funder}' {opeartor} fundingLevel0 LIKE '{funding_level}'"
	if "people" in table1:
		age = random.choice(ages)
		state = random.choice(states)
		return f"SELECT DEDUP * FROM {schema_name}.{table1} INNER JOIN {schema_name}.{table2} ON {table1}.organisation = {table2}.name  WHERE age > {age} {opeartor} state LIKE '{state}'"
	if "papers" in table:
		venue = random.choice(venues)
		year = random.choice(years)
		return f"SELECT DEDUP * FROM {schema_name}.{table1} INNER JOIN {schema_name}.{table2} ON {table1}.venue_name = {table2}.DisplayName WHERE year = {int(year)} {opeartor} venue_name LIKE '{venue}'"

for table in tables:
	f = open(f"{table}.sql", "w")
	for i in range(50):
		f.write(get_sp_query(table))
		f.write('\n')
	for i in range(50):
		f.write(get_sp_query(table, "AND"))
		f.write('\n')
	f.close


for join in joins:
	table1 = join[0]
	table2 = join[1]
	f = open(f"{table1}-{table2}.sql", "w")
	for i in range(50):
		f.write(get_spj_query(join))
		f.write('\n')
	for i in range(50):
		f.write(get_spj_query(join, "AND"))
		f.write('\n')
	f.close()

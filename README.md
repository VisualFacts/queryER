## Usage


### Configuration
In the default configuration the framework asks the user for queries, and does not need any further configuration.  The framework is configured through the **config.properties** file which has to be provided with the following properties:<br>
**schema.name**:{String} The schema for the queries. (Default = real, Choises = real/synthetic)<br>
**calcite.connection**:{String} The default calcite connection, points to model.json.<br>
**model.json**: Contains the schema, one database that points to the real data and one that points to the synthetic. **The datapath needs to be changed**<br>

For debugging and experimental purposes, the following can also be provided.<br>
**query.runs**{Integer}<br/>
**query.filepath=resources/tests/test_synth.sql** {Path to an sql file, if not provided the framework will ask the user for a query input}<br/>
**ground_truth.calculate**:{Boolean} Whether to calculate Ground Truth and find the Pair Completeness. Used only for SP queries.<br/>

### Queries
The queries that were used for the experimental evaluation can be found in the queries.sql file.


### Datasets
The used datasets can be found <a href="https://imisathena-my.sharepoint.com/personal/gpapas_imis_athena-innovation_gr/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fgpapas%5Fimis%5Fathena%2Dinnovation%5Fgr%2FDocuments%2FVisualFacts%2FImplementation%2FWP2%2FQuery%20ER%2Fdata&originalPath=aHR0cHM6Ly9pbWlzYXRoZW5hLW15LnNoYXJlcG9pbnQuY29tLzpmOi9nL3BlcnNvbmFsL2dwYXBhc19pbWlzX2F0aGVuYS1pbm5vdmF0aW9uX2dyL0VtSUJUNTJkcE5CRnF5bElEeEtZdXZVQnNPZ093RUp5dW9TS2lQUkdHQWppRGc_cnRpbWU9Vks0SzJpVjAyRWc">here</a> and need to be downloaded and placed on the resources folder so that model.json can access them. For the creation of the synthetic datasets modified versions of <a href="http://users.cecs.anu.edu.au/~Peter.Christen/Febrl/febrl-0.3/febrldoc-0.3/node70.html">febrl's data generator</a> were used, which can be found at the febrl folder.

### Run
To create the jar file, run: **`mvn package`**
To run the jar file, copy and paste the following command to the console:
java -jar target/queryER-API-0.0.1-SNAPSHOT-runnable.war

### Results
The query results are dumped to a csv file located at "./data/queryResults.csv"

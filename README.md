## Usage


### Configuration
In the default configuration the framework asks the user for queries, and does not need any further configuration.  The framework is configured through the **config.properties** file which has to be provided with the following properties:<br>
**schema.name**:{String} The schema for the queries. [synthetic, oag, dsd] <br>
**dump.path**:{String} Path to where the dump files of the framework will be stored. This includes, indices and statistics.
**calcite.connection**:{String} The default calcite connection, points to model.json. If not provided, the default model.json from resources will be loaded<br>
**model.json**: Contains the schema, one database that points to the data. The name of the schema will be one of: [synthetic, oag, dsd] depending on the data the user wants to deduplicate. **The datapath needs to be changed**<br>

For debugging and experimental purposes, the following can also be provided.<br>
**query.runs**{Integer}<br/>
**query.filepath=resources/tests/test_synth.sql** {Path to an sql file, if not provided the framework will ask the user for a query input}<br/>
**ground_truth.calculate**:{Boolean} Whether to calculate Ground Truth and find the Pair Completeness. Used only for SP queries. In case of ground_truth calculation the ground_truth data must be on the data folder.<br/>

### Queries
The queries that were used for the experimental evaluation can be found in the queries folder.


### Datasets
The used datasets can be found <a href="https://imisathena-my.sharepoint.com/personal/gpapas_imis_athena-innovation_gr/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fgpapas%5Fimis%5Fathena%2Dinnovation%5Fgr%2FDocuments%2FVisualFacts%2FImplementation%2FWP2%2FQuery%20ER%2Fdata&originalPath=aHR0cHM6Ly9pbWlzYXRoZW5hLW15LnNoYXJlcG9pbnQuY29tLzpmOi9nL3BlcnNvbmFsL2dwYXBhc19pbWlzX2F0aGVuYS1pbm5vdmF0aW9uX2dyL0VtSUJUNTJkcE5CRnF5bElEeEtZdXZVQnNPZ093RUp5dW9TS2lQUkdHQWppRGc_cnRpbWU9Vks0SzJpVjAyRWc">here</a> and need to be downloaded and placed on the folder defined by model.json, ex. "home/user/dsd". 

### Run
To create the jar file, run: **`mvn package`**
To run the jar file, copy and paste the following command to the console:
java -jar ./target/queryER-API-2.0.0-runnable.jar

### Results
The query results are dumped to a csv file located at ".queryResults.csv"

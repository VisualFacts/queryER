
# QueryER: A Framework for Fast Analysis-Aware Deduplication over Dirty Data
Giorgos Alexiou, George Papastefanatos, Vasilis Stamatopoulos, Georgia Koutrika, and Nectarios Koziris. QueryER: A Framework for Fast Analysis-Aware Deduplication over Dirty Data.

Code written in Java 8.

## Usage


### Configuration
In the default configuration the framework asks the user for queries, and does not need any further configuration.  The framework is configured through the **config.properties** file which has to be provided with the following properties:<br><br>
<ul>
<li><b>schema.name</b>: {String} The schema of the database. The name of the schema can be anything the user wants, default = q </li> 
<li><b>dump.path</b>: {String} Path to where the dump files of the framework will be stored. This includes, indices and statistics.</li>
<li><b>calcite.connection</b>: {String} The default calcite connection, points to model.json. If not provided, the default model.json from resources will be loaded</li>
<li><b> model.json</b>: This file contains the schema and the table paths of the database. The name must be the same as "schema.name". <b>The datapath needs to be changed to correspond to the ABSOLUTE PATH of the folder containing the data the user wants to query.</b>li>
</ul>

For debugging and experimental purposes, the following can also be provided.<br>
<ul>
<li><b>query.runs:</b> {Integer}</li>
<li><b>query.filepath:</b> {String} Path to an sql file, if not provided the framework will ask the user for a query input</li>
<li><b>ground_truth.calculate:</b> {Boolean} Whether to calculate Ground Truth and find the Pair Completeness. Used only for SP queries. 
<b>BEWARE: </b>In case of ground_truth calculation the ground_truth data must be on the same folder as the data files.</li>
</ul>

### Queries
In the queries folder, the user can find the python script that generates the queries. 
The static queries that were used for the experimental evaluation, like MOD queries are found in queries.sql.

### Datasets
The used datasets can be found <a href="https://imisathena-my.sharepoint.com/:f:/g/personal/bstam_athenarc_gr/EpNmNCfR_TBHjsQ2RES41noBQ_tMLB0YWmIgFxC3dP6M3Q?e=vk7Ezx">here</a>. Depending on the dataset the user wants to test, model.json must point to the specific folder, ex. "home/user/dsd". 

The table below summarizes the characteristics of the selected datasets: &#124;E&#124;:number of records in a dataset, &#124;L<sub>E</sub>&#124;:number of duplicates, &#124;A&#124;: number of attributes, &#124;TBI&#124;: number of blocks in TBI

| E | &#124;E&#124; | &#124;L<sub>E</sub>&#124; | &#124;A&#124; | &#124;TBI&#124;|
| --- | --- | --- | --- | --- |
|DSD     | 66879| 5347   | 4  |88K|
|OAO     | 55464| 5464   | 3  | 22K|
|OAP     | 500K | 58074  | 8  | 170K|
|PPL200K | 200K | 64762  | 12 | 160K|
|PPL500K | 500K | 161443 | 12 | 280K|
|PPL1M   | 1M   | 322722 | 12 | 470K|
|PPL1.5M | 1.5M | 403417 | 12 | 590K|
|PPL2M   | 2M   | 645489 | 12 | 850K|
|OAGP200K| 200K | 5679   | 18 | 110K|
|OAGP500K| 500K | 54132  | 18 | 180K|
|OAGP1M  | 1M   | 78341  | 18 | 240K|
|OAGP1.5M| 1.5M | 135313 | 18 | 320K|
|OAGP2M  | 2M   | 267843 | 18 | 360K|
|OAGV    | 130K | 29841  | 5  | 55K|




### Run
To create the jar file, run: **`mvn package`**
To run the jar file, copy and paste the following command to the console:
java -jar ./target/queryER-API-2.0.0-runnable.jar

### Results
The query results are dumped to a csv file located at "./data/queryResults.csv"

### Online Tool
A working demo of queryER can be found online [here](http://83.212.72.69:9000)

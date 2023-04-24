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
**ground_truth.calculate**:{Boolean} Whether to calculate Ground Truth and find the Pair Completeness. Used only for SP queries. 
<b>BEWARE:</b>In case of ground_truth calculation the ground_truth data must be on the same folder as the data files.<br/>

### Queries
The queries that were used for the experimental evaluation can be found in the queries folder.

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

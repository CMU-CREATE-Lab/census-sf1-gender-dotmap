# Census Summary 1 Files scripts

A collection of scripts to import Census data into PostgreSQL and a sample script used to generate the Gender Dot Map.

## Directory layout

The scripts are organized into three main groups: Python scripts to import the SF1 files from the Census zip files, SQL scripts to create the tables in Postgres, and the Python scripts to generate the gender dot map.

### SQL files
The ```sql``` folder contains the SQL statements to create the tables to store the 2000 and 2010 Census SF1 data. 

### Scripts to import SF1 data
The data_import folder contains the scripts to import the 2000 and 2010 into their corresponding tables.

### Gender Dot Map scripts
The logic to generate follows the approach of creating intermediate CSV files and then generating tiles used in the [tilling-helloworld](https://github.com/CMU-CREATE-Lab/tiling-helloworld) tutorial.

```generate_state_csv.py``` runs an SQL query and exports the results to a CSV file that contains the whole list of dots. In this case, a dot per person color-coded by sex.  This script uses celery to run a task for each state. The reason for dividing the task by states is to make debugging easier but this is not the most efficient approach and a better way to distribute the load more uniformly could be to launch tasks that process a fixed number of records or launching subtasks from within the per state task. 

```tiling.py``` reads the state CSV files and generates the tiles to use in the Time Machine visualization. This script processes the files sequentially and could be easily parallelized to reduce the time it takes to generate the tiles.


### Celery

```generate_state_csv.py``` uses Celery to generate the CSV files. All the celery code is inside the ``dotgen`` folder. Please see the [Celery documentation](http://docs.celeryproject.org/en/latest/getting-started/index.html) for instructions on how to start the Celery worker and configure the message broker.

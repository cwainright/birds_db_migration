# NCRN Landbirds database migration
2024-04-29

## Project description
This extract-transform-load pipeline collects data from two MS Access databases, transforms the data to a SQL Server database schema, and then loads the data to that SQL Server database.  
The pipeline enforces data integrity and referential integrity, along with business rules and error logic.  
The pipeline is intended to be used in Spring 2024 to migrate USNPS NCRN land bird monitoring data.  
After migration, the pipeline and its data sources will be archived for auditing and record-keeping.  

## Table of contents

Find an archived copy of the source files, pipeline, python environment, and dataset here: "Documents - NCRN Birds\Data Management\2024\backup\20240412".

`main.py` The pipeline's primary entry point. Running main.py from the command line will start the pipeline as a command-line-interface.  
`requirements.txt` The python environment that ran this pipeline in April 2024.

### assets/
`assets/` Contains source data files and sensitive information; not stored on GitHub. See "~Documents - NCRN Birds\Data Management\2024\backup\20240412".
### data/
`duplicate_visits.py` A python script to find duplicate visits for subject-matter-experts to de-duplicate before database migration.  
`location_wrangling.py` A python script to find location data integrity problems for subject-matter-experts to resolve before database migration.  
`species_wrangling.py` A python script to find species-code data integrity problems for subject-matter-experts to resolve before database migration.  
### src/
`build_tbls.py` Python module to execute queries to retrieve destination and source tables.  
`check.py` Python module to check business logic and data integrity.  
`db_connect.py` Python module to connect to NCRN databases.  
`k_loads.py` Python module to update primary-key/foreign-key relationships.  
`load_tbls.py` Python module containing the SQL Server database loading procedure.  
`make_templates.py` Python module that builds the function call-stack and routes objects through the pipeline.  
`tbl_xwalks.py` Python module that encodes business logic to crosswalk data from source-file to destination-table.  
`src/qry/` A collection of SQL queries (mostly SELECT statements, some UPDATE statements) called in the pipeline.  

## Getting started
1. Make a local clone of this repo.
    - `$ git clone https://github.com/cwainright/birds_db_migration`
2. Reproduce the NCRN Landbirds pipeline's python environment.
    - `$ pip install requirements.txt`
    - Alternative: If your python environment has dependency problems while reproducing or running the pipeline, copy the python environment from source.
        - "Documents - NCRN Birds\Data Management\2024\backup\20240412\birds_db_migration_pyenv.zip"
3. Copy the `assets/` folder from "~Documents - NCRN Birds\Data Management\2024\backup\20240412" into your cloned repo.
4. Run the program.
    - `$ python main.py`.
    - Alternative: create a `sandbox.py` file in your local repo and step through the minimal reproducible example below.

## Minimal reproducible example
```
import src.make_templates as mt
import src.check as c
import src.load_tbls as loader

birds = mt.make_birds() # make data object from source files
c.check_birds(birds) # check birds object against database schema (i.e., check congruency and integrity)
c.unit_test(birds, 10, True) # check birds object against source files (i.e., validate individual records against their source records)
# loader.load_birds(birds) # load data to database
#        note: you must have the correct database:
#           a) a local SQL Server instance with a database named "NCRN_Landbirds" built to the spec in src/qry/devops/create_db_devops.sql and src/qry/devops/create_tables_devops.sql
#           b) a remote SQL Server instance to which you have write-access that matches the spec above
```

## Contact
charles_wainright@nps.gov Data Scientist US National Park Service 
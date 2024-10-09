Process which uses dlt(https://dlthub.com/docs/intro) to download raw JSON data from the NIST National Vulnerability Database API and loads it into a duckdb database using a Pydantic data model.

This is more of proof of concept to see what creating a pipeline using dlt looks like in an effort to see how using an upsert style flow would work.  
This approach uses the most recent last_modified_date in the database to only pull in the records since the last successful run. You will need to request your own API token
which the instructions and workflow used to create this process can be found at https://nvd.nist.gov/developers/api-workflows.

To run loading data into a development database:

`ENV_FOR_DYNACONF=development python data_pipeline.py`
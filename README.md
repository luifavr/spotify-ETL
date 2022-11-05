# Spotify ETL
## Data pipeline extracting recently played songs from the Spotify API for developers


## Setup

### In order to use this API you have to generate an authentication token in the following URL, spotify account needed.
### https://developer.spotify.com/console/get-recently-played/

## Initialize Python virtual enviroment
```
python3 -m venv venv
```
## Install the required modules
```
pip install -r requirements.txt
```

## Create a .env file to store the enviroment variables (SQLite Database connection and auth token)
```
DBCONN=VALUE
TOKEN=VALUE
```
## Run the ETL 
```
python3 etl.py
```
### After the etl is executed a file named info.log will be created at the current directory, containing all the logs from the execution and warnings in case any exceptions is raised


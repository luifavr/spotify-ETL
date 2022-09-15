# Spotify ETL
## Data pipeline extracting recently played songs data from the Spotify API for developers


## For setup

### In order to use this API yo have to geneteate an authentication token in the following URL, spotify account needed.
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


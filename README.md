[![Documentation Status](https://readthedocs.org/projects/japan/badge/?version=latest)](https://japan.readthedocs.io/en/latest/?badge=latest)

***

[Manual to use Japan Air Pollutio ANalytics Package](https://japan.readthedocs.io/en/latest/)

***

# SQL queries

### Execute the following two SQL queries to create tables in a database.

## Step 1: Create Station_info table

    CREATE TABLE station_info(stationid integer not null, location geography(POINT,4326), address varchar,constraint Sid unique (stationID));

## Step 2: Create table to store hourlyObservations

    create table hourly_observations(stationID int not null, obsDate timestamp, SO2 double precision, no double precision, no2 double precision, nox double precision, co double precision, ox double precision, nmhc double precision, ch4 double precision, thc double precision, spm double precision, pm25 double precision, sp double precision, wd varchar, ws double precision, temp double precision, hum double precision, constraint SOH unique (stationID,obsDate));


***

# Unzipping the data files

### 1.1. Old zip files

The programs in this directory are meant to store the SORAMAME air pollution data for the period 2018-01-01 to 2021-03-31.


1. Create two temporary directories, say _temp_ and _tempBak_.

       mkdir temp tempBak 
       #temp directory will store the data.
       #tempBak directory is used by the Python program. 
2. Download all the zip files into the _temp_ directory.
3. Enter into the _temp_ directory.
 
       cd temp

4. Uncompress the zip files using the below provided shell script. 

       vi uncompressZipFiles.sh
      
       #add the below provided code
       zipFiles=`ls ~/temp/*.zip`

       for eachZipFile in $zipFiles
       do
          unzip $eachZipFile
          rm $eachZipFile
       done
    
       subZipFiles=`ls ~/temp/*.zip`
       for eachZipfile in $subZipFiles
       do
              echo 'unzipping ' $eachZipfile
              unzip $eachZipfile
              rm -rf $eachZipfile
       done

5.  Execute the shell script.  

        sh uncompressZipFiles.sh

    The above program will create the folders '01' to '47'. Each folder represents a Prefecture in Japan.  


### 1.2. New zip files

The programs in this directory are meant to store the SORAMAME air pollution data generated from 2021-04-01~.


1. Download the data.zip file from the soramame website.
2. Move the file into the directory containing the file "insertDataFromZipFolderToDatabase.py"

       mv ~/Downloads/data.zip .

3. Create a temporary directory, say temp, to store the unzip files. 
   
       mkdir temp

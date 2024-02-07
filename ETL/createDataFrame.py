# Creating the CSV file dataset that contains timestamps and pm2.5 data.
#
# **Importing into a python program**
# -----------------------------------------------------
#             from japanAirAnalytics.ETL import createDataFrame as db
#
#             obj = db(startDate, endDate)
#
#             timestamps = obj.createTimeStampColumnInDataFrame()
#
#             pm25Data = obj.generateDataFrameForAllStations()
#
#             saveFile = obj.save("pm25_20180101_20231031.csv")


import sys
import psycopg2
import pandas as pd
from ETL import config
from datetime import timedelta, datetime
from alive_progress import alive_bar


class createDataFrame:
    """
    :Description: This program first creates timestamps with the help of the startDate and endDate, storing them in a dataframe. Secondly, it generates a pm2.5 data into a dataframe and merge with timeStamp.


    :param  startDate: str :
                    Specify the start date in the format 'YYYY-MM-DD'. Example: '2018-01-01'

    :param  endDate: str :
                    Specify the end date in the format 'YYYY-MM-DD'. Example: '2023-10-31'


    :Methods:

        createTimeStampColumnInDataFrame(): Creates timestamps by taking startDate and endDate as inputs.

        generateDataFrameForAllStations(): Collects station IDs and pm2.5 data, then merges them with the timestamp dataframe.

        save(fileName): saves the generated dataset into a CSV file.


    **Executing code on terminal**
    ------------------------------------

               Format:
                        >>>  python3 createDataFrame.py <startDate> <endDate> <pollutant> <saveFileName>
               Example:
                        >>>  python3 createDataFrame.py '2018-01-01' '2023-10-31' 'pm25' 'data.csv'

                        .. note:: Specify the name of the database in database.ini file


    **Importing into a python program**
    -----------------------------------------
    .. code-block:: python

             from ETL import createDataFrame as db

             obj = db(startDate, endDate, pollutant='pm25')

             timestamps = obj.createTimeStampColumnInDataFrame()

             pm25Data = obj.generateDataFrameForAllStations()

             saveFile = obj.save("pm25_20180101_20231031.csv")


    """

    def __init__(self, startDate: str, endDate: str, pollutant='pm25'):
        self.startDate = datetime.strptime(startDate, '%Y-%m-%d')
        self.endDate = datetime.strptime(endDate, '%Y-%m-%d')
        self.dataframe = None
        self.timeStamps = None
        self.stationIDs = None
        self.pm25Data = None
        self.pollutant = pollutant
        self.time = []

    def createTimeStampColumnInDataFrame(self) -> None:
        """
        This function takes two parameters as input (startDate, endDate), generates timestamps in format ('%Y-%m-%d %H:%M:%S') and finally stores in dataFrame.

        :return: None

                 DataFrame stores timestamp values

        """
        hoursList = []
        while self.startDate <= endDate:
            for hour in range(24):
                hoursList.append(self.startDate.replace(hour=hour, minute=0, second=0).strftime('%Y-%m-%d %H:%M:%S'))
            self.startDate += timedelta(days=1)

            # Creating dataframe with timestamps as column
        self.dataframe = pd.DataFrame(hoursList, columns=['TimeStamp'])

    def generateDataFrameForAllStations(self, tableName='hourly_observations') -> None:
        """
        This function involves three steps:

            - First, establish a connection to the database.

            - Second, collect station IDs and pm2.5 data from the table and store them in a temporary dictionary.

            - Third, merge the dataframe with timestamps and pm2.5 data.

        :param tableName: Specify the name of the table

        :return: None

                 DataFrame stores timestamp, pm2.5 values
        """
        dataframe = None
        conn = None
        try:
            # read connection parameters
            params = config()

            # print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)

            # create a cursor
            cur = conn.cursor()

            # Execute sql query to find  distinct sensors in a database
            query = 'SELECT DISTINCT(stationid) FROM ' + tableName
            cur.execute(query)
            stationIDs = cur.fetchall()

            with alive_bar(len(stationIDs)) as bar:
                for station in stationIDs:
                    bar()
                    query = 'select obsdate,' + self.pollutant +' from ' + tableName + ' where stationid = %s ORDER BY obsdate asc'
                    # print(station[0])
                    cur.execute(query, (station[0],))
                    pm25Data = cur.fetchall()
                    # print(pm25Data)

                    temp = {}
                    for sensorValue in pm25Data:
                        if sensorValue[1] in [-1000, 9999] or sensorValue[1] =='None':
                            temp[str(sensorValue[0])] = 'NaN'
                        else:
                            temp[str(sensorValue[0])] = str(sensorValue[1])

                    # print(temp.values()) # TOBE DELETED

                    sensorSpecificDataFrame = pd.DataFrame({'TimeStamp': temp.keys(), station[0]: temp.values()})
                    # print(sensorDf)
                    self.dataframe = pd.merge(self.dataframe, sensorSpecificDataFrame, on='TimeStamp', how='left')

                # print(self.dataframe)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def save(self, fileName):
        """
        Stores the pm2.5 data into a csv file

        :param fileName: Specify the name of the csv file

        :return: csv file
        """

        print(self.dataframe)
        self.dataframe.to_csv(fileName, index=False)


if __name__ == "__main__":
    """
    start the main() method.
    
    """
    try:
        if len(sys.argv) != 5:
            raise ValueError("Incorrect number of input parameters")

        startDate = sys.argv[1]
        endDate = sys.argv[2]
        pollutant = sys.argv[3]
        obj = createDataFrame(startDate, endDate,pollutant)
        obj.createTimeStampColumnInDataFrame()
        obj.generateDataFrameForAllStations()
        #obj.save("pm25_20180101_20231031.csv")
        obj.save(sys.argv[4])

    except ValueError as ve:
        print(f"Error: {ve}. Format: startDate = 2018-01-01, endDate = 2023-10-31, pollutant = 'pm25'")
    except Exception as e:
        print(f"An error occurred: {e}")


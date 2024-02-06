# This function inserts station information from a CSV file into a PostgreSQL database.
# It uses the provided input file to read station data and inserts it into the 'station_info' table in the database.
# **Importing data files in a Python program**
# ---------------------------------------------
#
#
#             from japanAirAnalytics.store import stationInfo as db
#
#             obj = stationInfo()
#
#             obj.insert(inputFile)


import csv
import sys
import psycopg2
from japanAirAnalytics.store import config
from alive_progress import alive_bar

# SQL query to create stationInformation table
# query = CREATE TABLE stationInfo(sid int not null, geog geography(POINT,4326), addressInfo varchar)

class stationInfo:
    """
    :Description: This class inserts station information from a CSV file into a PostgreSQL database. It reads station data from the provided input file and inserts it into the 'station_info' table in the database.

    :param  inputFile: str :
                File containing station information (-stationAdd.txt file in Data folder).

    :Methods:
            insert(inputFile): This method reads a CSV file and inserts station information into the 'station_info' table.


    **Executing on Python Terminal**
    ----------------------------------

                    Format:
                        >>>  stationInfo.insert(inputFile)
                    Example:
                        >>>  stationInfo.insert("station_info_data.csv")

    **Importing data files in a Python program**
    ---------------------------------------------

              from japanAirAnalytics.store import stationInfo as db

              obj = stationInfo()

              obj.insert(inputFile)


    """

    def insert(inputFile):

        """
        This method reads the specified CSV file and inserts station information into the 'station_info' table in a PostgreSQL database.

        :param:  inputFile: str :
                    The path to the CSV file containing station information. Example: stationInfo.insert("station_info_data.csv")

        Returns:
            None

        """
        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            #print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)

            # create a cursor
            cur = conn.cursor()

            lines = 0;
            with open(inputFile, 'r') as fp:
                lines = len(fp.readlines())

            # Open the CSV file
            csv_file = open(inputFile, encoding="utf-8", errors="",
                            newline="")
            fileObject = csv.reader(csv_file, delimiter=",")

            print("Inserting the information on stations into the database")
            with alive_bar(lines) as bar:
                for row in fileObject:
                    bar()
                    query = "insert into station_info values(" + row[1] + ',\'' + row[2] + '\',\'' + row[3] + "\')"
                    #print(query)
                    # executes query
                    cur.execute(query)


                conn.commit()
            cur.close()

        # Exception handling
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                # close database connection
                conn.close()
                print('Database connection closed.')


if __name__ == '__main__':
    """
        Start the main() Method
    """

    if len(sys.argv) < 2:
        print("Error : Incorrect number of input parameters")
        print("Format: python3  stationInfo.py  fileName")
    else:
        stationInfo.insert(sys.argv[1])


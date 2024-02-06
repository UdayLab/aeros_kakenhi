# This Program takes input as folder containing various csv files in it. Reads all csv files and handles missing dates and inserts the data into a PostgreSQL database.
#
# **Importing this japanAirAnalytics data files in a Python program**
# ---------------------------------------------------------------------
#
#
#           from japanAirAnalytics.store import oldHourlyDataFormat as db
#
#           obj = db()
#
#           obj.insertData(inputDataFolder)

import csv
import os
import sys
from os import listdir
from os.path import isfile, join

import psycopg2
from alive_progress import alive_bar

from japanAirAnalytics.store import config


class oldHourlyDataFormat:
    """
    :Description: This program reads CSV files from the specified folder, handles missing dates, and inserts the data into the hourly_observations table in a PostgreSQL database.

    :param  inputDataFolder: str :
                            The path to the folder containing CSV files. Example: oldHourlyDataFormat.insert('/path/to/inputDataFolder')
    :Attributes:
            None

    :Methods:
            insert(inputDataFolder): Reads CSV files, handles missing dates, and inserts data into the hourly_observations table.


    **Methods to execute japanAirAnalytics on terminal**
    -------------------------------------------------------

                Format:
                        >>>python3 oldHourlyDataFormat.py <inputDataFolder>
                Example:
                        >>>python3 oldHourlyDataFormat.py inputDataFolder

                        .. note:: Specify the name of the database in database.ini file


    **Importing this japanAirAnalytics data files into a python program**
    ------------------------------------------------------------------------
    .. code-block:: python

            from japanAirAnalytics.store import oldHourlyDataFormat as db

            obj = db()

            obj.insertData(inputDataFolder)

    """

    def insert(inputDataFolder):
        subFolders = [f.path for f in os.scandir(inputDataFolder) if f.is_dir() ]
        conn = None
        inputFileName = ""
        for folder in subFolders:
            try:
                params = config()
                conn = psycopg2.connect(**params)

                cur = conn.cursor()


                print('Reading the folder: ' + str(folder))
                files = [f for f in listdir(folder) if isfile(join(folder, f))]

                with alive_bar(len(files)) as bar:
                    for file in files:
                        bar()

                        inputFileName = folder + '/' + file

                        csv_file = open(inputFileName, encoding="cp932", errors="", newline="")
                        f = csv.reader(csv_file, delimiter=",", doublequote=True, lineterminator="\r\n", quotechar='"',
                                       skipinitialspace=True)
                        header = next(f)

                        for row in f:
                            date = ''
                            query = ''

                            for i in range(len(row)):
                                # filling missing values
                                # print(row[i])
                                if i == 1 or i ==2:
                                    if row[i] == '':
                                        date = 'NULL'
                                else:
                                    if row[i] == '' or row[i] == '-' or '#' in row[i]:
                                        row[i] = 'NULL'

                            if date == '':
                                # writing query
                                query = 'insert into hourly_observations values(' + row[0] + ',\'' + row[1] + ' ' + row[
                                    2] + ':00:00\'' + ',' + \
                                        row[3] + ',' + row[4] + ',' + row[5] + ',' \
                                        + row[6] + ',' + row[7] + ',' + row[8] + ',' + row[9] + ',' + row[10] + ',' + \
                                        row[
                                            11] + ',' + \
                                        row[12] + ',' + row[13] + ',' + row[14] + ',-1' + ',' + row[16] + ',' + row[
                                            17] + ',' + row[
                                            18] + ")"
                            else:
                                # writing query
                                query = 'insert into hourly_observations values(' + row[0] + ',' + date + ',' + \
                                        row[3] + ',' + row[4] + ',' + row[5] + ',' \
                                        + row[6] + ',' + row[7] + ',' + row[8] + ',' + row[9] + ',' + row[10] + ',' + row[
                                            11] + ',' + \
                                        row[12] + ',' + row[13] + ',' + row[14] + ',-1' + ',' + row[16] + ',' + row[17] + ',' + row[
                                            18] + ")"

                            # executing the query
                            cur.execute(query)
                    conn.commit()
                cur.close()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error,  inputFileName)
            finally:
                if conn is not None:
                    conn.close()


if __name__ == '__main__':
    """
        Start the main() Method
    """

    if len(sys.argv) < 2:
        print("Error : Incorrect number of input parameters")
        print("Format: python3  stationInfo.py  inputFolderContainingData")
    else:
        oldHourlyDataFormat.insert(sys.argv[1])
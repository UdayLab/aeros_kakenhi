import csv
import sys
from os import listdir
from os.path import isfile, join
import psycopg2
import config
from alive_progress import alive_bar


class newHourlyDataFormat:
    def insert(inputDataFolder):
        files = [f for f in listdir(inputDataFolder) if isfile(join(inputDataFolder, f))]
        with alive_bar(len(files)) as bar:
            for file in files:
                bar()
                conn = None
                try:
                    DB_NAME = "soramame_final"
                    DB_USER = "temp"
                    DB_PASS = "temp@14916"
                    DB_HOST = "163.143.165.141"
                    DB_PORT = "5432"
                    conn = psycopg2.connect(database=DB_NAME,
                                            user=DB_USER,
                                            password=DB_PASS,
                                            host=DB_HOST,
                                            port=DB_PORT)
                    # create a cursor
                    cur = conn.cursor()

                    # reading csv file
                    csv_file = open(inputDataFolder + '/' + file, encoding="cp932", errors="", newline="")

                    f = csv.reader(csv_file, delimiter=",", doublequote=True, lineterminator="\r\n", quotechar='"',
                                   skipinitialspace=True)

                    header = next(f)
                    for row in f:
                        date = ''
                        query = ''

                        for i in range(len(row)):
                            # filling missing values
                            # Handling empty dates
                            if i == 1 or i == 2:
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
                                    row[12] + ',' + row[13] + ',' + row[14] + ',-1' + ',' + row[16] + ',' + row[
                                        17] + ',' + row[
                                        18] + ")"
                        # executing the query
                        cur.execute(query)
                    conn.commit()
                    # print('Success')

                    # close the communication with the PostgreSQL
                    cur.close()


                except (Exception, psycopg2.DatabaseError) as error:
                    print(error, inputDataFolder + '/' + file)

                finally:
                    if conn is not None:
                        conn.close()
                        # print('Database connection closed.')



if __name__ == '__main__':
    """
        Start the main() Method
    """
    if len(sys.argv) < 2:
        print("Error : Incorrect number of input parameters")
        print("Format: python3  stationInfo.py  fileName")
    else:
        newHourlyDataFormat.insert(sys.argv[1])
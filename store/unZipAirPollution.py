# This function takes an input zip file and unzips its contents into the specified output directory.
# It returns the path to the unzipped folder.
# **Importing data files in a Python program**
# ---------------------------------------------
#
#           from japanAirAnalytics.store import getFolder as db
#
#           obj = db(zipFolder, outputLocation)

import os
import sys
import zipfile


def getFolder(zipFolder, outputLocation):
    """
    :Description: This function takes an input zip file (zipFolder) and unzips its contents into the specified output directory (outputLocation). It returns the path to the unzipped folder.

    :param  zipFolder: str :
                Input zip file containing data.
    :param  outputLocation: str :
                Directory to store the unzipped files.

    :return: str
            Returns the path to the unzipped folder.

    **Executing on terminal**
    -----------------------------

                Format:
                        >>>  python3 getFolder.py <zipFolder> <outputLocation>
                Example:
                        >>>  python3 getFolder.py 'data.zip' 'temp_data'

                        .. note:: Specify the name of the database in database.ini file

    **Importing data files into a python program**
    --------------------------------------------------
    .. code-block:: python

            from japanAirAnalytics.store import getFolder as db

            obj = db(zipFolder, outputLocation)

    """

    with zipfile.ZipFile(zipFolder, 'r') as zip_ref:
        zip_ref.extractall(outputLocation + str(zipFolder.split('.')[0]))
    return os.path.join(outputLocation, zipFolder.split('.')[0])


if __name__ == '__main__':
    """
    Start the main() Method
    
    """
    if len(sys.argv) < 3:
        print("Error : Incorrect number of input parameters given : " + str(len(sys.argv) - 1))
        print("Input Parameters-> zip folder Path, output Folder Path")
    else:
        unzippedLocation = getFolder(sys.argv[1], sys.argv[2])

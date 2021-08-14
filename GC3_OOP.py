import pandas
from datetime import datetime
import csv
from pathlib import Path
import math
import timer
import re
import logging
import codecs
import os
from abc import ABC, abstractmethod

logging.basicConfig(filename='Errorlog.txt',level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')     #Config for setup log file to detect errors
logging.debug('Start of program')

class FileProcessor:

    @abstractmethod
    def check_file_exists(self):
        pass

    @abstractmethod
    def read_file(self):
        pass

    @abstractmethod
    def check_file_status(self):
        pass

class timestamp_Validation:

    @classmethod
    def time_validator(cls):


class surfaceFileProcessor(FileProcessor):

    def __init__(self, filepath):
        self.filepath = filepath

    def check_file_exists(self):
        if os.path.isfile(self.filepath):
            return True
        else:
            return False

    def read_file(self):
        df = pandas.read_csv(self.filepath, header=None, low_memory=False, skiprows=1)
        return df

    def check_file_status(self):
        if self.check_file_exists():
            stationname = Path(self.filepath).resolve().stem
            surface_result = "File not found"
            reference_result = "N/A"
            percentNAN = "N/A"

        else:
            df = self.read_file()
            date_time_str = df.iloc[-1, 0]  # Assign the last timestamp to the variable date_time_str
            if "." in date_time_str:        # Conditional statement to remove miliseconds in timestamp
                date_time_str = re.sub(r'\..*', '', date_time_str)  # remove miliseconds from timestamp
            date_time_obj = datetime.strptime(date_time_str,
                                              '%Y-%m-%d %H:%M:%S')  # Assign the converted timestamp to datetime format to the variable date_time_obj
            if str(date_time_obj.date()) != datetime.today().strftime(
                    '%Y-%m-%d'):  # Conditional statement checking whether the surface file is up to dated
                surface_result = "Surface file not updated"
                first_row = list(df.iloc[0, :])  # assign the list of value of the first row to the variable a
                for prism in first_row:
                    if prism[-7:] == "relatif" and prism[-15:-12] == "MPO":
                        stationname = prism[0:-16]                   #Get the station name
                        break

                reference_result = "N/A"  # Set the reference_result
                percentNAN = "N/A"  # Set the percentNAN

            else:
                duplicated = "No"         #Set the the duplicated check
                surface_result = "Surface file updated"
                a = list(df.iloc[0, :])  # assign the list of value of the first row to the variable a
                b = [reference for reference in a if reference[-12:] == "NB_Reference" or reference[
                                                                                          -12:] == "Reference_NB"]  # create a list b with reference column label
                c = list(df.iloc[:, 0])  # assign the list of timestamp to the variable c
                d = []  # assign variable d as a blank list

       #Start from here

                for time_index in range(len(c)-1,-1,-1):  # Make a loop for each value of timestamp - starting from the last timestamp
                    todaytime = c[time_index]
                    if "." in todaytime:  # Conditional statement for removing miliseconds in timestamps
                        l = re.sub(r"\..*", "", todaytime)  # remove miliseconds from timestamp

                        if str(datetime.strptime(l, '%Y-%m-%d %H:%M:%S').date()) == datetime.today().strftime(
                                '%Y-%m-%d'):  # logical testing whether the timestamp value is up to dated
                            d.append(todaytime)  # append to d the corresponding timestamp
                        else:                       #Stop the loop if encountering the previous day
                            break
                    else:  # for timestamp without miliseconds
                        if str(datetime.strptime(todaytime,
                                                 '%Y-%m-%d %H:%M:%S').date()) == datetime.today().strftime(
                                '%Y-%m-%d'):  # logical testing whether the timestamp value is up to dated
                            d.append(todaytime)  # append to d the corresponding timestamp
                        else:                       #Stop the loop if encountering the previous day
                            break



                for i in range(0, len(d)):  # Loop for checking duplicated timestamps in SNCF files
                    if d[i] == d[i + 1]:
                        duplicated = "Yes"
                        break


                df3 = pandas.read_csv(dat_file,
                                      low_memory=False)  # Assign the rawdata_file dataframe as df3 with header reading
                df2 = df3.set_index(
                    "TIMESTAMP")  # Assign the df2 to the dataframe of df3 in version of setting TIMESTAMP column as index


                if duplicated == "Yes":  # remove duplicated timestamps in list d
                    d = list(dict.fromkeys(d))

                df2 = df2[~df2.index.duplicated(
                    keep="first")]  # remove duplicated rows in dataframe df2 with TIMESTAMP as index column

                if b:  # Conditional statement whether reference column is available in the surface file
                    df4 = df2.loc[d[0]:d[-1],
                          b]  # Filtered the dataframe with up to dated timestamp and prism only data
                    e = list(df2.loc[d[0]:d[-1],
                             :])  # Assign the list of the prisms value of the first up to dated timestamp
                    # df2_t=df2.T
                    # df2_t.set_index("TIMESTAMP",inplace=True)
                    # df2
                    # list(df2_t.loc[:,d[0]:d[-1]])

                    df5 = df4[b].values.tolist()  # Assign the Reference column values as a list to df5
                    for reference_nb in df5:  # loop for checking reference values in surface file
                        if int(reference_nb[0]) >= 3:  # Conditional statement for reference >=3
                            reference_result = int(reference_nb[0])  # Assign reference number to reference_result
                            break  # Stop the loop
                    if int(reference_nb[0]) < 3:  # Conditional statement for reference <=3
                        reference_result = "<3"  # Assign <3 to reference_result
                    f = [prism for prism in a if prism[-7:] == "relatif" and prism[
                                                                             -15:-12] == "MPO"]  # Creating a list with prisms name exclusively
                    df6 = df2.loc[d[0]:d[-1], f]  # Assign the dataframe consisted of prisms name, up to dated timestamp
                    g = list(df6.loc[d[0], :])  # Assign the first row of df6 dataframe as a list to variable g

                    for timestamp in d:  # Loop for calculating NAN values
                        prismattributes = list(df6.loc[timestamp,
                                               :])  # Assign each row of df6 dataframe as a list to variable prismattributes
                        g = [prismattribute if value == "NAN" and prismattribute != "NAN" else value for
                             value, prismattribute in zip(g,
                                                          prismattributes)]  # List comprehension to replace NAN value in g throughout the loop
                    percentNAN = str(
                        round(g.count("NAN") / len(g) * 100, 2)) + "%"  # Calculate the percentage of NAN value in g

                else:  # For the case which there is no reference column
                    f = [prism for prism in a if prism[-7:] == "relatif" and prism[
                                                                             -15:-12] == "MPO"]  # Creating a list with prisms name exclusively
                    df6 = df2.loc[d[0]:d[-1], f]  # Assign the dataframe consisted of prisms name, up to dated timestamp
                    g = list(df6.loc[d[0], :])  # Assign the first row of df6 dataframe as a list to variable g

                    for timestamp in d:  # Loop for calculating NAN values
                        prismattributes = list(df6.loc[timestamp,
                                               :])  # Assign each row of df6 dataframe as a list to variable prismattributes
                        g = [prismattribute if value == "NAN" and prismattribute != "NAN" else value for
                             value, prismattribute in zip(g,
                                                          prismattributes)]  # List comprehension to replace NAN value in g throughout the loop
                    percentNAN = str(
                        round(g.count("NAN") / len(g) * 100, 2)) + "%"  # Calculate the percentage of NAN value in g
                stationname = f[0][0:-16]  # Get the station name


            # logging.debug('End of processing surface file (%s)' % (surface_file))

        return stationname, surface_result, reference_result, percentNAN

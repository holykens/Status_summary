import pandas
from datetime import datetime
from pathlib import Path
import math
import time
import re
import logging
import os
from abc import ABC, abstractmethod

logging.basicConfig(filename='Errorlog.txt',level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')     #Config for setup log file to detect errors
logging.debug('Start of program')

class NAN_check(ABC):

    @abstractmethod
    def _NAN_calculator(self, df6, d, g):
        pass

class FileProcessor(ABC):

    @abstractmethod
    def _check_file_exists(self):
        pass

    @abstractmethod
    def _read_file(self):
        pass

    @abstractmethod
    def _isUpdated(self, df):
        pass

    @abstractmethod
    def _time_Formatter(self, c):
        pass

    @abstractmethod
    def check_file_status(self):
        pass


class surfaceFileProcessor(FileProcessor, NAN_check):

    def __init__(self, filepath):
        self.filepath = filepath

    def _check_file_exists(self):
        if os.path.isfile(self.filepath):
            return True
        else:
            return False

    def _read_file(self):
        df = pandas.read_csv(self.filepath, header=None, low_memory=False, skiprows=1)
        return df

    def _isUpdated(self, df):
        _date_time_str = df.iloc[-1, 0]  # Assign the last timestamp to the variable _date_time_str
        if "." in _date_time_str:  # Conditional statement to remove miliseconds in timestamp
            _date_time_str = re.sub(r'\..*', '', _date_time_str)  # remove miliseconds from timestamp
        _date_time_obj = datetime.strptime(_date_time_str,
                                          '%Y-%m-%d %H:%M:%S')  # Assign the converted timestamp to datetime format to the variable _date_time_obj

        # Conditional statement checking whether the surface file is up to dated
        if str(_date_time_obj.date()) != datetime.today().strftime(
                '%Y-%m-%d'):
            surface_result = "Surface file not updated"
        else:
            surface_result = "Surface file updated"

        return surface_result

    def _refCheck(self, df2, a, d):
        _refValues = [reference for reference in a if reference[-12:] == "NB_Reference" or reference[
                                                                                  -12:] == "Reference_NB"]  # create a list _refValues with reference column label
        if _refValues:
            _df4 = df2.loc[d[0]:d[-1],
                  _refValues]  # Filtered the dataframe with up to dated timestamp and prism only data

            _df5 = _df4[_refValues].values.tolist()  # Assign the Reference column values as a list to df5
            for reference_nb in _df5:  # loop for checking reference values in surface file
                if int(reference_nb[0]) >= 3:  # Conditional statement for reference >=3
                    reference_result = int(reference_nb[0])  # Assign reference number to reference_result
                    break  # Stop the loop
            if int(reference_nb[0]) < 3:  # Conditional statement for reference <=3
                reference_result = "<3"  # Assign <3 to reference_result

        else:
            reference_result = "This file does not have the reference column"

        return reference_result

    def _NAN_calculator(self, df6, d, g):
        for _timestamp in d:  # Loop for calculating NAN values
            _prismattributes = list(df6.loc[_timestamp,
                                   :])  # Assign each row of df6 dataframe as a list to variable _prismattributes
            g = [_prismattribute if _value == "NAN" and _prismattribute != "NAN" else _value for
                 _value, _prismattribute in zip(g,
                                              _prismattributes)]  # List comprehension to replace NAN value in g throughout the loop
        percentNAN = str(
            round(g.count("NAN") / len(g) * 100, 2)) + "%"  # Calculate the percentage of NAN value in g

        return percentNAN

    def _time_Formatter(self, c):
        d = []
        duplicated = "No"  # Set the the duplicated check
        for _time_index in range(len(c) - 1, -1,
                                 -1):  # Make a loop for each value of timestamp - starting from the last timestamp
            _todaytime = c[_time_index]
            if "." in _todaytime:  # Conditional statement for removing miliseconds in timestamps
                _l = re.sub(r"\..*", "", _todaytime)  # remove miliseconds from timestamp

                if str(datetime.strptime(_l, '%Y-%m-%d %H:%M:%S').date()) == datetime.today().strftime(
                        '%Y-%m-%d'):  # logical testing whether the timestamp value is up to dated
                    d.append(_todaytime)  # append to d the corresponding timestamp
                else:  # Stop the loop if encountering the previous day
                    break
            else:  # for timestamp without miliseconds
                if str(datetime.strptime(_todaytime,
                                         '%Y-%m-%d %H:%M:%S').date()) == datetime.today().strftime(
                    '%Y-%m-%d'):  # logical testing whether the timestamp value is up to dated
                    d.append(_todaytime)  # append to d the corresponding timestamp
                else:  # Stop the loop if encountering the previous day
                    break

        d.reverse()  # reverse the timestamp for correct order

        for i in range(0, len(d) - 1):  # Loop for checking duplicated timestamps
            if d[i] == d[i + 1]:
                duplicated = "Yes"
                break

        if duplicated == "Yes":  # remove duplicated timestamps in list d
            d = list(dict.fromkeys(d))

        return d

    def check_file_status(self):
        try:
            if not self._check_file_exists():
                stationname = Path(self.filepath).resolve().stem
                surface_result = "File not found"
                reference_result = "N/A"
                percentNAN = "N/A"

            else:
                df = self._read_file()
                surface_result = self._isUpdated(df)
                if surface_result == "Surface file not updated":

                    _first_row = list(df.iloc[0, :])  # assign the list of value of the first row to the variable _first_row
                    for _prism in _first_row:
                        if _prism[-7:] == "relatif" and _prism[-15:-12] == "MPO":
                            stationname = _prism[0:-16]                   #Get the station name
                            break

                    reference_result = "N/A"  # Set the reference_result
                    percentNAN = "N/A"  # Set the percentNAN

                else:

                    a = list(df.iloc[0, :])  # assign the list of value of the first row to the variable a
                    c = list(df.iloc[:, 0])  # assign the list of timestamp to the variable c
                    d = self._time_Formatter(c)         #Get the list of updated timestamp

                    df.columns = df.iloc[0]         #Set header of dataframe
                    _df1 = df.drop(df.index[0])      #drop the duplicated header row
                    df2 = _df1.set_index("TIMESTAMP")    #Set index for dataframe

                    df2 = df2[~df2.index.duplicated(
                        keep="first")]  # remove duplicated rows in dataframe df2 with TIMESTAMP as index column

                    reference_result = self._refCheck(df2, a, d)

                    _f = [_prism for _prism in a if _prism[-7:] == "relatif" and _prism[
                                                                                 -15:-12] == "MPO"]  # Creating a list with prisms name exclusively
                    df6 = df2.loc[d[0]:d[-1],_f]  # Assign the dataframe consisted of prisms name, up to dated timestamp
                    g = list(df6.loc[d[0], :])  # Assign the first row of df6 dataframe as a list to variable g

                    if reference_result != "This file does not have the reference column":  # Conditional statement whether reference column is available in the surface file
                        percentNAN = self._NAN_calculator(df6, d, g)                        #Calculate the percentage of NAN

                    else:  # For the case which there is no reference column
                        percentNAN = self._NAN_calculator(df6, d, g)        #Calculate the percentage of NAN

                    stationname = _f[0][0:-16]  # Get the station name

        except:
            logging.debug(
                'Error on surface file (%s)' % (self.filepath))  # Write the corresponding error dat file in error log
            stationname = Path(self.filepath).resolve().stem
            surface_result = "Unknown error"
            reference_result = "N/A"
            percentNAN = "N/A"

        return stationname, surface_result, reference_result, percentNAN

class rawdataFileProcessor(FileProcessor):

    def __init__(self, filepath):
        self.filepath = filepath

    def _check_file_exists(self):
        if os.path.isfile(self.filepath):
            return True
        else:
            return False

    def _read_file(self):
        df = pandas.read_csv(self.filepath, header=None, low_memory=False, skiprows=1)
        return df

    def _isUpdated(self, df):
        _date_time_str = df.iloc[-1, 0]  # Assign the last timestamp to the variable _date_time_str
        if "." in _date_time_str:  # Conditional statement to remove miliseconds in timestamp
            _date_time_str = re.sub(r'\..*', '', _date_time_str)  # remove miliseconds from timestamp
        _date_time_obj = datetime.strptime(_date_time_str,
                                           '%Y-%m-%d %H:%M:%S')  # Assign the converted timestamp to datetime format to the variable _date_time_obj

        if str(_date_time_obj.date()) != datetime.today().strftime(
                '%Y-%m-%d'):  # Conditional statement checking whether the rawdata file is up to dated
            rawdata_result = "Rawdata file not updated"
        else:
            rawdata_result = "Rawdata file updated"

        return rawdata_result

    def _non_mesurement_calculator(self, d, df2, e):
        for _timestamp in d:  # Create a loop to calculate the percentage of -99999; -99997; -99995
            _prismattributes = list(
                df2.loc[_timestamp, :])  # Assign each row of df6 dataframe as a list to variable prismattributes
            e = [_prismattribute if (
                                           _value == "-99995" or _value == "-99997" or _value == "-99999") and _prismattribute != "-99995" and _prismattribute != "-99997" and _prismattribute != "-99999"
                 else _value for _value, _prismattribute in zip(e,
                                                             _prismattributes)]  # List comprehension for replacing -99999; -99997 and -99995 values in e throughout the loop
        percent99999 = str(round(e.count("-99999") / len(e) * 100, 2)) + "%"  # Calculate the percentage of -99999
        percent99997 = str(round(e.count("-99997") / len(e) * 100, 2)) + "%"  # Calculate the percentage of -99997
        percent99995 = str(round(e.count("-99995") / len(e) * 100, 2)) + "%"  # Calculate the percentage of -99995

        return percent99999, percent99997, percent99995

    def _time_Formatter(self, c):
        d = []
        for _time_index in range(len(c) - 1, -1,
                                 -1):  # Make a loop for each value of timestamp - starting from the last timestamp
            _todaytime = c[_time_index]
            if "." in _todaytime:  # Conditional statement for removing miliseconds in timestamps
                _l = re.sub(r"\..*", "", _todaytime)  # remove miliseconds from timestamp

                if str(datetime.strptime(_l, '%Y-%m-%d %H:%M:%S').date()) == datetime.today().strftime(
                        '%Y-%m-%d'):  # logical testing whether the timestamp value is up to dated
                    d.append(_todaytime)  # append to d the corresponding timestamp
                else:  # Stop the loop if encountering the previous day
                    break
            else:  # for timestamp without miliseconds
                if str(datetime.strptime(_todaytime,
                                         '%Y-%m-%d %H:%M:%S').date()) == datetime.today().strftime(
                    '%Y-%m-%d'):  # logical testing whether the timestamp value is up to dated
                    d.append(_todaytime)  # append to d the corresponding timestamp
                else:  # Stop the loop if encountering the previous day
                    break

        d.reverse()  # reverse the timestamp for correct order

        return d

    def check_file_status(self):
        try:
            if not self._check_file_exists():
                rawdata_result = "File not found"
                percent99999 = "N/A"
                percent99997 = "N/A"
                percent99995 = "N/A"

            else:
                df = self._read_file()
                rawdata_result = self._isUpdated(df)
                if rawdata_result == "Rawdata file updated":
                    _a = list(df.iloc[0, :])  # assign the list of value of the first row to the variable _a
                    _b = [_prism for _prism in _a if
                         _prism[-2:] == "Hz" or _prism[-2:] == "Vt" or _prism[-2:] == "SD"]  # create a list b with prisms name only
                    c = list(df.iloc[:, 0])  # assign the list of timestamp to the variable _c
                    d = self._time_Formatter(c)  # Get the updated timestamps

                    _firstcolumnlabel = _b[0]  # assign the name of the first prism to the variable _firstcolumnlabel
                    _lastcolumnlabel = _b[-1]  # assign the name of the last prism to the variable _lastcolumnlabel
                    df.columns = df.iloc[0]  # Set header of dataframe
                    _df1 = df.drop(df.index[0])  # drop the duplicated header row
                    df2 = _df1.set_index("TIMESTAMP")  # Set index for dataframe
                    df2 = df2.loc[d[0]:d[-1],
                          _firstcolumnlabel:_lastcolumnlabel]  # Filtered the dataframe with up to dated timestamp and prism only data
                    e = list(df2.loc[d[0],
                             :])  # Assign the list of the prisms value of the first up to dated timestamp to the variable e

                    percent99999, percent99997, percent99995 = self._non_mesurement_calculator(d, df2, e)       #Calculate the percentage of 99999, 99997 and 99995

                else:
                    percent99999 = "N/A"
                    percent99997 = "N/A"
                    percent99995 = "N/A"



        except:
            logging.debug('Error on rawdata file (%s)' % (self.filepath))  # Write the corresponding error dat file in error log
            rawdata_result = "Unknown error"
            percent99999 = "N/A"
            percent99997 = "N/A"
            percent99995 = "N/A"

        return rawdata_result, percent99999, percent99997, percent99995

class laserFileProcessor(FileProcessor, NAN_check):

    def __init__(self, filepath):
        self.filepath = filepath

    def _check_file_exists(self):
        if os.path.isfile(self.filepath):
            return True
        else:
            return False

    def _read_file(self):
        try:
            df = pandas.read_csv(self.filepath, header=None, low_memory=False, skiprows=1)
        except pandas.errors.ParserError:  # In case of inconsistent data line
            df = "Data corrupted - inconsistent data line"  # Set laser_result
        return df

    def _isUpdated(self, df):
        _date_time_str = df.iloc[-1, 0]  # Assign the last timestamp to the variable _date_time_str
        if "." in _date_time_str:  # Conditional statement to remove miliseconds in timestamp
            _date_time_str = re.sub(r'\..*', '', _date_time_str)  # remove miliseconds from timestamp
        _date_time_obj = datetime.strptime(_date_time_str,
                                           '%Y-%m-%d %H:%M:%S')  # Assign the converted timestamp to datetime format to the variable _date_time_obj

        if str(_date_time_obj.date()) != datetime.today().strftime(
                '%Y-%m-%d'):  # Conditional statement checking whether the laser file is up to dated
            laser_result = "Laser file not updated"
        else:
            laser_result = "Laser file updated"

        return laser_result

    def _NAN_calculator(self, d, df2, g):
        for _timestamp in d:  # Loop for calculating NAN values
            _prismattributes = list(df2.loc[_timestamp,
                                    :])  # Assign each row of df6 dataframe as a list to variable _prismattributes
            g = [_prismattribute if _value == "NAN" and _prismattribute != "NAN" else _value for
                 _value, _prismattribute in zip(g,
                                                _prismattributes)]  # List comprehension to replace NAN value in g throughout the loop
        percentNAN = str(
            round(g.count("NAN") / len(g) * 100, 2)) + "%"  # Calculate the percentage of NAN value in g

        return percentNAN

    def _time_Formatter(self, c):
        d = []
        duplicated = "No"  # Set the the duplicated check
        for _time_index in range(len(c) - 1, -1,
                                 -1):  # Make a loop for each value of timestamp - starting from the last timestamp
            _todaytime = c[_time_index]
            if "." in _todaytime:  # Conditional statement for removing miliseconds in timestamps
                _l = re.sub(r"\..*", "", _todaytime)  # remove miliseconds from timestamp

                if str(datetime.strptime(_l, '%Y-%m-%d %H:%M:%S').date()) == datetime.today().strftime(
                        '%Y-%m-%d'):  # logical testing whether the timestamp value is up to dated
                    d.append(_todaytime)  # append to d the corresponding timestamp
                else:  # Stop the loop if encountering the previous day
                    break
            else:  # for timestamp without miliseconds
                if str(datetime.strptime(_todaytime,
                                         '%Y-%m-%d %H:%M:%S').date()) == datetime.today().strftime(
                    '%Y-%m-%d'):  # logical testing whether the timestamp value is up to dated
                    d.append(_todaytime)  # append to d the corresponding timestamp
                else:  # Stop the loop if encountering the previous day
                    break

        d.reverse()  # reverse the timestamp for correct order

        for i in range(0, len(d) - 1):  # Loop for checking duplicated timestamps
            if d[i] == d[i + 1]:
                duplicated = "Yes"
                break

        if duplicated == "Yes":  # remove duplicated timestamps in list d
            d = list(dict.fromkeys(d))

        return d

    def check_file_status(self):
        try:
            laser_station = Path(self.filepath).resolve().stem  # Get the laser file name
            if not self._check_file_exists():
                laser_result = "File not found"
                percentlaserNAN = "N/A"

            else:
                df = self._read_file()
                #Case of file corrupted
                if isinstance(df, str):
                    percentlaserNAN = "N/A"  # Set percentlaserNAN
                    laser_result = "Data corrupted - inconsistent data line"  # Set laser_result

                    return laser_station, percentlaserNAN, laser_result

                laser_result = self._isUpdated(df)
                if laser_result == "Laser file updated":
                    _a = list(df.iloc[0, 2:])  # assign the list of value of the first row to the variable _a
                    c = list(df.iloc[:, 0])  # assign the list of timestamp to the variable c
                    d = self._time_Formatter(c)  # Get the updated timestamp

                    df.columns = df.iloc[0]  # Set header of dataframe
                    _df1 = df.drop(df.index[0])  # drop the duplicated header row
                    df2 = _df1.set_index("TIMESTAMP")  # Set index for dataframe
                    df2 = df2.loc[d[0]:d[-1], _a]  # Filtered the dataframe with up to dated timestamp and prism only data
                    g = list(df2.loc[d[0],:])  # Assign the list of the prisms value of the first up to dated timestamp to the variable g

                    percentlaserNAN = self._NAN_calculator(d, df2, g)  # Calculate the percentage of NAN

                else:  # In case of laser file not updated
                    percentlaserNAN = "N/A"  # Set percentlaserNAN

        except:  # In case of other error
            logging.debug('Error on file (%s)' % (self.filepath))  # Write the error file in error log
            laser_station = Path(self.filepath).resolve().stem  # Get the laser file name
            percentlaserNAN = "N/A"  # Set percentlaserNAN
            laser_result = "Unknown error"  # Set laser_result

        return laser_station, percentlaserNAN, laser_result

class InclinometreFileProcessor(FileProcessor, NAN_check):

    def __init__(self, filepath):
        self.filepath = filepath

    def _check_file_exists(self):
        if os.path.isfile(self.filepath):
            return True
        else:
            return False

    def _read_file(self):
        try:
            df = pandas.read_csv(self.filepath, header=None, low_memory=False, skiprows=1)
        except pandas.errors.ParserError:  # In case of inconsistent data line
            df = "Data corrupted - inconsistent data line"  # Set inclinometre result
        return df

    def _isUpdated(self, df):
        _date_time_str = df.iloc[-1, 0]  # Assign the last timestamp to the variable _date_time_str
        _date_time_obj = datetime.strptime(_date_time_str,
                                           '%Y-%m-%d %H:%M:%S')  # Assign the converted timestamp to datetime format to the variable _date_time_obj

        if str(_date_time_obj.date()) != datetime.today().strftime(
                '%Y-%m-%d'):  # Conditional statement checking whether the inclinometre file is up to dated
            Inclinometre_result = "Inclinometre file not updated"
        else:
            Inclinometre_result = "Inclinometre file updated"

        return Inclinometre_result

    def _NAN_calculator(self, d, df2, g):
        for _timestamp in d:  # Loop for calculating NAN values
            _capteurattribute = list(df2.loc[_timestamp,
                                    :])  # Assign each row of df6 dataframe as a list to variable _capteurattribute
            g = [_capteurattribute if _value == "NAN" and _capteurattribute != "NAN" else _value for
                 _value, _capteurattribute in zip(g,
                                                _capteurattribute)]  # List comprehension to replace NAN value in g throughout the loop
        percentNAN = str(
            round(g.count("NAN") / len(g) * 100, 2)) + "%"  # Calculate the percentage of NAN value in g

        return percentNAN

    def _time_Formatter(self, c):
        d = []
        for _time_index in range(len(c) - 1, -1,
                                 -1):  # Make a loop for each value of timestamp - starting from the last timestamp
            _todaytime = c[_time_index]
            if str(datetime.strptime(_todaytime,
                                     '%Y-%m-%d %H:%M:%S').date()) == datetime.today().strftime(
                '%Y-%m-%d'):  # logical testing whether the timestamp value is up to dated
                d.append(_todaytime)  # append to d the corresponding timestamp
            else:  # Stop the loop if encountering the previous day
                break

        d.reverse()  # reverse the timestamp for correct order

        return d

    def check_file_status(self):
        Inclinometre_filename = Path(self.filepath).resolve().stem  # Get the inclinometre file name
        if not self._check_file_exists():
            Inclinometre_result = "File not found"
            percentInclinometreNAN = "N/A"

        else:
            df = self._read_file()
            #Case of file corrupted
            if isinstance(df, str):
                percentInclinometreNAN = "N/A"  # Set percentlaserNAN
                Inclinometre_result = "Data corrupted - inconsistent data line"  # Set laser_result

                return Inclinometre_filename, percentInclinometreNAN, Inclinometre_result

            Inclinometre_result = self._isUpdated(df)
            if Inclinometre_result == "Inclinometre file updated":
                _a = list(df.iloc[0, 4:])  # assign the list of value of the first row to the variable _a
                c = list(df.iloc[:, 0])  # assign the list of timestamp to the variable c
                d = self._time_Formatter(c)  # Get the updated timestamps

                _firstcolumnlabel = _a[0]  # assign the name of the first capteur to the variable _firstcolumnlabel
                _lastcolumnlabel = _a[-1]  # assign the name of the last capteur to the variable _lastcolumnlabel

                df.columns = df.iloc[0]  # Set header of dataframe
                _df1 = df.drop(df.index[0])  # drop the duplicated header row
                df2 = _df1.set_index("TIMESTAMP")  # Set index for dataframe
                df2 = df2.loc[d[0]:d[-1], _firstcolumnlabel:_lastcolumnlabel]  # Filtered the dataframe with up to dated timestamp and prism only data
                g = list(df2.loc[d[0],:])  # Assign the list of the prisms value of the first up to dated timestamp to the variable g

                percentInclinometreNAN = self._NAN_calculator(d, df2, g)  # Calculate the percentage of NAN

            else:  # In case of inclinometre file not updated
                percentInclinometreNAN = "N/A"          # Set percentInclinometreNAN

        return Inclinometre_result, percentInclinometreNAN, Inclinometre_filename

class SNCFFileProcessor(FileProcessor, NAN_check):

    def __init__(self, filepath):
        self.filepath = filepath

    def _check_file_exists(self):
        if os.path.isfile(self.filepath):
            return True
        else:
            return False

    def _read_file(self):
        try:
            df = pandas.read_csv(self.filepath, header=None, low_memory=False, skiprows=[0, 2, 3], error_bad_lines=False)
        except pandas.errors.ParserError:  # In case of inconsistent data line
            df = "Data corrupted - inconsistent data line"  # Set laser_result
        return df

    def _isUpdated(self, df):
        _date_time_str = df.iloc[-1, 0]  # Assign the last timestamp to the variable _date_time_str
        _date_time_obj = datetime.strptime(_date_time_str,
                                           '%Y-%m-%d %H:%M:%S')  # Assign the converted timestamp to datetime format to the variable _date_time_obj

        if str(_date_time_obj.date()) != datetime.today().strftime(
                '%Y-%m-%d'):  # Conditional statement checking whether the SNCF file is up to dated
            SNCF_result = "SNCF file not updated"
        else:
            SNCF_result = "SNCF file updated"

        return SNCF_result

    def _NAN_calculator(self, d, df2, g):
        for _timestamp in d:  # Loop for calculating NAN values
            _prismattributes = list(df2.loc[_timestamp,
                                    :])  # Assign each row of df6 dataframe as a list to variable _prismattributes
            g = [_prismattribute if _value != _value and _prismattribute == _prismattribute else _value for
                 _value, _prismattribute in zip(g,
                                                _prismattributes)]  # List comprehension to replace nan value in g throughout the loop
        percentNAN = str(round(sum(math.isnan(x) for x in g if not isinstance(x,str))/len(g)*100,2))+"%"   # Calculate the percentage of nan value in g

        return percentNAN

    def _time_Formatter(self, c):
        d = []
        duplicated = "No"  # Set the the duplicated check
        for _time_index in range(len(c) - 1, -1,
                                 -1):  # Make a loop for each value of timestamp - starting from the last timestamp
            _todaytime = c[_time_index]
            if str(datetime.strptime(_todaytime,
                                     '%Y-%m-%d %H:%M:%S').date()) == datetime.today().strftime(
                '%Y-%m-%d'):  # logical testing whether the timestamp value is up to dated
                d.append(_todaytime)  # append to d the corresponding timestamp
            else:  # Stop the loop if encountering the previous day
                break

        d.reverse()  # reverse the timestamp for correct order

        for i in range(0, len(d) - 1):  # Loop for checking duplicated timestamps
            if d[i] == d[i + 1]:
                duplicated = "Yes"
                break

        if duplicated == "Yes":  # remove duplicated timestamps in list d
            d = list(dict.fromkeys(d))

        return d

    def check_file_status(self):
        try:
            SNCF_filename = Path(self.filepath).resolve().stem  # Get the SNCF file name
            if not self._check_file_exists():
                SNCF_result = "File not found"
                percentnan = "N/A"

            else:
                df = self._read_file()
                #Case of file corrupted
                if isinstance(df, str):
                    percentnan = "N/A"  # Set percentlaserNAN
                    SNCF_result = "Data corrupted - inconsistent data line"  # Set laser_result

                    return SNCF_result, percentnan, SNCF_filename

                SNCF_result = self._isUpdated(df)
                if SNCF_result == "SNCF file updated":
                    _a = list(df.iloc[0, :])  # assign the list of value of the first row to the variable _a
                    b = [prism for prism in _a if prism[0:3] == "DEV" or prism[0:3] == "GAU" or prism[
                                                                                               0:3] == "NIV"]  # create a list b with calculation name only

                    c = list(df.iloc[:, 0])  # assign the list of timestamp to the variable c
                    d = self._time_Formatter(c)  # Get the updated timestamp

                    _firstcolumnlabel = b[0]  # assign the name of the first calculation to the variable _firstcolumnlabel
                    _lastcolumnlabel = b[-1]  # assign the name of the last calculation to the variable _lastcolumnlabel

                    df.columns = df.iloc[0]  # Set header of dataframe
                    df2 = df.set_index("TIMESTAMP")  # Set index for dataframe
                    df2 = df2.loc[d[0]:d[-1], _firstcolumnlabel:_lastcolumnlabel]  # Filtered the dataframe with up to dated timestamp and prism only data
                    df2 = df2[~df2.index.duplicated(
                        keep="first")]  # remove duplicated rows in dataframe df2 with TIMESTAMP as index column
                    g = list(df2.loc[d[0],:])  # Assign the list of the prisms value of the first up to dated timestamp to the variable g

                    percentnan = self._NAN_calculator(d, df2, g)  # Calculate the percentage of NAN

                else:  # In case of laser file not updated
                    percentnan = "N/A"  # Set percentnan

        except:  # In case of other error
            logging.debug('Error on file (%s)' % (self.filepath))  # Write the error file in error log
            SNCF_filename = Path(self.filepath).resolve().stem  # Get the SNCF file name
            percentnan = "N/A"  # Set percentnan
            SNCF_result = "Unknown error"  # Set SNCF_result

        return SNCF_result, percentnan, SNCF_filename

class mesureFileProcessor(FileProcessor, NAN_check):

    def __init__(self, filepath):
        self.filepath = filepath

    def _check_file_exists(self):
        if os.path.isfile(self.filepath):
            return True
        else:
            return False

    def _read_file(self):
        try:
            df = pandas.read_csv(self.filepath, header=None, low_memory=False, skiprows=1)
        except pandas.errors.ParserError:  # In case of inconsistent data line
            df = "Data corrupted - inconsistent data line"  # Set laser_result
        return df

    def _isUpdated(self, df):
        _date_time_str = df.iloc[-1, 0]  # Assign the last timestamp to the variable _date_time_str
        _date_time_obj = datetime.strptime(_date_time_str,
                                           '%Y-%m-%d %H:%M:%S')  # Assign the converted timestamp to datetime format to the variable _date_time_obj

        if str(_date_time_obj.date()) != datetime.today().strftime(
                '%Y-%m-%d'):  # Conditional statement checking whether the laser file is up to dated
            Mesure_result = "file not updated"
        else:
            Mesure_result = "file updated"

        return Mesure_result

    def _NAN_calculator(self, d, df2, g):
        for _timestamp in d:  # Loop for calculating NAN values
            _prismattributes = list(df2.loc[_timestamp,
                                    :])  # Assign each row of df6 dataframe as a list to variable _prismattributes
            g = [_prismattribute if _value == "NAN" and _prismattribute != "NAN" else _value for
                 _value, _prismattribute in zip(g,
                                                _prismattributes)]  # List comprehension to replace NAN value in g throughout the loop
        percentNAN = str(
            round(g.count("NAN") / len(g) * 100, 2)) + "%"  # Calculate the percentage of NAN value in g

        return percentNAN

    def _time_Formatter(self, c):
        d = []
        duplicated = "No"  # Set the the duplicated check
        for _time_index in range(len(c) - 1, -1,
                                 -1):  # Make a loop for each value of timestamp - starting from the last timestamp
            _todaytime = c[_time_index]
            if str(datetime.strptime(_todaytime,
                                     '%Y-%m-%d %H:%M:%S').date()) == datetime.today().strftime(
                '%Y-%m-%d'):  # logical testing whether the timestamp value is up to dated
                d.append(_todaytime)  # append to d the corresponding timestamp
            else:  # Stop the loop if encountering the previous day
                break

        d.reverse()  # reverse the timestamp for correct order

        return d

    def check_file_status(self):
        try:
            mesure_station = Path(self.filepath).resolve().stem  # Get the mesure file name
            if not self._check_file_exists():
                Mesure_result = "File not found"
                percentmesureNAN = "N/A"

            else:
                df = self._read_file()
                #Case of file corrupted
                if isinstance(df, str):
                    percentmesureNAN = "N/A"  # Set percentlaserNAN
                    Mesure_result = "Data corrupted - inconsistent data line"  # Set Mesure_result

                    return Mesure_result, percentmesureNAN, mesure_station

                Mesure_result = self._isUpdated(df)
                if Mesure_result == "file updated":
                    _a = list(df.iloc[0, 3:])  # assign the list of value of the first row to the variable _a
                    c = list(df.iloc[:, 0])  # assign the list of timestamp to the variable c
                    d = self._time_Formatter(c)  # Get the updated timestamp

                    df.columns = df.iloc[0]  # Set header of dataframe
                    _df1 = df.drop(df.index[0])  # drop the duplicated header row
                    df2 = _df1.set_index("TIMESTAMP")  # Set index for dataframe
                    df2 = df2.loc[d[0]:d[-1], _a]  # Filtered the dataframe with up to dated timestamp and prism only data
                    g = list(df2.loc[d[0],:])  # Assign the list of the prisms value of the first up to dated timestamp to the variable g

                    percentmesureNAN = self._NAN_calculator(d, df2, g)  # Calculate the percentage of NAN

                else:  # In case of laser file not updated
                    percentmesureNAN = "N/A"  # Set percentlaserNAN

        except:  # In case of other error
            logging.debug('Error on file (%s)' % (self.filepath))  # Write the error file in error log
            mesure_station = Path(self.filepath).resolve().stem  # Get the mesure file name
            percentmesureNAN = "N/A"  # Set percentmesureNAN
            Mesure_result = "Unknown error"  # Set Mesure_result

        return Mesure_result, percentmesureNAN, mesure_station

class Other1FileProcessor:

    def __init__(self, filepath):
        self.filepath = filepath

    def _check_file_exists(self):
        if os.path.isfile(self.filepath):
            return True
        else:
            return False

    def _read_file(self):
        try:
            df = pandas.read_csv(self.filepath, header=None, low_memory=False, skiprows=1)
        except pandas.errors.ParserError:  # In case of inconsistent data line
            df = "Data corrupted - inconsistent data line"  # Set laser_result
        return df

    def _isUpdated(self, df):
        _date_time_str = df.iloc[-1, 0]  # Assign the last timestamp to the variable _date_time_str
        _date_time_obj = datetime.strptime(_date_time_str,
                                           '%Y-%m-%d %H:%M:%S')  # Assign the converted timestamp to datetime format to the variable _date_time_obj

        if str(_date_time_obj.date()) != datetime.today().strftime(
                '%Y-%m-%d'):  # Conditional statement checking whether the laser file is up to dated
            Mesure_result = "file not updated"
        else:
            Mesure_result = "file updated"

        return Mesure_result


    def check_file_status(self):
        Other1_station = Path(self.filepath).resolve().stem  # Get the mesure file name
        if not self._check_file_exists():
            Other1_result = "File not found"
        else:
            df = self._read_file()
            #Case of file corrupted
            if isinstance(df, str):
                Other1_result = "Data corrupted - inconsistent data line"  # Set Mesure_result

                return Other1_result, Other1_station

            Other1_result = self._isUpdated(df)

        return Other1_result, Other1_station

class status_summary:

    def output_summary(self, stationname, rawdata_def_result, percent99999, percent99997, percent99995, surface_def_result, reference_def_result, percent_def_NAN, corresponding_filepath):
        try:
            if percent_def_NAN != "N/A":
                if rawdata_def_result == "Rawdata file not updated" or surface_def_result[-11:] == "not updated" or \
                        surface_def_result == "Data corrupted - inconsistent data line" or surface_def_result == "File not found" or \
                        reference_def_result == "<3" or float(percent_def_NAN[:-1]) > 50 or surface_def_result == "Unknown error" or rawdata_def_result == "Unknown error":

                    output_dict.setdefault("Station", []).append(stationname)  # appending new value to Station column
                    output_dict.setdefault("Rawdata Status", []).append(
                        rawdata_def_result)  # appending new value to Rawdata Status column
                    output_dict.setdefault("-99999", []).append(percent99999)  # appending new value to -99999 column
                    output_dict.setdefault("-99997", []).append(percent99997)  # appending new value to -99997 column
                    output_dict.setdefault("-99995", []).append(percent99995)  # appending new value to -99995 column
                    output_dict.setdefault("Surface Status", []).append(
                        surface_def_result)  # appending new value to Surface Status column
                    output_dict.setdefault("Reference", []).append(
                        reference_def_result)  # appending new value to Reference column
                    output_dict.setdefault("NAN", []).append(percent_def_NAN)  # appending new value to NAN column
                    output_dict.setdefault("Filepath", []).append(
                        corresponding_filepath)  # appending new value to Filepath column

                    output_dict_sort.setdefault("Station", []).append(stationname)  # appending new value to Station column
                    output_dict_sort.setdefault("Rawdata Status", []).append(
                        rawdata_def_result)  # appending new value to Rawdata Status column
                    output_dict_sort.setdefault("-99999", []).append(percent99999)  # appending new value to -99999 column
                    output_dict_sort.setdefault("-99997", []).append(percent99997)  # appending new value to -99997 column
                    output_dict_sort.setdefault("-99995", []).append(percent99995)  # appending new value to -99995 column
                    output_dict_sort.setdefault("Surface Status", []).append(
                        surface_def_result)  # appending new value to Surface Status column
                    output_dict_sort.setdefault("Reference", []).append(
                        reference_def_result)  # appending new value to Reference column
                    output_dict_sort.setdefault("NAN", []).append(percent_def_NAN)  # appending new value to NAN column
                    output_dict_sort.setdefault("Filepath", []).append(
                        corresponding_filepath)  # appending new value to Filepath column

                else:
                    output_dict.setdefault("Station", []).append(stationname)  # appending new value to Station column
                    output_dict.setdefault("Rawdata Status", []).append(
                        rawdata_def_result)  # appending new value to Rawdata Status column
                    output_dict.setdefault("-99999", []).append(percent99999)  # appending new value to -99999 column
                    output_dict.setdefault("-99997", []).append(percent99997)  # appending new value to -99997 column
                    output_dict.setdefault("-99995", []).append(percent99995)  # appending new value to -99995 column
                    output_dict.setdefault("Surface Status", []).append(
                        surface_def_result)  # appending new value to Surface Status column
                    output_dict.setdefault("Reference", []).append(
                        reference_def_result)  # appending new value to Reference column
                    output_dict.setdefault("NAN", []).append(percent_def_NAN)  # appending new value to NAN column
                    output_dict.setdefault("Filepath", []).append(
                        corresponding_filepath)  # appending new value to Filepath column

            else:
                if rawdata_def_result == "Rawdata file not updated" or surface_def_result[-11:] == "not updated" or \
                        surface_def_result == "Data corrupted - inconsistent data line" or surface_def_result == "File not found" or \
                        reference_def_result == "<3" or surface_def_result == "Unknown error" or rawdata_def_result == "Unknown error":

                    output_dict.setdefault("Station", []).append(stationname)  # appending new value to Station column
                    output_dict.setdefault("Rawdata Status", []).append(
                        rawdata_def_result)  # appending new value to Rawdata Status column
                    output_dict.setdefault("-99999", []).append(percent99999)  # appending new value to -99999 column
                    output_dict.setdefault("-99997", []).append(percent99997)  # appending new value to -99997 column
                    output_dict.setdefault("-99995", []).append(percent99995)  # appending new value to -99995 column
                    output_dict.setdefault("Surface Status", []).append(
                        surface_def_result)  # appending new value to Surface Status column
                    output_dict.setdefault("Reference", []).append(
                        reference_def_result)  # appending new value to Reference column
                    output_dict.setdefault("NAN", []).append(percent_def_NAN)  # appending new value to NAN column
                    output_dict.setdefault("Filepath", []).append(
                        corresponding_filepath)  # appending new value to Filepath column

                    output_dict_sort.setdefault("Station", []).append(
                        stationname)  # appending new value to Station column
                    output_dict_sort.setdefault("Rawdata Status", []).append(
                        rawdata_def_result)  # appending new value to Rawdata Status column
                    output_dict_sort.setdefault("-99999", []).append(
                        percent99999)  # appending new value to -99999 column
                    output_dict_sort.setdefault("-99997", []).append(
                        percent99997)  # appending new value to -99997 column
                    output_dict_sort.setdefault("-99995", []).append(
                        percent99995)  # appending new value to -99995 column
                    output_dict_sort.setdefault("Surface Status", []).append(
                        surface_def_result)  # appending new value to Surface Status column
                    output_dict_sort.setdefault("Reference", []).append(
                        reference_def_result)  # appending new value to Reference column
                    output_dict_sort.setdefault("NAN", []).append(percent_def_NAN)  # appending new value to NAN column
                    output_dict_sort.setdefault("Filepath", []).append(
                        corresponding_filepath)  # appending new value to Filepath column

                else:
                    output_dict.setdefault("Station", []).append(stationname)  # appending new value to Station column
                    output_dict.setdefault("Rawdata Status", []).append(
                        rawdata_def_result)  # appending new value to Rawdata Status column
                    output_dict.setdefault("-99999", []).append(percent99999)  # appending new value to -99999 column
                    output_dict.setdefault("-99997", []).append(percent99997)  # appending new value to -99997 column
                    output_dict.setdefault("-99995", []).append(percent99995)  # appending new value to -99995 column
                    output_dict.setdefault("Surface Status", []).append(
                        surface_def_result)  # appending new value to Surface Status column
                    output_dict.setdefault("Reference", []).append(
                        reference_def_result)  # appending new value to Reference column
                    output_dict.setdefault("NAN", []).append(percent_def_NAN)  # appending new value to NAN column
                    output_dict.setdefault("Filepath", []).append(
                        corresponding_filepath)  # appending new value to Filepath column
        except ValueError:
            logging.debug('Error on file (%s)' % (corresponding_filepath))  # Write the error file in error log


start = time.time()
if __name__ == "__main__":
    output_dict = {}
    output_dict_sort = {}
    status_check = status_summary()
    f = open(r"C:\Users\Astreinte_nord\Desktop\For Daily report - No delete\Summary\Surface Filepaths GC2.txt",
             "r")
    r = open(r"C:\Users\Astreinte_nord\Desktop\For Daily report - No delete\Summary\Rawdata Filepaths GC2.txt",
             "r")
    i = open(
        r"C:\Users\Astreinte_nord\Desktop\For Daily report - No delete\Summary\Inclinometre Filepaths GC2.txt",
        "r", encoding='utf-8')
    s = open(r"C:\Users\Astreinte_nord\Desktop\For Daily report - No delete\Summary\SNCF Filepaths GC2.txt", "r")
    l = open(r"C:\Users\Astreinte_nord\Desktop\For Daily report - No delete\Summary\Laser Filepaths GC2.txt",
             "r")
    m = open(r"C:\Users\Astreinte_nord\Desktop\For Daily report - No delete\Summary\Mesure Filepaths GC2.txt",
             "r", encoding='utf-8')
    # o1 = open(r"C:\Users\Astreinte_nord\Desktop\Khanh - No delete\Khanh - No delete\Summary\Other1 Filepaths GC3.txt",
    #           "r")
    Surface_links = f.readlines()
    Rawdata_links = r.readlines()
    Inclinometre_links = i.readlines()
    SNCF_links = s.readlines()
    Laser_links = l.readlines()
    Mesure_links = m.readlines()
    # Other1_links = o1.readlines()
    for sur_filepath, raw_filepath in zip(Surface_links, Rawdata_links):
        sur_filepath = sur_filepath.strip("\n")
        raw_filepath = raw_filepath.strip("\n")
        surface_file_processor = surfaceFileProcessor(sur_filepath)
        rawdata_file_processor = rawdataFileProcessor(raw_filepath)
        stationname, surface_result, reference_result, percentNAN = surface_file_processor.check_file_status()
        rawdata_result, percent99999, percent99997, percent99995 = rawdata_file_processor.check_file_status()
        status_check.output_summary(stationname, rawdata_result, percent99999, percent99997, percent99995, surface_result,
                       reference_result, percentNAN, sur_filepath)

    for Inclinometre_filepath in Inclinometre_links:
        Inclinometre_filepath = Inclinometre_filepath.strip("\r\n")
        inclinometre_file_processor = InclinometreFileProcessor(Inclinometre_filepath)
        Inclinometre_result, percentInclinometreNAN, Inclinometre_filename = inclinometre_file_processor.check_file_status()
        status_check.output_summary(Inclinometre_filename, "N/A", "N/A", "N/A", "N/A", Inclinometre_result, "N/A",
                       percentInclinometreNAN, Inclinometre_filepath)

    for SNCF_filepath in SNCF_links:
        SNCF_filepath = SNCF_filepath.strip("\n")
        SNCF_file_processor = SNCFFileProcessor(SNCF_filepath)
        SNCF_result, percentnan, SNCF_filename = SNCF_file_processor.check_file_status()
        status_check.output_summary(SNCF_filename, "N/A", "N/A", "N/A", "N/A", SNCF_result, "N/A", percentnan, SNCF_filepath)

    for Laser_filepath in Laser_links:
        Laser_filepath = Laser_filepath.strip("\n")
        laser_file_processor = laserFileProcessor(Laser_filepath)
        laser_station, percentlaserNAN, laser_result = laser_file_processor.check_file_status()
        status_check.output_summary(laser_station, "N/A", "N/A", "N/A", "N/A", laser_result, "N/A", percentlaserNAN, Laser_filepath)

    for Mesure_filepath in Mesure_links:
        Mesure_filepath = Mesure_filepath.strip("\n")
        mesure_file_processor = mesureFileProcessor(Mesure_filepath)
        Mesure_result, percentmesureNAN, mesure_station = mesure_file_processor.check_file_status()
        status_check.output_summary(mesure_station, "N/A", "N/A", "N/A", "N/A", Mesure_result, "N/A", percentmesureNAN,
                       Mesure_filepath)

    # for Other1_filepath in Other1_links:
    #     Other1_filepath = Other1_filepath.strip("\n")
    #     other1_file_processor = Other1FileProcessor(Other1_filepath)
    #     Other1_result, Other1_station = other1_file_processor.check_file_status()
    #     status_check.output_summary(Other1_station, "N/A", "N/A", "N/A", "N/A", Other1_result, "N/A", "N/A", Other1_filepath)

    df10 = pandas.DataFrame(output_dict)
    df10.to_csv(r'C:\Users\Astreinte_nord\Desktop\For Daily report - No delete\Summary\Summary GC2.csv',
                index=False)

    df11 = pandas.DataFrame(output_dict_sort)
    df11.to_csv(r'C:\Users\Astreinte_nord\Desktop\For Daily report - No delete\Summary\Summary Sorted GC2.csv',
                index=False)

end = time.time()

print(f"Runtime of the program is {end - start}")

# merge optical data with sensor data
# last updated: 2025.2.19

import numpy as np
import matplotlib.pyplot as plt
import time
import pandas as pd
import os
from glob import glob

from tables import open_file, Filters
from tables import Float32Col, Float64Col, Int16Col, Int32Col, Int64Col
from tables import UInt16Col, UInt32Col, UInt64Col
import traceback

from utility import header, unixTimeToTimestamp, load_conf


def fillRdfTables(fileName, spectrumDict, attrs=None):
    """Save data from spectrumDict to tables in an HDF5 output file.

    Args:
        fileName: Name of HDF5 file to receive spectra
        spectrumDict: This is a dictionary corresponding to 1 optical file/one spectrum. It consists
            of a dictionary with keys "rdData", "sensorData", "tagalongData" and "controlData". The
            values are tables of data (stored as a dictionary whose keys are the column names and whose
            values are lists of the column data) which are to be written to the output file.
        attrs: Dictionary of attributes to be written to HDF5 file
    """
    hdf5Filters = Filters(complevel=1, fletcher32=True)
    try:
        hdf5Handle = open_file(fileName, "w")
        if attrs is not None:
            for a in attrs:
                setattr(hdf5Handle.root._v_attrs, a, attrs[a])
        # Lookup table giving pyTables column generation function keyed
        # by the numpy dtype.name
        colByName = dict(
            float32=Float32Col,
            float64=Float64Col,
            int16=Int16Col,
            int32=Int32Col,
            int64=Int64Col,
            uint16=UInt16Col,
            uint32=UInt32Col,
            uint64=UInt64Col,
        )

        # We make HDF5 tables and define the columns needed in these tables
        tableDict = {}
        # Iterate over rdData, sensorData, tagalongData and controlData tables
        for tableName in spectrumDict:
            if tableName in [
                "rdData",
                "sensorData",
                "tagalongData",
                "controlData",
            ]:
                spectTableData = spectrumDict[tableName]
                if len(spectTableData) > 0:
                    keys, values = list(zip(*sorted(spectTableData.items())))
                    if tableName not in tableDict:
                        # We are encountering this table for the first time, so we
                        #  need to build up colDict whose keys are the column names and
                        #  whose values are the subclasses of Col used by pytables to
                        #  define the HDF5 column. These are retrieved from colByName.
                        colDict = {}
                        # Use numpy to get the dtype names for the various data
                        values = [np.asarray(v) for v in values]
                        for key, value in zip(keys, values):
                            colDict[key] = colByName[value.dtype.name]()
                        tableDict[tableName] = hdf5Handle.create_table(
                            hdf5Handle.root,
                            tableName,
                            colDict,
                            filters=hdf5Filters,
                        )
                    table = tableDict[tableName]
                    # Go through the arrays in values and fill up each row of the table
                    #  one element at a time
                    row = table.row
                    for j in range(len(values[0])):
                        for i, key in enumerate(keys):
                            try:
                                row[key] = values[i][j]
                            except KeyError:
                                pass
                        row.append()
                    table.flush()
    except:
        print(traceback.format_exc())
    finally:
        hdf5Handle.close()


#fixing to a new center of circle
#this assumes no large outliers

def find_circle_centers(X,Y):
    #see https://lucidar.me/en/mathematics/least-squares-fitting-of-circle/ for math

    A = np.vstack([X,Y,np.ones(X.size)]).T
    B = X**2 + Y**2
    C = np.linalg.pinv(A) @ B

    x_center = C[0] / 2
    y_center = C[1] / 2
    radius = np.sqrt(4*C[2] + C[0]**2 + C[1]**2)/2

    return x_center, y_center, radius


def convert_to_rdf(optical_path, sensor_data_list, out_path, cal_file):
    """combine optical file and its corresponding sensor data then save as RDF h5 file.

    Args:
        optical_path: path of optical data csv
        sensor_data_list: list of paths of all sensor data csv files for this optical file
        out_path: path of output h5 file, will have the same file name as optical file
        cal_file
    """

    rd_data = np.genfromtxt(optical_path, delimiter=',',
                            names=True,
                            dtype=None, encoding='utf-8')

    ##########################################################
    num_rd = rd_data['timestamp'].size

    # cal_file = R"laser_cal.npz"
    if cal_file is not None:
        from laser_cal import Laser_Cal
        laser_cal_obj = Laser_Cal()
        laser_cal_obj.load_cal(cal_file, only_phi_to_freq=True)
        have_cal_file = True
    else:
        have_cal_file = False

        x_center, y_center, radius = find_circle_centers(
        rd_data["ratio1"],
        rd_data["ratio2"])

        # plt.figure(figsize=(6,6))
        # plt.plot(rd_data["ratio1"], rd_data["ratio2"], ".")
        # circle1 = plt.Circle((x_center, y_center), radius, edgecolor='r', fill=False, linewidth=2, zorder=10)
        # plt.gca().add_patch(circle1)
        # plt.title("find circle center")

        wlm_angle_recalc = np.arctan2(rd_data["ratio2"] - y_center, rd_data["ratio1"] - x_center)
        wlm_angle_recalc = rd_data['anglesSetpoint'] - np.pi + (wlm_angle_recalc - rd_data['anglesSetpoint'] + np.pi) % (2*np.pi)

    # print(wlm_angle_recalc)

    spectrumDict = {
        "rdData": {},
        "sensorData": {},
        "tagalongData": {},
        "controlData": {},
        "schemeInfo": None,
    }

    c = 29979.2458 #speed of light cm / us
    spectrumDict['rdData']["timestamp"] = np.asarray([unixTimeToTimestamp(t) for t in rd_data['timestamp']]) #UNIX epoch time in seconds -> picarro timestamp in ms  
    spectrumDict['rdData']["wlmAngle"] = rd_data['wlm_angle']
    spectrumDict['rdData']["waveNumberSetpoint"] = rd_data["waveNumberSetpoint"]
    spectrumDict['rdData']["uncorrectedAbsorbance"] = 1e6 / (c * rd_data['ringdown_time']) #unit conversion: us -> ppm/cm
    # spectrumDict['rdData']["correctedAbsorbance"] = # obsolete
    # spectrumDict['rdData']["status"] = ?????
    spectrumDict['rdData']["count"] = np.ones(num_rd, dtype=int)
    spectrumDict['rdData']["pztValue"] = rd_data['Cavity_phase']
    spectrumDict['rdData']["laserUsed"] = np.ones(num_rd, dtype=int)
    spectrumDict['rdData']["subschemeId"] = rd_data["subschemeID"].astype(int) #includes fit flag 32768, 16384 ignore, 8192 is pzt center, 4096 enable cal  
    spectrumDict['rdData']["schemeRow"] = rd_data['schemeRow'].astype(int) 
    spectrumDict['rdData']["ratio1"] = np.rint(rd_data["ratio1"] * 32768).astype(int) 
    spectrumDict['rdData']["ratio2"] = np.rint(rd_data["ratio2"] * 32768).astype(int) 
    spectrumDict['rdData']["coarseLaserCurrent"] = rd_data["laser_phase"].astype(int)
    spectrumDict['rdData']["fitAmplitude"] = rd_data["fit_amplitude"]
    spectrumDict['rdData']["fitBackground"] = rd_data["fit_offset"]
    spectrumDict['rdData']["fitRmsResidual"] = rd_data['fit_rms_residual']
    spectrumDict['rdData']["frontMirrorDac"] = rd_data['front_mirror'].astype(int) 
    spectrumDict['rdData']["backMirrorDac"] = rd_data['back_mirror'].astype(int) 
    spectrumDict['rdData']["gainCurrentDac"] = rd_data['laser_gain'].astype(int)
    spectrumDict['rdData']["soaCurrentDac"] = rd_data['laser_SOA'].astype(int)
    spectrumDict['rdData']["coarsePhaseDac"] = rd_data['laser_phase'].astype(int) # before phase temp correction 
    spectrumDict['rdData']["extra1"] = rd_data['extra1'].astype(int) 
    spectrumDict['rdData']["extra2"] = rd_data['extra2'].astype(int)
    spectrumDict['rdData']["extra3"] = rd_data['extra3'].astype(int)
    spectrumDict['rdData']["extra4"] = rd_data['extra4'].astype(int) 
    spectrumDict['rdData']["sequenceNumber"] = np.arange(1,num_rd+1) #continually incrementing
    spectrumDict['rdData']["average1"] = (rd_data['wlm_eta1'] +  rd_data['wlm_ref1']) / 2
    spectrumDict['rdData']["average2"] = (rd_data['wlm_eta2'] +  rd_data['wlm_ref2']) / 2
    spectrumDict['rdData']["modeIndex"] = rd_data['modeIndex'].astype(int)
    # spectrumDict['rdData']["pztCntrlRef"] = fast pzt
    # spectrumDict['rdData']["cosPztCntrlRef"] = fast pzt
    # spectrumDict['rdData']["sinPztCntrlRef"] = fast pzt

    #keys being faked

    # dont have schemeVersionAndTable, adding a fake. Everything is going to be from scheme table 1 with python scheme version (1): 
    # result is 17 for all rd (16 * schemeVersion + schemeTable) 
    spectrumDict['rdData']["schemeVersionAndTable"] = 17 + np.zeros(num_rd).astype(int)

    if have_cal_file:    
        spectrumDict['rdData']["waveNumber"] = laser_cal_obj.convert_ratios_to_freq(
                                                    rd_data['waveNumberSetpoint'],
                                                    rd_data["ratio1"],
                                                    rd_data["ratio2"])
        spectrumDict['rdData']["angleSetpoint"] = rd_data['anglesSetpoint']
    else:
        spectrumDict['rdData']["waveNumber"] = rd_data['waveNumberSetpoint']
        spectrumDict['rdData']["angleSetpoint"] = wlm_angle_recalc

    spectrumDict['rdData']["cavityPressure"] = 140 * np.ones(num_rd) # needs merging 

    #keys being filled with zeros
    spectrumDict['rdData']["tunerValue"] = np.zeros(num_rd, dtype=int) #dont have this one
    spectrumDict['rdData']["ringdownThreshold"] = np.zeros(num_rd, dtype=int) #dont have this one
    spectrumDict['rdData']["fineLaserCurrent"] = np.zeros(num_rd, dtype=int) # dont have this
    spectrumDict['rdData']["laserTemperature"] = np.zeros(num_rd) # needs merging
    spectrumDict['rdData']["etalonTemperature"] = np.zeros(num_rd) # This one is a mess, the thermister is hooked to the wrong WLM
    spectrumDict['rdData']["finePhaseDac"] = np.zeros(num_rd, dtype=int) # dont have this one

    # new keys being added
    spectrumDict['rdData']["opticalPhase"] = rd_data['OF_phase']
    spectrumDict['rdData']["eta1"] = rd_data['wlm_eta1']
    spectrumDict['rdData']["eta2"] = rd_data['wlm_eta2']
    spectrumDict['rdData']["ref1"] = rd_data['wlm_ref1']
    spectrumDict['rdData']["ref2"] = rd_data['wlm_ref2']
    spectrumDict['rdData']["dwells"] = rd_data['dwells'].astype(int)
    spectrumDict['rdData']["OF_tune"] = rd_data['OF_tune']
    spectrumDict['rdData']["transient_mult"] = rd_data['transient_mult']
    spectrumDict['rdData']["FSRDisplaced"] = rd_data['FSRDisplaced'].astype(int)
    spectrumDict['rdData']["laser_gain"] = rd_data['laser_gain']
    spectrumDict['rdData']["laser_SOA"] = rd_data['laser_SOA']
    
    #controlData
    fit_flag = spectrumDict['rdData']["subschemeId"] // 32768 == 1
    num_spectra = np.sum(fit_flag)
    #this counts the ringdowns in each spectra (from one fit flag to the next)
    spectrumDict["controlData"]['RDDataSize'] = np.diff(np.concatenate((np.asarray([-1]), np.nonzero(fit_flag)[0])))
    spectrumDict["controlData"]['SpectrumQueueSize'] = np.zeros(num_spectra)
    spectrumDict["controlData"]['Latency'] = np.zeros(num_spectra)

    # sensor data
    # merge all data in the sensor data list
    combined_df = pd.concat(map(pd.read_csv, sensor_data_list))  # , ignore_index=True)
    for item in header:
        spectrumDict["sensorData"][item] = combined_df[item].tolist()

    # save spectrumDict to h5 file
    fillRdfTables(out_path, spectrumDict)


if __name__ == "__main__":
    t0 = time.time()

    conf = load_conf()
    print(conf)
    optical_folder_path = conf["optical_folder_path"]
    sensor_folder_path = conf["sensor_folder_path"]
    output_folder = conf["output_folder"]

    # get the optical data time range and file list
    p = os.path.join(optical_folder_path, "*.csv")
    ls = glob(p)  # path
    optical_file_list = []
    # only include sensor folders within this date range
    date_range = []  # ['20250123', '20250124']
    
    for p in ls:
        x = p.split('/')[-1]
        f1 = x[:-4]  # 20250123_1519
        try:
            t1 = int(time.mktime(time.strptime(f1, "%Y%m%d_%H%M")))
            optical_file_list.append(f1)
            if f1[:8] not in date_range:
                date_range.append(f1[:8])
        except:
            # file does not have the specified format, not what we want, skip
            pass

    optical_file_list.sort()  # ['20250122_1336']
    print("* total number of optical files: ", len(optical_file_list))

    # match optical data with the list of sensor data covering it
    optical_time_list = []  # [start, end] list, day_hour+minute
    i = 1
    for op in optical_file_list:
        if i % 20 == 0:
            print("... %s optical files read" % i)
        i += 1

        p1 = os.path.join(optical_folder_path, op + '.csv')
        df = pd.read_csv(p1)
        # Access a specific value using row and column index
        end_epoch = df.iloc[-1, 1] + 18000  # last row, timestamp, (+5h)
        end_file = time.strftime('%Y%m%d_%H%M', time.localtime(end_epoch))
        optical_time_list.append([op, end_file])  # ["20250123_1519", "20250123_1525"]
    print("* End time of all optical files extracted.")

    sensor_folder_list = []
    for day in date_range:
        sensor_folder_list.append("Sensors_" + day)

    # pick all available sensor file paths
    temp = []
    for item in sensor_folder_list:
        print(item)
        p = os.path.join(sensor_folder_path, item, "*.csv")
        ls = glob(p)
        # if folder does not exist, ls=[]
        temp.append(ls)
    sensor_file_path_list = sum(temp, [])  # flatten 2d list to 1d
    sensor_file_path_list.sort()

    # create dictionary, # {optical file name: list of sensor file path}
    sensor_dynamic_list = sensor_file_path_list
    matchDict = {}
    
    for (start, end) in optical_time_list:
        matchDict[start] = []
        # print("sensor list length: ", len(sensor_dynamic_list))
        x = []
        for s in sensor_dynamic_list:
            filename = (s.split('/')[-1])[:-4]  # 20250123_1248
    
            if start <= filename < end:  # < end, avoid duplicate, but may include less data (< 1 min)
                x.append(s)
    
            if filename == end:
                # create dictionary
                matchDict[start] = x
                idx = sensor_dynamic_list.index(s)
                sensor_dynamic_list = sensor_dynamic_list[idx:]
                break
    # print(matchDict)

    # create h5
    i = 1
    for op in optical_file_list:
        if i % 20 == 0:
            print("... %s RDF files created" % i)
        i += 1

        if matchDict[op]:
            p1 = os.path.join(optical_folder_path, op + '.csv')
            out_path = os.path.join(output_folder, op+'.h5')
            try:
                convert_to_rdf(p1, matchDict[op], out_path, None)
                # print("created RDF for optical file: %s.csv" % op)
            except:
                print("Failed to create RDF file for: %s.csv " % op)
        else:
            print("No sensor data for optical file: %s.csv" % op)

    t = time.time() - t0
    print("* Merge finished! took %.2f min " % (t/60))




# @author: Yilin Shi | 2025.1.29
# shiyilin890@gmail.com
# Bog the Fat Crocodile vvvvvvv
#                       ^^^^^^^
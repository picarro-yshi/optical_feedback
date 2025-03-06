# stream sensor data to r-drive.
# last updated: 2025.3.6

import os
import queue
import time
import pandas as pd

from ctypes import c_ubyte, c_byte, c_uint, c_int, c_ushort, c_short
from ctypes import c_longlong, c_float, c_double, Structure, Union, sizeof

from Listener_py3 import Listener

# RPC_PORT_DRIVER = 50010
BROADCAST_PORT_SENSORSTREAM = 40020


class SensorEntryType(Structure):
    _fields_ = [
        ("timestamp", c_longlong),
        ("streamNum", c_uint),
        ("value", c_float)
    ]

from utility import header, unixTime, load_conf

# stream_num, keys used in STREAM_MemberTypeDict
# 4, 5, 6, 28, 7, 8, 29, 30, 35   # save frequency 5/s
# 2, 26, 3, 27, 9, 20, 21, 31, 32, 33, 34, 61, 62, 63, 10, 11, 12, 13, 14, 15  # freq = 1/s

# Dictionary, {stream_num: position in list 'sensor'}
sensorNumberDict = {
    'timestamp': 0,
    2: 1,  # save frequency 5/s
    26: 2,
    3: 3,
    27: 4,
    9: 5,
    20: 6,
    21: 7,
    31: 8,
    32: 9,
    33: 10,
    34: 11,
    61: 12,
    62: 13,
    63: 14,
    10: 15,
    11: 16,
    12: 17,
    13: 18,
    14: 19,
    15: 20,

    4: 21,  # save frequency 1/5s
    5: 22,
    6: 23,
    28: 24,
    7: 25,
    8: 26,
    29: 27,
    30: 28,
    35: 29,
}
COLUMN_NUM = len(sensorNumberDict)  # column number in csv file


if __name__ == "__main__":
    conf = load_conf()
    analyzerIP = conf["analyzerIP"]  # "10.100.3.36"
    SENSOR_FOLDER = conf["sensor_folder_path"]
    save_time = conf["save_interval"]  # 60, save csv every 60s
    LOCAL_FOLDER = conf["local_folder_path"]

    t0 = int(time.time())
    epoch = 0
    huge_list = []
    sensor = [0] * COLUMN_NUM
    day_folder = 'Sensors_' + time.strftime("%Y%m%d")

    subfolder = os.path.join(SENSOR_FOLDER, day_folder)
    if not os.path.isdir(subfolder):
        os.mkdir(subfolder)

    subfolder = os.path.join(LOCAL_FOLDER, day_folder)
    if not os.path.isdir(subfolder):
        os.mkdir(subfolder)

    q = queue.Queue(100)
    listener = Listener(
        queue=q,
        host=analyzerIP,
        port=BROADCAST_PORT_SENSORSTREAM,
        elementType=SensorEntryType,
        retry=True,
        name="Sensor stream listener",
    )
    
    uncopied = []  # uncopied csv files, try again later
    if not os.path.isdir("../temp"):
        os.mkdir("../temp")

    try:
        print("start recording sensor data, press ctrl+C to quit...")
        while True:
            data = q.get(timeout=10)
            utime = unixTime(data.timestamp)
            stream_num = data.streamNum
            value = data.value
            # print(utime, stream_num, value)

            if utime != epoch:  # start the next sensor list
                if sensor[0] != 0:
                    huge_list.append(sensor)
                sensor = [0] * COLUMN_NUM

            # create huge list
            if not sensor[0]:  # initiate
                epoch = utime
                sensor[0] = epoch

            try:
                sensor[sensorNumberDict[stream_num]] = value

                t = int(time.time())
                if t - t0 > save_time:
                    # data failed to save to r-drive previously: try again
                    if uncopied:
                        for i in range(len(uncopied)):
                            try:
                                f0 = uncopied[0]
                                shutil.copy2("../temp/" + f0, os.path.join(SENSOR_FOLDER, 'Sensors_' + f0[:8], f0))
                                print("* copy to r-drive successful: %s" % f0)
                                uncopied.pop(0)
                                os.remove("../temp/" + f0)
                            except:
                                pass

                    # create csv
                    my_df = pd.DataFrame(huge_list)

                    f1 = time.strftime("%Y%m%d_%H%M") + '.csv'
                    today = 'Sensors_' + time.strftime("%Y%m%d")
                    if day_folder != today:
                        subfolder = os.path.join(SENSOR_FOLDER, today)
                        os.mkdir(subfolder)
                        subfolder = os.path.join(LOCAL_FOLDER, today)
                        os.mkdir(subfolder)
                        day_folder = today
                        print("a new day just started: ", time.ctime())

                    p = os.path.join(LOCAL_FOLDER, day_folder, f1)
                    my_df.to_csv(p, index=False, header=header)

                    p = os.path.join(SENSOR_FOLDER, day_folder, f1)
                    try:
                        my_df.to_csv(p, index=False, header=header)
                    except:
                        # save locally then move later
                        my_df.to_csv("../temp/" + f1, index=False, header=header)
                        uncopied.append(f1)
                        print("! save to r-drive failed: %s, will try again later." % f1)
                    t0 = t
                    huge_list = []
            except:  # skip value not in dictionary keys
                pass

    except KeyboardInterrupt:
        pass


# @author: Yilin Shi | 2025.1.29
# shiyilin890@gmail.com
# Bog the Fat Crocodile vvvvvvv
#                       ^^^^^^^


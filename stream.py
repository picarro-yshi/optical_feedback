# stream sensor data to r-drive.
# last updated: 2025.2.19

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
    10: 12,
    11: 13,
    12: 14,
    13: 15,
    14: 16,
    15: 17,

    4: 18,  # save frequency 1/5s
    5: 19,
    6: 20,
    28: 21,
    7: 22,
    8: 23,
    29: 24,
    30: 25,
    35: 26,
}
COLUMN_NUM = len(sensorNumberDict)  # column number in csv file


if __name__ == "__main__":
    conf = load_conf()
    analyzerIP = conf["analyzerIP"]  # "10.100.3.36"
    SENSOR_FOLDER = conf["sensor_folder_path"]
    save_time = conf["save_interval"]  # 60, save csv every 60s

    t0 = int(time.time())
    epoch = 0
    huge_list = []
    sensor = [0] * COLUMN_NUM

    day_folder = 'Sensors_' + time.strftime("%Y%m%d")
    subfolder = os.path.join(SENSOR_FOLDER, day_folder)
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
                    # create csv
                    my_df = pd.DataFrame(huge_list)

                    f1 = time.strftime("%Y%m%d_%H%M") + '.csv'
                    today = 'Sensors_' + time.strftime("%Y%m%d")
                    if day_folder != today:
                        subfolder = os.path.join(SENSOR_FOLDER, today)
                        os.mkdir(subfolder)
                        day_folder = today
                        print("a new day just started: ", time.ctime())

                    p = os.path.join(SENSOR_FOLDER, day_folder, f1)
                    try:
                        my_df.to_csv(p, index=False, header=header)
                        # print("file saved:  %s" % f1)
                    except:
                        print('error save csv')
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


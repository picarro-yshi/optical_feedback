# constants, utility functions
import datetime

header = [
    "timestamp",
    "Laser3Temp",
    "Laser3Tec",
    "Laser4Temp",
    "Laser4Tec",
    "DasTemp",
    "CavityPressure",
    "AmbientPressure",
    "InletValve",
    "OutletValve",
    "ValveMask",
    "MPVPosition",
    "Etalon1",
    "Reference1",
    "Etalon2",
    "Reference2",
    "Ratio1",
    "Ratio2",

    "EtalonTemp",
    "WarmBoxTemp",
    "WarmBoxHeatsinkTemp",
    "WarmBoxTec",
    "CavityTemp",
    "HotBoxHeatsinkTemp",
    "HotBoxTec",
    "HotBoxHeater",
    "FanState",
]


ORIGIN = datetime.datetime(datetime.MINYEAR, 1, 1, 0, 0, 0, 0)
UNIXORIGIN = datetime.datetime(1970, 1, 1, 0, 0, 0, 0)

def unixTime(timestamp):
    dt = (ORIGIN - UNIXORIGIN) + datetime.timedelta(microseconds=1000 * float(timestamp))
    return 86400.0 * dt.days + dt.seconds + 1.e-6 * dt.microseconds

def unixTimeToTimestamp(u):
    dt = UNIXORIGIN + datetime.timedelta(seconds=float(u))
    return datetimeToTimestamp(dt)

def datetimeToTimestamp(t):
    td = t - ORIGIN
    return (td.days * 86400 + td.seconds) * 1000 + td.microseconds // 1000


################# for reference ####################
# from Host.autogen interface.py
STREAM_MemberTypeDict = {}
STREAM_MemberTypeDict[0] = 'STREAM_Laser1Temp'  #
STREAM_MemberTypeDict[1] = 'STREAM_Laser2Temp'  #
STREAM_MemberTypeDict[2] = 'STREAM_Laser3Temp'  #
STREAM_MemberTypeDict[3] = 'STREAM_Laser4Temp'  #
STREAM_MemberTypeDict[4] = 'STREAM_EtalonTemp'  #
STREAM_MemberTypeDict[5] = 'STREAM_WarmBoxTemp'  #
STREAM_MemberTypeDict[6] = 'STREAM_WarmBoxHeatsinkTemp'  #
STREAM_MemberTypeDict[7] = 'STREAM_CavityTemp'  #
STREAM_MemberTypeDict[8] = 'STREAM_HotBoxHeatsinkTemp'  #
STREAM_MemberTypeDict[9] = 'STREAM_DasTemp'  #
STREAM_MemberTypeDict[10] = 'STREAM_Etalon1'  #
STREAM_MemberTypeDict[11] = 'STREAM_Reference1'  #
STREAM_MemberTypeDict[12] = 'STREAM_Etalon2'  #
STREAM_MemberTypeDict[13] = 'STREAM_Reference2'  #
STREAM_MemberTypeDict[14] = 'STREAM_Ratio1'  #
STREAM_MemberTypeDict[15] = 'STREAM_Ratio2'  #
STREAM_MemberTypeDict[16] = 'STREAM_Laser1Current'  #
STREAM_MemberTypeDict[17] = 'STREAM_Laser2Current'  #
STREAM_MemberTypeDict[18] = 'STREAM_Laser3Current'  #
STREAM_MemberTypeDict[19] = 'STREAM_Laser4Current'  #
STREAM_MemberTypeDict[20] = 'STREAM_CavityPressure'  #
STREAM_MemberTypeDict[21] = 'STREAM_AmbientPressure'  #
STREAM_MemberTypeDict[22] = 'STREAM_Cavity2Pressure'  #
STREAM_MemberTypeDict[23] = 'STREAM_Ambient2Pressure'  #
STREAM_MemberTypeDict[24] = 'STREAM_Laser1Tec'  #
STREAM_MemberTypeDict[25] = 'STREAM_Laser2Tec'  #
STREAM_MemberTypeDict[26] = 'STREAM_Laser3Tec'  #
STREAM_MemberTypeDict[27] = 'STREAM_Laser4Tec'  #
STREAM_MemberTypeDict[28] = 'STREAM_WarmBoxTec'  #
STREAM_MemberTypeDict[29] = 'STREAM_HotBoxTec'  #
STREAM_MemberTypeDict[30] = 'STREAM_HotBoxHeater'  #
STREAM_MemberTypeDict[31] = 'STREAM_InletValve'  #
STREAM_MemberTypeDict[32] = 'STREAM_OutletValve'  #
STREAM_MemberTypeDict[33] = 'STREAM_ValveMask'  #
STREAM_MemberTypeDict[34] = 'STREAM_MPVPosition'  #
STREAM_MemberTypeDict[35] = 'STREAM_FanState'  #
STREAM_MemberTypeDict[36] = 'STREAM_ProcessedLoss1'  #
STREAM_MemberTypeDict[37] = 'STREAM_ProcessedLoss2'  #
STREAM_MemberTypeDict[38] = 'STREAM_ProcessedLoss3'  #
STREAM_MemberTypeDict[39] = 'STREAM_ProcessedLoss4'  #
STREAM_MemberTypeDict[40] = 'STREAM_Flow1'  #
STREAM_MemberTypeDict[41] = 'STREAM_Battery_Voltage'  #
STREAM_MemberTypeDict[42] = 'STREAM_Battery_Current'  #
STREAM_MemberTypeDict[43] = 'STREAM_Battery_Charge'  #
STREAM_MemberTypeDict[44] = 'STREAM_Battery_Temperature'  #
STREAM_MemberTypeDict[45] = 'STREAM_AccelX'  #
STREAM_MemberTypeDict[46] = 'STREAM_AccelY'  #
STREAM_MemberTypeDict[47] = 'STREAM_AccelZ'  #
STREAM_MemberTypeDict[48] = 'STREAM_CavityTemp1'  #
STREAM_MemberTypeDict[49] = 'STREAM_CavityTemp2'  #
STREAM_MemberTypeDict[50] = 'STREAM_CavityTemp3'  #
STREAM_MemberTypeDict[51] = 'STREAM_CavityTemp4'  #
STREAM_MemberTypeDict[52] = 'STREAM_Cavity2Temp1'  #
STREAM_MemberTypeDict[53] = 'STREAM_Cavity2Temp2'  #
STREAM_MemberTypeDict[54] = 'STREAM_Cavity2Temp3'  #
STREAM_MemberTypeDict[55] = 'STREAM_Cavity2Temp4'  #
STREAM_MemberTypeDict[56] = 'STREAM_FilterHeaterTemp'  #
STREAM_MemberTypeDict[57] = 'STREAM_FilterHeater'  #
STREAM_MemberTypeDict[58] = 'STREAM_Cavity2Temp'  #


# stream data keys:
# STREAM_EtalonTemp
# STREAM_WarmBoxTemp
# STREAM_WarmBoxHeatsinkTemp
# STREAM_WarmBoxTec
# STREAM_CavityTemp
# STREAM_HotBoxHeatsinkTemp
# STREAM_HotBoxTec
# STREAM_HotBoxHeater
# STREAM_FanState
# STREAM_Laser3Temp
# STREAM_Laser3Tec
# STREAM_Laser4Temp
# STREAM_Laser4Tec
# STREAM_DasTemp
# STREAM_CavityPressure
# STREAM_AmbientPressure
# STREAM_InletValve
# STREAM_OutletValve
# STREAM_ValveMask
# STREAM_MPVPosition
# STREAM_Etalon1
# STREAM_Reference1
# STREAM_Etalon2
# STREAM_Reference2
# STREAM_Ratio1
# STREAM_Ratio2

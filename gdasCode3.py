###############################################################################
# Copyright (c) 2018, GNOME Collaboration.
#
# Produced at Johannes Gutenberg University Mainz.
#
# Written by J. Smiga (joseph.smiga@gmail.com).
#
# All rights reserved.
#
# This file is part of GDAS.
#
# For details, see github.com/vincentdumont/gdas.
#
# For details about use and distribution, please read GDAS/LICENSE.
###############################################################################
# Code for reading GNOME data.
#

# EDITED FROM ORIGINAL 
# --Claire

# Summary of contents:
# getDataFromFile()-----Gets magnetometer data from single file.
# getFListInRange()-----Gets list of files in time range.
# getDataInRange()------Gets list of data from files in time range.
# getFileNameTime()-----Gets file time from its name.
# getFListFromDates()---Gets list of file names from dates.
# getStartTimes()-------Gets list of start times for set of data files.
# getSaneList()---------Finds which data files contain only 'sane' data.

## WRAPPER FUNCTIONS by Ibrahim

from gwpy.timeseries import TimeSeries,TimeSeriesList
import h5py
import time, calendar
from os import listdir
from os.path import isfile, join
from gwpy.time import to_gps,from_gps

# class TimeSeries: #for no wifi tests w/ fully simulated data
#     def __init__(self, value, sample_rate, t0=None):
#         self.value = value
#         self.sample_rate = sample_rate
#         self.t0 = t0
#     def __iter__(self):
#         return iter(self.value)
    

def getDataFromFile(fName, startUnix, endUnix, firstFile, lastFile, convert = False, verbose = False, debug = True):
    '''
    Gets magnetometer data from file.
    
    fName: str
        Name of file
    convert: boolean (default: False)
        Whether to use conversion function from file.
        
    returns (data, sanity data) as astropy TimeSeries
    
    Note: must evaluate values in 'sanity' (e.g., using 'value' attribute) to get boolean
    '''
    h5pyFile = h5py.File(fName,'r')
    saneList = h5pyFile['SanityChannel']
    dataList = h5pyFile['MagneticFields']
    
    # get mag field attributes
    attrs = dataList.attrs
    sampRate = float(attrs['SamplingRate(Hz)'])
    startT = calendar.timegm(time.strptime(attrs['Date']+' '+attrs['t0'], '%Y/%m/%d %H:%M:%S.%f'))
    endT = calendar.timegm(time.strptime(attrs['Date']+' '+attrs['t1'], '%Y/%m/%d %H:%M:%S.%f'))


    if debug: 
        print("\nattrs['Date']:\t{}".format(attrs['Date']))
        print("attrs['t0']:\t{}".format(attrs['t0']))
        print("startT:\t{}".format(startT))
        print("attrs.items():\t{}".format(attrs.items()))
        
    # get milliseconds
    decPt = attrs['t0'].rfind('.')
    if(decPt >= 0): # has decimal point
        startT += float('0'+attrs['t0'][decPt:])
    if verbose: print("getDataFromFile() --- got t0 = {}, tf = {}".format(startT, endT))
        
    # get sanity attributes
    saneAttrs = saneList.attrs
    saneRate = float(saneAttrs['SamplingRate(Hz)'])
    saneStart = calendar.timegm(time.strptime(saneAttrs['Date']+' '+saneAttrs['t0'], '%Y/%m/%d %H:%M:%S.%f'))
    # get milliseconds
    decPt = saneAttrs['t0'].rfind('.')
    if(decPt >= 0): # has decimal point
        saneStart += float('0'+saneAttrs['t0'][decPt:])
    if debug: print("getDataFromFile() --- got sanet0 = {}".format(saneStart))
    
    startCut = 0
    endCut = 0
    if firstFile:
        startCut = startUnix - startT
    if lastFile:
        endCut = endUnix - endT
    
    if debug: 
        print("\ngetDataFromFile() --- startCut = {}".format(startCut))
        print("getDataFromFile() --- endCut = {}".format(endCut))


    if startCut > 0:
        if verbose: print("\ngetDataFromFile() --- cutting start ({} seconds, {} points)".format(startCut, startCut*sampRate))
        startT += startCut
        saneStart += startCut
        dataList = dataList[int(startCut*sampRate):]
        saneList = saneList[int(startCut):]
        if debug: 
            print("\tgetDataFromFile() --- new t0 = {}".format(startT))
            print("\tgetDataFromFile() --- new sanet0 = {}".format(saneStart))
    if endCut < 0:
        if verbose: 
            print("\ngetDataFromFile() --- cutting end ({} seconds, {} points)".format(endCut, endCut*sampRate))
        dataList = dataList[:int(endCut*sampRate)]
        saneList = saneList[:int(endCut)]
        if debug: 
            print("\tgetDataFromFile() --- new total number of pts: {}".format(dataList.size))
            print("\tgetDataFromFile() --- new sanity total number of pts: {}".format(saneList.size))
        
            print("\ndatalist size: {}".format(dataList.size))
            print("saneList.size: {}".format(saneList.size))
   
    # create data TimeSeries
  
    # Convert Unix Time back to GPS time for compatibility with GNOME data format. This takes care of WARNING: ErfaWarning: ERFA function "taiutc" yielded 1 of "dubious year (Note 4)" [astropy._erfa.core]
    
    startT=to_gps(time.strftime("%a, %d %b %Y %H:%M:%S +0000",time.gmtime(startT)))
    
    
    dataTS = TimeSeries(dataList, sample_rate=sampRate, epoch=startT) # put data in TimeSeries
    if(convert):
        convStr = attrs['MagFieldEq'] #string contatining conversion function
        unitLoc = convStr.find('[') # unit info is at end in []
        if(unitLoc >= 0): # unit given
            convStr = convStr[:unitLoc] # get substring before units
        l=locals()
        convStr=convStr.replace('MagneticFields','dataTS') # relabel with correct variable
        exec ('dataTS = '+convStr,l) # dynamic execution to convert dataTS
        dataTS= l['dataTS']
    # create sanity TimeSeries
    saneTS = TimeSeries(saneList, sample_rate=saneRate, epoch=startT)
    
    h5pyFile.close()    
    return dataTS, saneTS


def getFListInRange(station, startTime, endTime, path='./', verbose=False, debug = False):
    '''
    Get list of file names for GNOME experiment within a time period.
    
    Data located in folder of the form 'path/station/yyyy/mm/dd/'.
    Time range uses the start time listed in the 'hdf5' file name.
    
    station: str
        Name of station.
    startTime: float (unix time), str
        Earliest time. String formatted as 'yyyy-mm-dd-HH-MM-SS' (omitted values defaulted as 0)
    endTime: float (unix time), str
        Last time. Format same as startTime
    path: str (default './')
        Location of files
    verbose: bool (default False)
        Verbose output
        
    returns list of file names
    '''
    # put date in consistant format
    # Note that the file names do not contain milliseconds, so "time" tuple is ok
    makeSU = True # Need to calculate start time in unix
    makeEU = True # Need to calculate end time in unix
    if(not type(startTime) is str): # given unix time
        startUnix = startTime
        makeSU = False
        startTime = time.strftime('%Y-%m-%d-%H-%M-%S',time.gmtime(startTime)) 
    if(not type(endTime) is str): # given unix time
        endUnix = endTime
        makeEU = False
        endTime = time.strftime('%Y-%m-%d-%H-%M-%S',time.gmtime(endTime))
    
    # Format start/end times (in array and unix time, if needed)
    startTList = [0.]*9 # date-time tuples (note that last 3 should never be changed)
    endTList   = [0.]*9
    startTime = str.split(startTime,'-')
    endTime = str.split(endTime,'-')
    startTList[:len(startTime)] = [int(t) if len(t)>0 else 0. for t in startTime]
    endTList[:len(endTime)]     = [int(t) if len(t)>0 else 0. for t in endTime]
    if(makeSU):
        startUnix = calendar.timegm(startTList)
        if debug: print("startUnix: {}".format(startUnix))
    if(makeEU):
        endUnix = calendar.timegm(endTList)
        if debug: print("endUnix: {}".format(endUnix))

    # check for bad input
    if(startUnix > endUnix):
        if(verbose):
            print ('getFListInRange() --- Bad input time range (check order).')
        return []
    
    # create array of dates (used for folders)
    dummy = [0.]*9
    dummy[0:3] = startTList[0:3] # start date (beginning of day)
    currTime = calendar.timegm(dummy)
    dates = []
    while(currTime < endUnix):
        dates.append(time.strftime('%Y/%m/%d/',time.gmtime(currTime))) # path segment
        currTime += 86400 # add a day
    firstTime = startUnix
    lastTime = endUnix
    fList = [] #will hold list of files
    if debug: 
    	print("dates: {}".format(dates))
    for i in range(len(dates)):
        firstDate = i==0 # use these bools to skip checks for middle dates
        lastDate = i==len(dates)-1
        
        dataDir = join(path,station,dates[i]) #directory of files from date
        
        try:
            # get list of files (ignore, e.g., folders)
            foldFiles = [f for f in listdir(dataDir) if isfile(join(dataDir, f))]
            
            # add file to list if it is in time range
            # files like: fribourg01_20170102_122226.hdf5
            for f in foldFiles:
                inRange = not (firstDate or lastDate)
                if(not inRange): # need to check
                    if debug: print('getFListInRange() --- checking date {}, file {}'.format(dates[i],f))
                    fTime = f.split('_')[2].split('.')[0] # get time 'hhmmss'
                    fTime = fTime[0:2]+':'+fTime[2:4]+':'+fTime[4:6] # time format hh:mm:ss
#                     print dates[i]+fTime, f
                    fTime = calendar.timegm(time.strptime(dates[i]+fTime, '%Y/%m/%d/%H:%M:%S')) # in unix
                    if(fTime + 60. > startUnix and fTime < endUnix):
                        inRange = True
                        if firstDate: 
                            firstTime = fTime
                            if debug: print("\nfirst folder in range: {}".format(firstTime))
                            firstDate = False
                        if lastDate: 
                            lastTime = fTime
                            if debug: print("last folder in range: {}".format(lastTime))
#                     if verbose: print '\t', inRange
                if(inRange): # add file to list
                    fList.append(join(dataDir, f))
                # in case the file list is not sorted, look through all files.
        except OSError:
            if(verbose):
                print ('getFListInRange() --- Data not found for:', dates[i])
    startCut = startUnix - firstTime
    endCut = (lastTime+60.)-endUnix
    if(verbose):
        print('\ngetFListInRange() --- (startUnix - folder start) = {}'.format(startCut))
        print('getFListInRange() --- ((last folder + 60) - endUnix) = {}'.format(startCut))

    return fList, startUnix, endUnix
    #return fList,startCut,endCut

def getDataInRange(station, startTime, endTime, sortTime=True, convert=False, path='./', verbose=False, debug = False, sanity = False):
    '''
    Get list of data in time range
    
    station: str
        Name of station.
    startTime: float (unix time), str
        Earliest time. String formatted as 'yyyy-mm-dd-HH-MM-SS' 
        (omitted values defaulted as 0)
    endTime: float (unix time), str
        Last time. Format same as startTime
    sortTime: bool (default: True)
        Actively sort output by start time (using data in file)
    convert: boolean (default: False)
        Whether to use conversion function from file.
    path: str (default './')
        Location of files
    verbose: bool (default False)
        Verbose output
    
    returns (data, sanity, fileList). Data and sanity are astropy TimeSeriesList
    
    Note: must evaluate values in 'sanity' (e.g., using 'value' attribute) to get boolean
    Note: use, e.g., dataTSL.join(pad=float('nan'),gap='pad') to combine 
    TimeSeriesList into single Time series.
    '''
    if(verbose):
        print ('getDataInRange() --- Finding files')
    fList, su, eu = getFListInRange(station, startTime, endTime, path = path, verbose = verbose, debug = debug)
    ####SAMI CHANGE 5/4: Added option to check sanity of data.
    if(sanity):
        #print("Total # of files: ", len(fList))
        fList = getSaneList(fList, working=None)
    numFiles = len(fList)
    #print("# of sane files: ", numFiles)
    
    # get data
    if(verbose):
        print ('getDataInRange() --- Reading files')
    dataList = [None]*numFiles
    saneList = [None]*numFiles
    for i in range(numFiles):
        firstFile = False
        lastFile = False
        if i == 0 : 
            firstFile = True
        if i == numFiles-1:
            lastFile = True
            
#         if (i == 0): 
#             startCut = sc
#             endCut = 0
#         elif (i == numFiles - 1): 
#             endCut = ec
#             startCut = 0
#         else:
#             startCut = 0
#             endCut = 0
        dataList[i],saneList[i] = getDataFromFile(fName = fList[i], startUnix = su, endUnix = eu, firstFile = firstFile, lastFile = lastFile, convert = convert, verbose = verbose, debug = debug)
    
    # sort if needed
    if(sortTime):
        if(verbose):
            print ('getDataInRange() --- Sorting data')
        # do insertion sort (likely that list is sorted)
        sortIndex = list(range(numFiles)) # sorted list of indices
        
        for sRange in range(1, numFiles): # sRange is size of sorted segment
            # note, sortIndex[sRange] = sRange
            insPtTime = dataList[sRange].epoch # for point being inserted
            insHere = sRange # place to insert point
            while(insHere > 0 and dataList[sortIndex[insHere-1]].epoch > insPtTime): 
                insHere -= 1 # decrement until finding place to insert
            # insert point
            dummy1 = sRange # point being moved
            while(insHere <= sRange):
                dummy2 = sortIndex[insHere]
                sortIndex[insHere] = dummy1
                dummy1 = dummy2
                insHere+=1
    else:
        sortIndex = range(numFiles)
    
    # put data in TimeSeriesList
    dataTSL = TimeSeriesList()
    saneTSL = TimeSeriesList()
    for i in sortIndex:
        dataTSL.append(dataList[i])
        saneTSL.append(saneList[i])    
    
    return dataTSL, saneTSL, [fList[i] for i in sortIndex]

def getFileNameTime(fileName):
    '''
    Gives unix time from file name (does not read file). 
    Files of the form */yyyy/mm/dd/*hhmmss.hdf5, no '/' after/in last *
    
    fileName: str
        file name
    
    return unix time of file, according to name
    '''
    fileComp = str.split(fileName,'/')
    fileComp = fileComp[-4:] # last 4 elements
    year = int(fileComp[0])
    month = int(fileComp[1])
    day = int(fileComp[2])
    
    dayTime = fileComp[3][-11:-5] # hhmmss
    hour = int(dayTime[:2])
    minute = int(dayTime[2:4])
    second = int(dayTime[4:])
    
    timeList = [year, month, day, hour, minute, second,0,0,0] # date-time tuples (note that last 3 should never be changed)
    return calendar.timegm(timeList)

def getFListFromDates(station,dates,path='./', verbose=False):
    '''
    Gets list of files from a date (relies on file organization: path/station/yyyy/mm/dd/).
    
    station: str
        Name of station used in file system
    dates: list<str>
        List of date strings (format: year-month-day)
    path: str (default './')
        Data path of folder holding station folders
    verbose: bool (default: False)
        print when file not found 
    
    returns list of file names
    '''
    if(type(dates) is str): # make 'dates' an array is single val given
        dates = [dates]
    fList = [] #will hold list of files
    
    for date in dates:
        dArr = [int(s) for s in date.split('-') if s.isdigit()] #split date into integer array
        dataDir = join(path,station,"{0:04d}/{1:02d}/{2:02d}".format(dArr[0],dArr[1],dArr[2])) #directory of files from date
        
        # append list of files (exclude directories). Use full path
        try:
            fList.extend([join(dataDir, f) for f in listdir(dataDir) if isfile(join(dataDir, f))]) 
        except OSError:
            if(verbose):
                print ('Data not found for:', date)
    
    return fList

def getStartTimes(fList):
    '''
    Gets start time (in sec since UNIX epoch) for file list. 
    
    fList: List<str>
        list of file names
    
    retruns list of start times
    '''
    nFiles = len(fList)
    stTimes = [0.]*nFiles
    for i in range(nFiles):
#         if(not i%1000): #print progress
#             print i,'/',nFiles
        
        f = fList[i]
        h5pyFile= h5py.File(f,'r')
        magFieldAttrs = h5pyFile['MagneticFields'].attrs
        timeStr = magFieldAttrs['Date'] + ' ' + magFieldAttrs['t0'] + ' UTC'
        h5pyFile.close()
        
        # get time since epoch (in sec)
        stTimes[i] = calendar.timegm(time.strptime(timeStr, "%Y/%m/%d %H:%M:%S.%f %Z"))
    
    return stTimes

def getSaneList(fList, working=None):
    '''
    Gets list of good data, i.e., those with perfect samity channels
    
    fList: list<str>
        list of file names
    working: list<int>
        indices of working stations. If not specified, all stations are used.
    
    returns list of good files (indices)
    '''
    saneList = []
    if(not working): # no filter specified
        working = range(len(fList))
    
    for i in range(len(fList)):
        fName = fList[i]
        h5pyFile = h5py.File(fName,'r')
        sane = h5pyFile['SanityChannel']
        isGood = True
        
        for check in sane: # go through every second to find bad parts
            if(not check):
                isGood = check
                break; # can break out if one bad is found
        if(isGood):
            ###SAMI CHANGE 5/4: Changed from appending index of sane files to appending sane files.
            #saneList.append(i)
            saneList.append(fList[i])
        h5pyFile.close()
    
    return saneList

 
####Wrapper Functions Ibrahim has  written ###########
def getStationTS(stationname,tstart,tend,datapath = '/home/iasulai/ELF/'):
    dataTSL=getDataInRange(stationname, tstart, tend, sortTime=True, convert=True, path=datapath, verbose=False)
    stationdata=dataTSL[0].join(pad=float('nan'),gap = 'pad')
    stationdata.name = stationname
    return stationdata

def getStationTS2(stationname,tstart,tend,datapath = '/home/iasulai/ELF/'):
    dataTSL=getDataInRange(stationname, tstart, tend, sortTime=True, convert=True, path=datapath, verbose=False)
    stationdata = dataTSL[0].join(pad=float('nan'),gap = 'pad')
    stationsanity = dataTSL[1].join(pad=float('nan'),gap = 'pad')
    #Making the start time of the sanity channel match the start time of the data channel
    stationdata.name = stationname
    return [stationdata,stationsanity]


def getStationTSanity(stationname,tstart,tend,datapath = '/home/iasulai/ELF/'):
    dataTSL=getDataInRange(stationname, tstart, tend, sortTime=True, convert=True, path=datapath, verbose=False)
    stationdata = dataTSL[0].join(pad=float('nan'),gap = 'pad')
    
    
    #Making the start time of the sanity channel match the start time of the data channel
    stationsanity.t0 = dataTSL[0].t0;
    
    stationdata.name = stationname
    return stationsanity

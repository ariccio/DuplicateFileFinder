#!C:\Python33\Python.exe
#-*- coding:utf-8 -*-
'''
Based DISTANTLY on code by JEANNENOT Stephane

idea: for possibly duplicate files, read a block at position 2^(number of iterations)

Windows throws MANY errors. List of Python supported errors here: http://bugs.python.org/file7326/winerror.py
List of ALL errors here: http://msdn.microsoft.com/en-us/library/ms681381(v=vs.85).aspx
Alternate lists:
    http://www.mathemainzel.info/files/w32errcodes.html
    http://user.tninet.se/~tdf275m/wincode2.htm
    http://www.briandunning.com/error-codes/?source=Windows
    http://www.madanmohan.com/p/codes.html
    http://march-hare.com/puuu/doc/Winerror.h
THE MOST AUTHORITATIVE(?):
    http://bugs.python.org/file7326/winerror.py
List of errors supported by Java (sort of like a mirror):
    https://java.net/projects/jna/sources/svn/content/trunk/jnalib/contrib/platform/src/com/sun/jna/platform/win32/W32Errors.java?rev=1187
Humorous error lists:
    http://www.testingcircus.com/tastro-2014-testers-astrology/
'''
from __future__ import print_function
import sys
if sys.version_info[0] < 3:
    from exceptions import NotImplementedError, ValueError, IOError

#support for python 2.7 and 3
if sys.version_info.major < 3:
    import Queue as queue
    print("I've given up activley supporting python 2.x, so best of luck to you!")
elif sys.version_info.major > 2:
    import queue

import logging
import os
import hashlib
import contextlib
import io
#import itertools
#import collections
NO_HUMANFRIENDLY = True
try:
    import humanfriendly
except ImportError:
    print('Install "human friendly" with "pip install humansize" for human-readable file sizes')
    NO_HUMANFRIENDLY = None

#import optparse
import argparse
#version = '1.0'


def multi_input(prompt):
    '''
    eases python 2/3 support
    '''
    if sys.version_info.major < 3:
        return raw_input(prompt)
    else:
        return input(prompt)

class Worker():
    def __init__(self, hashname, rmode='rb', bufsize=16777216, name=None):#268435456 = 2^ 28#2097152
        self.hash_known    = ['md5', 'sha1', 'sha512']
        self.hash_length   = {32:'md5', 40: "sha1", 128: "sha512"}
        self.__bufsize     = bufsize
        self.__hashname    = hashname
        self.__hashdata    = None
        self.__rmode       = rmode
        if name is not None:
            self.__name = name
        else:
            self.__name = "worker-%s" % self.__hashname

    def compute(self, fname, incremental=None):
        #this is depreciated, will be removed!
        '''
        calculates the hash (with algorithm self.__hashname) of fname and returns a 2-item tuple: (fname, a hexdigest of the hash)
        '''
        self.__fname = fname
        try:
            self.__hashdata = hashlib.new(self.__hashname)
        except ValueError:
            raise NotImplementedError("# %s : hash algorithm [ %s ] not implemented" % (self.__name, self.__hashname))
        except KeyboardInterrupt:
            sys.exit()
        try:
            logging.info('\tcompute opening %s' % (str(self.__fname)))
            with open(self.__fname, self.__rmode) as fhandle:
                if incremental is not None:
                    if NO_HUMANFRIENDLY is None:
                        logging.debug('\t\treading incrementally... (increments of: %s bytes)' % (str(self.__bufsize)))
                    elif NO_HUMANFRIENDLY is not None:
                        logging.debug('\t\treading incrementally... (increments of: %s)' % (humanfriendly.format_size(self.__bufsize)))

                    data = fhandle.read(self.__bufsize)
                    while data:
                        self.__hashdata.update(data)
                        data = fhandle.read(self.__bufsize)
                else:
                    logging.debug('\t\treading in one huge chunk...')
                    data = fhandle.read()#this is ugly and will break for large files
                    self.__hashdata.update(data)
        except PermissionError:
            logging.warning("\t\t\twhoops! PermissionError in Worker.compute, is file a lock?")
        except IOError:
            logging.warning("\t\t\twhoops! IOError in Worker.compute")
        except KeyboardInterrupt:
            sys.exit()

        logging.debug('\t\t\t\tfile: "%s" : %s' % (str(self.__fname), str(self.__hashdata.hexdigest())))
        return (self.__fname, self.__hashdata.hexdigest())

    def computeByteArray(self, fname, fSize, incremental=None):
        #this is depreciated, will be removed!
        localFName = fname
        localByteBuffer = bytearray(fSize)
        localHashdata = hashlib.new('sha1')
        try:
            with io.open(localFName, 'rb') as fhandle:
                data = fhandle.readinto(localByteBuffer)
                while data != 0:
                    localHashdata.update(localByteBuffer[:data])
                    data = fhandle.readinto(localByteBuffer)
        except PermissionError:
            logging.warning("PermissionError while opening %s" % (str(localFName)))
        return (localFName, localHashdata.hexdigest())

    def computeMultipleByteArrays(self, listOfFileNames, fileSize, incremental=None, extFSize=[134217728]):
        '''
        Computes the hash of a LIST of files, chunk by chunk, and stops at the FIRST divergance
        '''
        #TODO: eliminate function call overhead associated with calling computeByteArray for EVERY goddamned file!
        fSize = extFSize
        iteration = 0
        #logging.debug('computeMultipleByteArrays')
        localListOfFileNames = listOfFileNames
        #localDictOfFileHandles = {}
        localDictOfBytes = {}
        localDictOfFileHashResults = {}
        #localByteBuffer = bytearray(fSize[0])
        localHashLib = hashlib
        localLogging = logging
        localLen = len
        localAll = all
        localAny = any
        localBytearray = bytearray
        localPermissionError = PermissionError
        localIO = io
        localMemoryError = MemoryError
        localStr = str
        localHashLibNew = localHashLib.new
        contextLibExitStack = contextlib.ExitStack
        for item in localListOfFileNames:
            try:
                localDictOfFileHashResults[item] = [localHashLibNew('sha1'), localBytearray(fSize[0])]
            except localMemoryError:
                logging.error("Your computer probably just got really slow! That was my fault. Sorry!")
                logging.error("File %s was just TOO big" % localStr(item))

        fSize[0] = 0
        #localLogging.debug('\tComputing multiple byte arrays: %s' % str(localListOfFileNames))
        #localLogging.debug('\tfSize: %i' % fSize[-1])
        try:
            with contextLibExitStack() as stack:
                for fileName in localDictOfFileHashResults.keys():
                    #localLogging.debug('\n\t\t\tworking on %s...' % str(fileName))
                    localDictOfFileHashResults[fileName].append(stack.enter_context(localIO.open(fileName, 'rb')))
                    keepReading = True
                    localDictOfFileHashResults[fileName].append(keepReading)
                    localDictOfBytes[fileName] = localDictOfFileHashResults[fileName][2].readinto(localDictOfFileHashResults[fileName][1])
                    #localLogging.debug('\t\tread %i bytes!' % (int(localDictOfBytes[fileName])*8))
                    #localDictOfFileHashResults[key] = [hashlibSHA1, bytearray, fileObject.rb, bool]
                #localLogging.debug("\t\tfSize[-1]: %i" % fSize[-1])
                #localLogging.debug("\t\tlocalDictOfBytes[fileName]: %i" % localDictOfBytes[fileName])
                while localAny(localDictOfFileHashResults[aSingleFileName][3] for aSingleFileName in localDictOfFileHashResults.keys()) and fSize[-1] <= fileSize:
                    fSize.append(2**iteration)
                    for fileName in localDictOfFileHashResults.keys():
                        #localLogging.debug('\t\t\tworking on %s...' % str(fileName))
                        if localDictOfFileHashResults[fileName][3]:
                            #localLogging.debug("\t\t\tfSize[-2]:fSize[-1] %i:%i" % (fSize[-2],fSize[-1]))
                            localDictOfFileHashResults[fileName][0].update(localDictOfFileHashResults[fileName][1][fSize[-2]:fSize[-1]])
                    if localAll(localDictOfFileHashResults[fileName][0].hexdigest() == localDictOfFileHashResults[aFileName][0].hexdigest() for aFileName in localDictOfFileHashResults.keys() if localDictOfFileHashResults[aFileName] and aFileName != fileName):
                        if localLen(localDictOfFileHashResults.keys()) > 1:
                            pass
                            #localLogging.debug('\t\t\t\tall converge\n')
                        else:
                            pass
                            #logging.debug("\t\t\t\tlen([key for key in localDictOfFileHashResults.keys()])NOT>1 %i" % len(localDictOfFileHashResults.keys()))
                    else:
                        #localLogging.debug('\t\t\t\t\t%s diverges!\n' % str(fileName))
                        localDictOfFileHashResults[fileName][3] = False
##                    logging.debug("\t\t\tlocalDictOfFileHashResults[fileName][3] for fileName in keys: %s\n" % str([localDictOfFileHashResults[fileName][3] for fileName in localDictOfFileHashResults.keys()]))
                    iteration += 1
            #localLogging.debug("\tLeft context!")
        except localPermissionError:
            localLogging.warning("PermissionError while opening %s" % ('a file'))#TODO: fix
        except Exception as e:
            localLogging.fatal("Some strange error ocurred, try running again.")
            sys.exc_info()
            print("Exception: %s" % str(e))
            print(sys.exc_info())
            sys.exit(666)
        return localDictOfFileHashResults

def getFileSizeFromOS(theFileInQuestion):
    '''
    NEW (inspired by "C:\\Python27\\Lib\\genericpath.py".getsize(filename)) method:
    --------------------------------------------------------------------------------------------
    `os.stat(theFileInQuestion).st_size` returns some number followed by 'L' (<type 'long'>) in Python 2.x, some number without postfixed 'L' (<class 'int'>)
    e.g:
        os.stat("C:\\Users\\Alexander Riccio\\Documents\\GitHub\\DuplicateFileFinder\\README.md").st_size returns:
            Python 3.x: '42'
            Python 2.x: '42L'


    -------------------------------------------------------------------------------------------
    >>> (fd, path) = tempfile.mkstemp(); os.write(fd, 'aaaa'); os.close(fd); print os.stat(path).st_size; os.remove(path)
    4
    4

    '''
    localLogging = logging
    localLoggingWarning = localLogging.warning
    localOS = os
    localInt = int
    localWindowsError = WindowsError
    localStr = str
    localOSstat = localOS.stat
    localOSPath = localOS.path
    localOSPath_islink = localOSPath.islink
    #localOSPath_getSize = localOSPath.getSize
    try:
        fileSizeInBytes = localInt(localOSstat(theFileInQuestion).st_size)

    except localWindowsError as winErr:
        #logging.warning('Windows error %s while getting size of "%s" in printDuplicateFilesAndReturnWastedSpace!\n\tMay god have mercy on your soul.\n\n' % ( str(winErr.errno), str(theFileInQuestion)))
        if winErr.errno == 1920:
            if localOSPath_islink(theFileInQuestion):
                fileSizeInBytes = localOS.path.getSize(theFileInQuestion)
            else:
                fileSizeInBytes = 0
        elif winErr.errno == 2:
            localLoggingWarning('Windows could not find %s, and thereby failed to find the size of said file.' % (theFileInQuestion))
            fileSizeInBytes = 0
        elif winErr.errno == 22:
            localLoggingWarning("Windows threw error 22 while getting the size of %s - error 22 means 'ERROR_BAD_COMMAND', and ERROR_BAD_COMMAND means 'The device does not recognize the command.' It doesn't make sense to me either." % (theFileInQuestion))
            fileSizeInBytes = 0
        elif winErr.errno == 13:
            localLoggingWarning("Windows threw error 13 while getting the size of %s - error 13 means 'ERROR_INVALID_DATA', and ERROR_INVALID_DATA means 'The data is invalid.' It doesn't make sense to me either." % (theFileInQuestion))
            fileSizeInBytes = 0
        else:
            localLoggingWarning('Windows error %s while getting size of "%s"!\n\tMay god have mercy on your soul.\n\n' % (localStr(winErr.errno), theFileInQuestion))
            fileSizeInBytes = 0
        #fileSizeInBytes = 0
    return fileSizeInBytes

def printListOfDuplicateFiles(extlistOfDuplicateFiles, showZeroBytes, stopOnFirstDiff):
    '''
    expects a list:
        item[0] = the SIZE of a file
        item[1] = a list of fileNAMES with that size
    '''
    listOfDuplicateFiles = extlistOfDuplicateFiles
    localLogging = logging
    localLogging.debug('\tprinting list of duplicate files!')
    localPrint = print
    localLen = len
    localStr = str
    localUnicodeEncodeError = UnicodeEncodeError
    localValueError = ValueError
    localRange = range
    localLoggingDebug = localLogging.debug
    localLoggingWarning = localLogging.warning
    localLoggingError = localLogging.error
    humanfriendly_format_size = humanfriendly.format_size
    if NO_HUMANFRIENDLY is None:
        localLoggingDebug('\t\thumanfriendly NOT installed, proceeding with crappy formatting...')
        for item in listOfDuplicateFiles:
##            localLogging.debug("item: %s" % str(item))
            if item[0] > 0 or showZeroBytes:
                if stopOnFirstDiff and localLen(item[1]) > 1:
                    localPrint("\n%s:" % (localStr(item[0])))
                    for aFileName in item[1]:
                        try:
                            #print("\t%s" % (str(aFileName)))
                            localPrint("\t%s" % (aFileName))
                        except localUnicodeEncodeError:
                            localLoggingWarning("\t\t\tfilename is evil! filename: ", aFileName)
                elif not stopOnFirstDiff:
                    localPrint("\n%s:" % (localStr(item[0])))
                    for aFileName in item[1]:
                        try:
                            #print("\t%s" % (str(aFileName)))
                            localPrint("\t%s" % (aFileName))
                        except localUnicodeEncodeError:
                            localLoggingWarning("\t\t\tfilename is evil! filename: ", aFileName)

    elif NO_HUMANFRIENDLY is not None:
        localLoggingDebug('\t\thumanfriendly IS installed, proceeding with nice formatting...')
        for item in listOfDuplicateFiles:
            if item[0] > 0 or showZeroBytes:
                try:
                    item.append(humanfriendly_format_size(item[0]))
                    #↑that is a HACK, to fix the weirdness of python's pass-by-assignment nature
                    localPrint('\n%s:' % (item[2]))
                    for aFileName in item[1]:
                        localPrint('\t%s' % (aFileName))
                except localValueError:
                    localLoggingWarning('\t\tError formatting item %s for human friendly printing!' % localStr(item[2]))
                    localLoggingDebug('\t\t\tItem: "%s" at fault!' % (localStr(item)))
                    for i in localRange(localLen(item)):
                        indent = '\t' * (4 + i)
                        localLoggingDebug('%sitem[%i]: %s' % (indent, i, localStr(item[i])))
                    #sys.exit()
                except localUnicodeEncodeError:
                    localLoggingError('\t\tfilename is evil! filename: "%s"' % (localStr(aFileName)))
                except:
                    localPrint('---------------------------------------------------------------------')
                    localLogging.fatal('SOMETHING IS VERY WRONG!')
                    sys.exc_info()
                    localPrint("faulting item: ", item)
                    sys.exit(666)
                    localPrint('---------------------------------------------------------------------')
    else:
        localLogging.error('Something is VERY wrong in printListOfDuplicateFiles')
        sys.exit(666)

def printDuplicateFilesAndReturnWastedSpace(extKnownFiles, stopOnFirstDiff, showZeroBytes):
    '''
    prints duplicate files (as of now, only those > 0 bytes) in the form of

    size:
    numBytes bytes          pathToFile

    e.g.:

    374.27 MB:
            C:\\Users\\Alexander Riccio\\Downloads\\testDir\\blah.pdf
            C:\\Users\\Alexander Riccio\\Downloads\\testDir\\blahBlah.pdf

    If you have humanfriendly ("humansize") installed, the output will be more readable - i.e. 1 KB, not 1024.
    I also increment a counter, wastedSpace, by filesize over each file.

    ALOT happens in this function - AND it is performance critical that we make as few function calls as possible - so it's huge
    '''
    wastedSpace = 0
    sizeOfKnownFiles = {}
    knownFiles = extKnownFiles
    localLogging = logging
    #localLogging.debug('\tcalculating wasted space...')
    localHumanFriendly = humanfriendly
    localLen = len
    localValueError = ValueError
    localIndexError = IndexError
    localAll = all
    localSorted = sorted
    localGetFileSizeFromOS = getFileSizeFromOS
    local_NO_HUMANFRIENDLY = NO_HUMANFRIENDLY
    localInt = int
    for key in knownFiles.keys():
##        logging.debug("\tknownFiles data example: %s --- %s" % (str(key), str(knownFiles[key])))
        aFile = knownFiles[key][0]
        lenKnownFilesKey = localLen(knownFiles[key])
        fileSizeInBytes = localGetFileSizeFromOS(aFile)
        #localLogging.debug('\t\tgot file size: %i' % fileSizeInBytes)
        #localLogging.debug('\t\tprocessing: %s' % ( str(knownFiles[key])))
        if (lenKnownFilesKey > 0) or (stopOnFirstDiff):
            if fileSizeInBytes > 0 and not stopOnFirstDiff:
                wastedSpace += fileSizeInBytes * (lenKnownFilesKey-1)
                sizeOfKnownFiles[key] = fileSizeInBytes * (lenKnownFilesKey-1)
                #localLogging.debug("\n\t\t\tkey:%s:"%key)
            elif fileSizeInBytes > 0 and stopOnFirstDiff:
                wastedSpace += fileSizeInBytes * (lenKnownFilesKey)
                sizeOfKnownFiles[key] = fileSizeInBytes * (lenKnownFilesKey)
                #localLogging.debug("\n\t\t\tkey:%s:"%key)
            elif fileSizeInBytes == 0:
                sizeOfKnownFiles[key] = 0
            if local_NO_HUMANFRIENDLY is None:
                pass
                #for aSingleFile in knownFiles[key]:
                    #localLogging.debug("\t\t\t%s bytes\t\t\t%s" % (fileSizeInBytes, aSingleFile))
                    #localLogging.debug('\t\twasted space so far: %i' % wastedSpace)
            elif local_NO_HUMANFRIENDLY is not None:
                pass
                #for aSingleFile in knownFiles[key]:
                    #localLogging.debug("\t\t\t%s\t\t\t%s" % (localHumanFriendly.format_size(fileSizeInBytes), aSingleFile))
                    #localLogging.debug('\t\twasted space so far: %s' % localHumanFriendly.format_size(wastedSpace))
        #localLogging.debug('\t\twasted space so far: %i' % wastedSpace)
    #localLogging.debug('\tcalculated wasted space: %i' % wastedSpace)

    sortedSizeOfKnownFiles = localSorted(sizeOfKnownFiles, key=knownFiles.__getitem__)
    sortedListSize = []
    #logging.debug('\n\n')
    for sortedHash in sortedSizeOfKnownFiles:
        #logging.debug("\t\t\tfor sortedHash in sortedSizeOfKnownFiles: %s" % str([sizeOfKnownFiles.get(sortedHash), knownFiles.get(sortedHash)]))
        sortedListSize.append([sizeOfKnownFiles.get(sortedHash), knownFiles.get(sortedHash)])
    sizes = {}
    #logging.debug("\t\tsortedListSize: %s" % str(sortedListSize))
    #logging.debug('\n\n')
    for size in sortedListSize:
        it = sizes.get(size[0])
        if it is None:
            sizes[size[0]] = size[1]
            #logging.debug("\t\t\tsize[1]: %s" % str(size[1]))
        else:
            #logging.debug("\t\t\tit is: %s" % str(it))
            #logging.debug("\t\t\t\tsizes[%s].append(size[1][0])" % str(size[0]))
            #logging.debug("\t\t\t\tsize[1][0]: %s" % str(size[1][0]))
            sizes[size[0]].append(size[1][0])
    newLSizes = []
    #logging.debug("\t\t\tsizes:")
    for aSizeKey in sizes.keys():
        #logging.debug("\t\t\t\t%s"% str(sizes[aSizeKey]) )
        if localLen(sizes[aSizeKey]) > 1:
            newLSizes.append([aSizeKey, sizes[aSizeKey]])
    #localLogging.debug('\n\n\n\n')
    #localLogging.debug('ready to print list of duplicate files!')
##UNPYTHONIC
    iterator = 0
    lastKnownSortedIndex = 0
    while not localAll(newLSizes[newLSizes.index(itera)+1] > newLSizes[newLSizes.index(itera)] for itera in newLSizes[lastKnownSortedIndex:] if itera != newLSizes[-1]):#hotspot
        #even with this ugliness, order is a bit borked!
        iterator = 0
        lenNewLSizes = localLen(newLSizes)
        while iterator < lenNewLSizes-1:
            try:
                if newLSizes[iterator+1] < newLSizes[iterator]:
                    try:
                        popped = newLSizes.pop(iterator)
                        newLSizes.insert(iterator+1, popped)
                    except localValueError:
                        pass
                        #localLogging.debug('valueerror')
                else:
                    lastKnownSortedIndex += 1
            except localIndexError:
                pass
                #localLogging.debug("IndexError ", iterator)
            iterator += 1
##ENDUNPYTHONIC

##    printListOfDuplicateFiles(sortedListSize, showZeroBytes)
    printListOfDuplicateFiles(newLSizes, showZeroBytes, stopOnFirstDiff)
##HACK
    if stopOnFirstDiff:
        hackWastedSpace = 0
        for item in newLSizes:
            fileSizeInBytes = localInt(item[0])
            itemOneLen = localLen(item[1])
            if itemOneLen > 0:
                hackWastedSpace += fileSizeInBytes * localInt(itemOneLen)
        return hackWastedSpace
##END HACK
    else:
        return wastedSpace


def removeDuplicatesForHeuristic(sortedSizes):
    '''
    This name is very misleading. We're removing NON-duplicates!
    '''
    #localLogging = logging
    localLen = len
    localStr = str
    localTypeError = TypeError
    localSys = sys
    sortedSizesRemove = sortedSizes.remove
    for size in sortedSizes:
        #size should be format: [36, ['C:\\Users\\Alexander Riccio\\Documents\\t.txt']]
        try:
            if localLen(size[1]) < 2:
                #localLogging.debug('\t\tremoving sub-two element (%s[1]) list...\n' % (str(size[1])))
                sortedSizesRemove(size)
        except localTypeError:
            #localLogging.error('\t\titem "%s" is neither a sequence nor a mapping!' % (localStr(size)))
            localSys.exit()
    return sortedSizes

def walkDirAndReturnQueueOfFiles(directoryToWalk):
    queueOfFiles = queue.Queue()
    #logging.debug('Walking %s...' % (str(directoryToWalk)))
    localOS = os
    for root, dirs, files in localOS.walk(directoryToWalk):
        '''move to:
            build queue of files -> build pool of processes -> start processes
            like I did in factah over summer, where mp_factorizer is passed nums(a list of numbers to factorize) and a number nprocs (how many processes)

        '''
        for fname in files:
            fullpath = localOS.path.abspath(os.path.join(root, fname))
            queueOfFiles.put(fullpath)
        queueOfFiles.put(False)
    return queueOfFiles


def walkDirAndReturnListOfFiles(directoryToWalk):
    ListOfFiles = []
    localOS = os
    localOSWalk = localOS.walk
    localOSPath = localOS.path
    localOSPath_abspath = localOSPath.abspath
    localOSPath_join = localOSPath.join
    #logging.debug('Walking %s...' % (str(directoryToWalk)))
    for root, dirs, files in localOSWalk(directoryToWalk):
        '''move to:
            build queue of files -> build pool of processes -> start processes
            like I did in factah over summer, where mp_factorizer is passed nums(a list of numbers to factorize) and a number nprocs (how many processes)

        '''
        for fname in files:
            fullpath = localOSPath_abspath(localOSPath_join(root, fname))
            ListOfFiles.append(fullpath)
    return ListOfFiles


def main_method(heuristic, algorithm, stopOnFirstDiff, args, showZeroBytes):
    if args:
        arg0 = args.directory
        fileSizeList = []
        fileSizeDict = {}
        knownFiles = {}
        localIndexError = IndexError
        localKeyboardInterrupt = KeyboardInterrupt
        localStr = str
        localPrint = print
        localLen = len
        localSum = sum
        if os.path.isfile(arg0):
            logging.debug('%s is a file!' % (localStr(arg0)))
            logging.error("There are no duplicates in a single file! Duh.")
            sys.exit(-1)
        elif os.path.isdir(arg0):
            logging.debug('%s is a directory!!' % (localStr(arg0)))
            fileList = walkDirAndReturnListOfFiles(arg0)
            lenAllFiles = localLen(fileList)
            logging.debug('Found %i files!' % (lenAllFiles))
            logging.debug('Getting their sizes...')
            for aFile in fileList:
                fileSize = getFileSizeFromOS(aFile)
                if fileSize > 0:
                    if fileSizeDict.get(fileSize) is None:
                        fileSizeDict[fileSize] = [aFile]
                        fileSizeList.append(fileSize)
                    else:
                        fileSizeDict[fileSize].append(aFile)
            logging.debug('Populated a dictionary of file sizes!')

            sortedSizes = []
            fileSizeList.sort()
            sortedFileSizes = fileSizeList
            for sortedSize in sortedFileSizes:
                sortedSizes.append([sortedSize, fileSizeDict.get(sortedSize)])
            sortedSizes.sort()
            logging.debug('Sorting a list of %i file sizes!' % (lenAllFiles))
            logging.debug('\tdeduplicating that list...')
            sortedSizes = removeDuplicatesForHeuristic(sortedSizes)

            localPrint('Heuristically identified %i possible duplicate files, from a set of %i files!' % (localSum([localLen(size[1]) for size in sortedSizes]), lenAllFiles))

            fileHashes = []
            out_q = queue.Queue()

            aWorker = Worker(algorithm)
            logging.debug('Starting computation!')
            if heuristic is None:
                #this is depreciated, will be removed!
                logging.debug('\tNOT computing with heuristic!')
                try:
                    for sortedSizeAndPaths in sortedSizes:
                        listPaths = sortedSizeAndPaths[1]
                        for aPath in listPaths:
                            result = aWorker.compute(aPath, incremental=True)
                            out_q.put(result)
                        out_q.put(False)
                        nextHash = out_q.get()
                        while nextHash:
                            fileHashes.append(nextHash)
                            nextHash = out_q.get()
                    logging.debug('\tComputation complete!')
                except localKeyboardInterrupt:
                    sys.exit()

                for (fileFullPath, fileHashHex) in fileHashes:
                    try:
                        it = knownFiles.get(fileHashHex)
                        if it is None:
                            knownFiles[fileHashHex] = [fileFullPath]
                        else:
                            knownFiles[fileHashHex].append(fileFullPath)
                    except localKeyboardInterrupt:
                        sys.exit()

            elif heuristic is not None:
                logging.debug('\tComputing with heuristic!')
                #each item in sortedSizes should be format: [36, ['C:\\Users\\Alexander Riccio\\Documents\\t.txt']]
                try:
                    if stopOnFirstDiff:
                        for aFileNameList in sortedSizes:
                            #instead of aFileNameList[0] (which is the size of the file) as the second argument to computMultipleByteArrays, should pass a chunk size!
                            #logging.debug("\t\tcomputing file of size %i" % aFileNameList[0])
                            numberOfFilesOfSize = len(aFileNameList[1])
                            #if there are more than 512 files of size aFileNameList[0], we need to increase the number of files that we can open!
                            if numberOfFilesOfSize > 512:
                                #There is NO platform-independent way of doing this!
                                if ('win32' or 'win64') in sys.platform:
                                    #The Microsoft Visual C runtime library WILL NOT let us open more than 2048 files at a time!
                                    if numberOfFilesOfSize < 2049:
                                        logging.warning("\tThere are so many (%i) files of size %i that we need to increase the number of files that we can open at once! To do this, we need to import ctypes and call _setmaxstdio in the MSVCRT library. THIS WILL BE SLOW!" % (len(aFileNameList[1]), aFileNameList[0]))
                                        import ctypes
                                        if sys.version_info.major < 3:
                                            #python 2.6 -> 3.2 link against msvcr90
                                            if sys.version_info.minor > 5:
                                                logging.debug("Name of microsoftVisualCRuntimeLibrary: msvcr90")
                                                newMax = ctypes.cdll.msvcr90._setmaxstdio(2048)
                                            else:
                                                sys.exit("what the hell are you using such an old version of python for??!?")
                                        elif sys.version_info.major > 2:
                                            if sys.version_info.minor < 3:
                                                #python 2.6 -> 3.2 link against msvcr90
                                                logging.debug("Name of microsoftVisualCRuntimeLibrary: msvcr90")
                                                newMax = ctypes.cdll.msvcr90._setmaxstdio(2048)
                                            elif sys.version_info.minor == 3:
                                                #python 3.3 links against msvcr100
                                                logging.debug("Name of microsoftVisualCRuntimeLibrary: msvcr100")
                                                newMax = ctypes.cdll.msvcr100._setmaxstdio(2048)
                                            elif sys.version_info.minor > 4:
                                                logging.debug("I don't have a crystal ball, trying to use msvcr100")
                                                newMax = ctypes.cdll.msvcr100._setmaxstdio(2048)
                                        else:
                                            logging.error("I have no idea what Python 4+ will link against!")
                                            sys.exit("You are from the future. I can't support you.")
                                        if newMax != 2048:
                                            logging.fatal("Failed to increase number of files openable at once! Got return value: %s" % str(newMax))
                                            sys.exit("_setmaxstdio failed!")
                                    elif numberOfFilesOfSize > 2048:
                                        logging.fatal("There are FAR too many files of size %i! We cannot possibly open %i files at once!" % (aFileNameList[0], numberOfFilesOfSize))
                                        sys.exit("TOO MANY FILES!")
                                else:
                                    if numberOfFilesOfSize > 1024:
                                        logging.fatal("There are too many files of size %i! You'd need to increase the unix per-process limit; support for doing this automatically isn't available at this time" % aFileNameList[0])
                                        sys.exit("TOO MANY FILES!")
                            result = aWorker.computeMultipleByteArrays(aFileNameList[1], aFileNameList[0], incremental=True)
                            #logging.debug("\t\tknown files so far: %s" % str(knownFiles))
                            for aKey in result.keys():
                                #logging.debug("\t\t\taFileName:           %s" % str(aKey))
                                #logging.debug("\t\t\tresult[aFileName][2] %s" % str(result[aKey][2].name))
                                try:
                                    it = knownFiles.get((result[aKey][2]))
                                    if it is None:
                                        knownFiles[(result[aKey][2].name)] = [aKey]
                                    else:
                                        knownFiles[(result[aKey][2].name)].append(aKey)
                                except localIndexError:
                                    #knownFiles[(result[aKey][2].name)] = [aKey]
                                    pass
##                                knownFiles = fileHashHexDict
                    else:
                        #this is depreciated, will be removed!
                        for aFileNameList in sortedSizes:
                            for thisHashFileName in aFileNameList[1]:
                                result = aWorker.computeByteArray(thisHashFileName, aFileNameList[0], incremental=True)
                                fileHashes.append(result)
                        for (fileFullPath, fileHashHex) in fileHashes:
                            try:
                                it = knownFiles.get(fileHashHex)
                                if it is None:
                                    knownFiles[fileHashHex] = [fileFullPath]
                                else:
                                    knownFiles[fileHashHex].append(fileFullPath)
                            except localKeyboardInterrupt:
                                sys.exit()


                    logging.debug('\tComputation complete!')
                except localKeyboardInterrupt:
                    sys.exit()

        else:
            raise IOError("Specified file or directory not found!")
        localPrint("\tComputation complete!")
        wastedSpace = printDuplicateFilesAndReturnWastedSpace(knownFiles, stopOnFirstDiff, showZeroBytes)
        localPrint('\n')
        if NO_HUMANFRIENDLY is None:
            localPrint("%s bytes of wasted space!" % wastedSpace)
        elif NO_HUMANFRIENDLY is not None:
            localPrint("%s of wasted space!" % humanfriendly.format_size(wastedSpace))

def _profile(continuation):
    prof_file = 'duplicateFileFinder.prof'
    try:
        import cProfile
        import pstats
        print('Profiling using cProfile')
        cProfile.runctx('continuation()', globals(), locals(), prof_file)
        stats = pstats.Stats(prof_file)
    except ImportError:
        import hotshot
        import hotshot.stats
        prof = hotshot.Profile(prof_file, lineevents=1)
        print('Profiling using hotshot')
        prof.runcall(continuation)
        prof.close()
        stats = hotshot.stats.load(prof_file)
    stats.strip_dirs()
    #for a in ['calls', 'cumtime', 'cumulative', 'ncalls', 'time', 'tottime']:
    for a in ['cumtime', 'time', 'ncalls']:
        print("------------------------------------------------------------------------------------------------------------------------------")
        try:
            stats.sort_stats(a)
            stats.print_stats(150)
            stats.print_callees(150)
            stats.print_callers(150)
        except KeyError:
            pass
    os.remove(prof_file)



def main():
    parser = argparse.ArgumentParser(prog='Duplicate File Finder')
    parser.add_argument("--hash", dest="hashname", default="auto", help="select hash algorithm")
    parser.add_argument("--heuristic", dest="heuristic", default=True, help="Attempt to hash ONLY files that may be duplicates. ON by default")
    parser.add_argument("--debug", action='store_true', dest="isDebugMode", default=False, help="For the curious ;)")
    parser.add_argument('--profile', action='store_true', dest='profile', default=False, help="for the hackers")
    parser.add_argument("--stopFirstDiff", action='store_true', dest='stopOnFirstDiff', default=True, help="stops reading at first chunk that diverges. ON by default.")
    parser.add_argument("--showZeroByteFiles", action='store_true', dest='showZeroBytes', default=False, help="shows files of size 0")
    parser.add_argument("--stfu", "--beQuiet", action='store_true', dest='stfu', default=False, help="Whine about 'warning's less")
    parser.add_argument("directory", action='store', help='directory to scan')
    parser.add_argument("--halt", action='store_true', dest='halting', default=False, help="Used to study module imports") 
    #logging.basicConfig(level=logging.DEBUG)
    args = parser.parse_args()

    if args.halting:
        print("halt!")
        sys.exit(0)
    if args.stfu and args.isDebugMode:
        print("Do you want me to shut up or log everything?!?! Make up your mind!")
        sys.exit("Choose one!")
    elif args.stfu:
        logging.basicConfig(level=logging.ERROR)
    elif args.isDebugMode:
        logging.basicConfig(level=logging.DEBUG)
        logging.debug('debug mode set!')
    else:
        logging.basicConfig(level=logging.WARNING)
        logging.debug('not debugging! If you see this, something is wrong.')

    if args.hashname == "auto":
        args.hashname = "sha1"
        logging.debug("'auto' as hash selected, so defaulting to 'sha1'\n")
    logging.info("This is a VERY I/O heavy program. You may want to temporarily exclude %s from anti-malware/anti-virus monitoring, especially for Microsoft Security Essentials/Windows Defender. That said, I've never seen Malwarebytes Anti-Malware have a performance impact; leave MBAM as it is." % (str(sys.executable)))
    heuristic = args.heuristic
    algorithm = args.hashname
    showZeroBytes = args.showZeroBytes
    stopOnFirstDiff = args.stopOnFirstDiff
    if args.profile:
        def safe_main():
            try:
                main_method(heuristic, algorithm, stopOnFirstDiff, args, showZeroBytes)
            except:
                pass
        _profile(safe_main)
    else:
        try:
            main_method(heuristic, algorithm, stopOnFirstDiff, args, showZeroBytes)
        except KeyboardInterrupt:
            sys.exit(0)
if __name__ == "__main__":
    main()

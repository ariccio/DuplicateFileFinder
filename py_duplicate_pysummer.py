#!C:\Python33\Python.exe
#-*- coding:utf-8 -*-
'''
Based DISTANTLY on code by JEANNENOT Stephane

idea: for possibly duplicate files, read a block at position 2^(number of iterations)
'''
from __future__ import print_function
import sys
if sys.version_info[0] < 3:
    from exceptions import NotImplementedError, ValueError, IOError

#support for python 2.7 and 3
if sys.version_info.major < 3:
    import Queue as queue
elif sys.version_info.major > 2:
    import queue

import logging
import os
import hashlib
import contextlib
import io
import itertools
import collections
NO_HUMANFRIENDLY = True
try:
    import humanfriendly
except ImportError:
    print('Install "human friendly" with "pip install humansize" for human-readable file sizes')
    NO_HUMANFRIENDLY = None
# TODO : replace optparse with argparse : will brake compatibility with Python < 2.7

from optparse import OptionParser

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
        #TODO: eliminate function call overhead associated with calling computeByteArray for EVERY goddamned file!
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
        localDictOfFileHandles = {}
        localDictOfBytes = {}
        localDictOfFileHashResults = {}
        localByteBuffer = bytearray(fSize[0])
        localHashLib = hashlib
        for item in localListOfFileNames:
            #localListOfHashWorkers.append(hashlib.new('sha1'))
            localDictOfFileHashResults[item] = [localHashLib.new('sha1'), bytearray(fSize[0])]
        fSize[0] = 0
        logging.debug('\tComputing multiple byte arrays: %s' % str(localListOfFileNames))
        logging.debug('\tfSize: %i' % fSize[-1])
        try:
            with contextlib.ExitStack() as stack:
                for fileName in localDictOfFileHashResults.keys():
                    logging.debug('\n\t\t\tworking on %s...' % str(fileName))
                    localDictOfFileHashResults[fileName].append(stack.enter_context(io.open(fileName, 'rb')))
                    keepReading = True
                    localDictOfFileHashResults[fileName].append(keepReading)
                    #logging.debug('reading %i bytes...' % (int(fSize)*8))
                    localDictOfBytes[fileName] = localDictOfFileHashResults[fileName][2].readinto(localDictOfFileHashResults[fileName][1])
                    logging.debug('\t\tread %i bytes!' % (int(localDictOfBytes[fileName])*8))
                    #localDictOfFileHashResults[key] = [hashlibSHA1, bytearray, fileObject.rb, bool]
                #logging.debug("\tsum([localDictOfBytes[aFile] for aFile in localDictOfBytes.keys()]): %s" % str(sum([localDictOfBytes[aFile] for aFile in localDictOfBytes.keys()])))
##                logging.debug("\tany(localDictOfFileHashResults[aSingleFileName][3] for aSingleFileName in localDictOfFileHashResults.keys()) %s\n\n" % str(any(localDictOfFileHashResults[aSingleFileName][3] for aSingleFileName in localDictOfFileHashResults.keys())))
##                while sum([localDictOfBytes[aFile] for aFile in localDictOfBytes.keys()]) != 0 and any(localDictOfFileHashResults[aSingleFileName][3] for aSingleFileName in localDictOfFileHashResults.keys()):
                logging.debug("\t\tfSize[-1]: %i" % fSize[-1])
                logging.debug("\t\tlocalDictOfBytes[fileName]: %i" % localDictOfBytes[fileName])
                while any(localDictOfFileHashResults[aSingleFileName][3] for aSingleFileName in localDictOfFileHashResults.keys()) and fSize[-1] <= fileSize:
                    fSize.append(2**iteration)
                    for fileName in localDictOfFileHashResults.keys():
                        logging.debug('\t\t\tworking on %s...' % str(fileName))
                        if localDictOfFileHashResults[fileName][3]:
                            #localDictOfFileHashResults[fileName][1].clear
                            #localDictOfFileHashResults[fileName][1] = bytearray(fSize)
##                            localDictOfFileHashResults[fileName][0].update(localDictOfFileHashResults[fileName][1][:localDictOfBytes[fileName]])
                            logging.debug("\t\t\tfSize[-2]:fSize[-1] %i:%i" % (fSize[-2],fSize[-1]))
                            localDictOfFileHashResults[fileName][0].update(localDictOfFileHashResults[fileName][1][fSize[-2]:fSize[-1]])
                            #logging.debug('\treading %i bytes...' % (int(fSize)*8))
                            #localDictOfBytes[fileName] = localDictOfFileHashResults[fileName][2].readinto(localDictOfFileHashResults[fileName][1])
                            #logging.debug('\tread %i bytes!' % (int(localDictOfBytes[fileName])*8))
#                    for fileName in localDictOfFileHashResults.keys():
                        #logging.debug("\t\t\t\tany(localDictOfBytes[aNumRead] > 0 for aNumRead in localDictOfBytes.keys()) %s" % str(any(localDictOfBytes[aNumRead] > 0 for aNumRead in localDictOfBytes.keys())))
                        #logging.debug("\t\t\t\tlen(localDictOfFileHashResults.keys()) %s" % str(len(localDictOfFileHashResults.keys())))
                    #if len([key for key in localDictOfFileHashResults.keys()]) >0:
                        #logging.debug("\t\t\t\tall(blah[fileName][0].hexdigest() ==  blah[aFileName][0].hexdigest() for aFileName in blah.keys() if blah[aFileName]).......................%s" % str(all(localDictOfFileHashResults[fileName][0].hexdigest() == localDictOfFileHashResults[aFileName][0].hexdigest() for aFileName in localDictOfFileHashResults.keys() if localDictOfFileHashResults[aFileName])))
                        #logging.debug("\t\t\t\tlen([key for key in localDictOfFileHashResults.keys()]).....................................................................................%s" % str(len([key for key in localDictOfFileHashResults.keys()])))
                        #logging.debug("\t\t\t\t[blah[fileName][0].hexdigest() == blah[aFileName][0].hexdigest() for aFileName in blah.keys() if blah[aFileName] and aFileName !=fileName]: %s" % (str([localDictOfFileHashResults[fileName][0].hexdigest() == localDictOfFileHashResults[aFileName][0].hexdigest() for aFileName in localDictOfFileHashResults.keys() if localDictOfFileHashResults[aFileName] and aFileName !=fileName])))
                        #logging.debug("\t\t\t\t[blah[fileName][0].hexdigest(), blah[aFileName][0].hexdigest()..............................................................................%s" % (str([(localDictOfFileHashResults[fileName][0].hexdigest(), localDictOfFileHashResults[aFileName][0].hexdigest()) for aFileName in localDictOfFileHashResults.keys() if localDictOfFileHashResults[aFileName] and aFileName !=fileName])))
                        #logging.debug("\t\t\t\tlocalDictOfFileHashResults[fileName][3]:....................................................................................................%s" % str(localDictOfFileHashResults[fileName][3]))
                    #if any(localDictOfBytes[aNumRead] > 0 for aNumRead in localDictOfBytes.keys()) and all(localDictOfFileHashResults[fileName][0].hexdigest() == localDictOfFileHashResults[aFileName][0].hexdigest() for aFileName in localDictOfFileHashResults.keys() if localDictOfFileHashResults[aFileName]) and (len([key for key in localDictOfFileHashResults.keys()])>1):
                    if all(localDictOfFileHashResults[fileName][0].hexdigest() == localDictOfFileHashResults[aFileName][0].hexdigest() for aFileName in localDictOfFileHashResults.keys() if localDictOfFileHashResults[aFileName] and aFileName !=fileName):
                        if len(localDictOfFileHashResults.keys())>1:
                            logging.debug('\t\t\t\tall converge\n')
                            #logging.debug("\t\t\t\tlen(localDictOfFileHashResults.keys())>1 %i" % len(localDictOfFileHashResults.keys()))
                        #pass
                        else:
                            pass
                            #logging.debug("\t\t\t\tlen([key for key in localDictOfFileHashResults.keys()])NOT>1 %i" % len(localDictOfFileHashResults.keys()))
                    else:
                        logging.debug('\t\t\t\t\t%s diverges!\n' % str(fileName))
                        localDictOfFileHashResults[fileName][3] = False
##                    logging.debug("\t\t\tlocalDictOfFileHashResults[fileName][3] for fileName in keys: %s\n" % str([localDictOfFileHashResults[fileName][3] for fileName in localDictOfFileHashResults.keys()]))
                    iteration += 1
        except PermissionError:
            logging.warning("PermissionError while opening %s" % (str(localFName)))
        #returnResult[fileName] = [_hashlib.HASH, someByteArray, hexdigest, didReadEntireFile]
        #return returnResult
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
    localOS = os
    try:
        fileSizeInBytes = int(localOS.stat(theFileInQuestion).st_size)

    except WindowsError as winErr:
        #logging.warning('Windows error %s while getting size of "%s" in printDuplicateFilesAndReturnWastedSpace!\n\tMay god have mercy on your soul.\n\n' % ( str(winErr.errno), str(theFileInQuestion)))
        if winErr.errno == 1920:
            if localOS.path.islink(theFileInQuestion):
                fileSizeInBytes = localOS.path.getSize(theFileInQuestion)
            else:
                fileSizeInBytes = 0
        elif winErr.errno == 2:
            localLogging.warning('Windows could not find %s, and thereby failed to find the size of said file.' % (theFileInQuestion))
            fileSizeInBytes = 0
        else:
            localLogging.warning('Windows error %s while getting size of "%s"!\n\tMay god have mercy on your soul.\n\n' % (str(winErr.errno), theFileInQuestion))
            fileSizeInBytes = 0
        #fileSizeInBytes = 0
    return fileSizeInBytes

def printListOfDuplicateFiles(listOfDuplicateFiles, showZeroBytes):
    '''
    expects a list:
        item[0] = the SIZE of a file
        item[1] = a list of fileNAMES with that size
    '''
    localLogging = logging
    localLogging.debug('\tprinting list of duplicate files!')
    if NO_HUMANFRIENDLY is None:
        localLogging.debug('\t\thumanfriendly NOT installed, proceeding with crappy formatting...')
        for item in listOfDuplicateFiles:
##            localLogging.debug("item: %s" % str(item))
            if item[0] > 0 or showZeroBytes:
                print("\n%s:" % (str(item[0])))
                for aFileName in item[1]:
                    try:
                        #print("\t%s" % (str(aFileName)))
                        print("\t%s" % (aFileName))
                    except UnicodeEncodeError:
                        localLogging.warning("\t\t\tfilename is evil! filename: ", aFileName)

    elif NO_HUMANFRIENDLY is not None:
        localLogging.debug('\t\thumanfriendly IS installed, proceeding with nice formatting...')
        for item in listOfDuplicateFiles:
##            localLogging.debug("\t\t\t\titem: %s" % str(item))
            if item[0] > 0 or showZeroBytes:
                try:
                    item[0] = humanfriendly.format_size(item[0])
                    #print('\n%s:' % (str(item[0])))
                    print('\n%s:' % (item[0]))
                    for aFileName in item[1]:
                        print('\t%s' % (aFileName))
                        #print('\t%s' % (aFileName))
                except ValueError:
                    localLogging.warning('\t\tError formatting item %s for human friendly printing!' % str(item[0]))
                    localLogging.debug('\t\t\tItem: "%s" at fault!' % (str(item)))
                    for i in range(len(item)):
                        indent = '\t' * (4 + i)
                        localLogging.debug('%sitem[%i]: %s' % (indent, i, str(item[i])))
                    #sys.exit()
                except UnicodeEncodeError:
                    localLogging.error('\t\tfilename is evil! filename: "%s"' % (str(aFileName)))
                except:
                    print('---------------------------------------------------------------------')
                    localLogging.fatal('SOMETHING IS VERY WRONG!')
                    sys.exc_info()
                    print("faulting item: ", item)
                    sys.exit(666)
                    print('---------------------------------------------------------------------')
    else:
        localLogging.error('Something is VERY wrong in printListOfDuplicateFiles')


def printDuplicateFilesAndReturnWastedSpace(extKnownFiles, stopOnFirstDiff, showZeroBytes):
    '''
    prints duplicate files (as of now, only those > 0 bytes) in the form of

    someVeryLongHashOfADuplicateFile:
    numBytes bytes          pathToFile

    e.g.:

    f9f629771f00de09cb63f10a4dbae4d7a8f72544:
    59217687 bytes          C:\\Users\\Alexander Riccio\\Downloads\\testDir\\blah.pdf
    59217687 bytes          C:\\Users\\Alexander Riccio\\Downloads\\testDir\\blahBlah.pdf

    If you have humanfriendly ("humansize") installed, the output will be more readable - i.e. 1 KB, not 1024.
    I also increment a counter, wastedSpace, by filesize over each file.
    '''
    wastedSpace = 0
    sizeOfKnownFiles = {}
    knownFiles = extKnownFiles
    localLogging = logging
    localLogging.debug('\tcalculating wasted space...')
    localHumanFriendly = humanfriendly
    for key in knownFiles:
        aFile = knownFiles[key][0]
        fileSizeInBytes = getFileSizeFromOS(aFile)
        #localLogging.debug('\t\tgot file size: %i' % fileSizeInBytes)
        #localLogging.debug('\t\tknownFiles[%s] %s' % (str(key), str(knownFiles[key])))
        if (len(knownFiles[key]) > 0) or (stopOnFirstDiff):
            if fileSizeInBytes > 0 and not stopOnFirstDiff:
                wastedSpace += fileSizeInBytes * (len(knownFiles[key])-1)
                sizeOfKnownFiles[key] = fileSizeInBytes * (len(knownFiles[key])-1)
                localLogging.debug("\n\t\t\tkey:%s:"%key)
            elif fileSizeInBytes >0 and stopOnFirstDiff:
                wastedSpace += fileSizeInBytes * (len(knownFiles[key]))
                sizeOfKnownFiles[key] = fileSizeInBytes * (len(knownFiles[key]))
                #localLogging.debug("\n\t\t\tkey:%s:"%key)
            elif fileSizeInBytes == 0:
                sizeOfKnownFiles[key] = 0
            if NO_HUMANFRIENDLY is None:
                for aSingleFile in knownFiles[key]:
                    localLogging.debug("\t\t\t%s bytes\t\t\t%s" % (fileSizeInBytes, aSingleFile))
                    #localLogging.debug('\t\twasted space so far: %i' % wastedSpace)
            elif NO_HUMANFRIENDLY is not None:
                for aSingleFile in knownFiles[key]:
                    localLogging.debug("\t\t\t%s\t\t\t%s" % (localHumanFriendly.format_size(fileSizeInBytes), aSingleFile))
                    #localLogging.debug('\t\twasted space so far: %s' % localHumanFriendly.format_size(wastedSpace))
        #localLogging.debug('\t\twasted space so far: %i' % wastedSpace)
    localLogging.debug('\tcalculated wasted space: %i' % wastedSpace)
    sortedSizeOfKnownFiles = sorted(sizeOfKnownFiles, key=knownFiles.__getitem__)
    sortedListSize = []
    logging.debug('\n\n')
    for sortedHash in sortedSizeOfKnownFiles:
        #logging.debug("\t\t\tfor sortedHash in sortedSizeOfKnownFiles: %s" % str([sizeOfKnownFiles.get(sortedHash), knownFiles.get(sortedHash)]))
        sortedListSize.append([sizeOfKnownFiles.get(sortedHash), knownFiles.get(sortedHash)])
    sizes = {}
    logging.debug("\t\tsortedListSize: %s" % str(sortedListSize))
    logging.debug('\n\n')
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
        logging.debug("\t\t\tsizes:")
        for aSizeKey in sizes.keys():
             logging.debug("\t\t\t\t%s"% str(sizes[aSizeKey]) )
        logging.debug("\t\t\t-----------------------")
        #it = sizes.get(size[0])
        #logging.debug("\t\t\t\t\tit is now: %s\n" % str(it))
##    listSizes = list(sizes.keys())
##    #logging.debug("listSizes: %s" % str(listSizes))
##    logging.debug('\n\n')
##    listSizes.sort()
##    nList = []
##    for item in listSizes:
##        logging.debug("\t\t\t\titem: %s" % str(item))
####        listSizes[listSizes.index(item)] = [item, [item_name for x in sizes[item] for item_name in x]]
##        for aNestList in sizes[item]:
##            try:
##                nList[nList.index(item)].append(aNestList)
##            except ValueError:
##                nList.append(aNestList)
##    logging.debug('\n\n')
##    for anItem in nList:
##        logging.debug("\t\t\t\t\tanItem: %s" % str(anItem))
##        #works
        
    sortedListSize.sort()
##    logging.debug("\t\tdict sizes: ")

##    for key in sizes.keys():
##        logging.debug("\t\t\tkey:%s: sizes[key]: %s" % (str(key), str(sizes[key])))

    logging.debug('\n\n')
    logging.debug("\t\tsortedListSize has been sort()ed!")

    for item in sortedListSize:
        logging.debug("\t\t\titem: %s" % str(item))
    

##    for item in sortedListSize:
##        a = collections.Counter(sublist[0] for sublist in sortedListSize if sublist[0] == item[0])
##        countOfSizes = a.get(item[0])
##        dupIndices =[]
##        logging.debug("\t\t\tcollections.Counter(sublist[0] for sublist in sortedListSize): %s" % (str(countOfSizes)))
##        if countOfSizes > 1:
##            logging.debug("\t\t\t\titem[0] '%s' not unique!" % str(item[0]))
##            for eachIndex in sortedListSize:
##                if eachIndex in dupIndices:
##                    logging.debug("\t\t\t\tremoving sortedListSize[%i]" % sortedListSize.index(eachIndex))
##                    sortedListSize[sortedListSize.index(eachIndex)] = None
    ##                else:
    ####                    try:
    ##                        #dupIndices.append(sortedListSize[dupIndices[-1]:].index(eachIndex))
    ##                        #logging.debug("\t\t\t\tappended %s..." % str(dupIndices[-1]))
    ####                    except IndexError:
    ##                    dupIndices.append(sortedListSize.index(eachIndex))
    ##                    logging.debug("\t\t\t\tappended %s..." % str(dupIndices[-1]))
    ##            duplicateItemIndices = dupIndices
    ##            #duplicateItemIndices = sortedListSize.index(sublist[0] for sublist in sortedListSize if sublist[0] == item[0])
    ##            for duplicateItemIndex in duplicateItemIndices:
    ##                #logging.debug("\t\t\t\t\tchecking %s" % str(duplicateItemIndex))
    ##                if all([len(sortedListSize[duplicateItemIndex][1]) > len(sortedListSize[aDuplicateItemIndex][1]) for aDuplicateItemIndex in duplicateItemIndices if aDuplicateItemIndex != duplicateItemIndex and sortedListSize[aDuplicateItemIndex] is not None]):
    ##                    #logging.debug("\t\t\t\t\tremoving duplicates")
    ##                    #logging.debug("\t\t\t\t\t\t\tduplicateItemIndices: %s" % str(duplicateItemIndices))
    ##                    #[(logging.debug("\n\t\t\t\t\t\tremoving %s!" % str(aDuplicateItemIndex)), (sortedListSize.__setitem__(aDuplicateItemIndex, ['', [0]]))) for aDuplicateItemIndex in duplicateItemIndices if aDuplicateItemIndex != duplicateItemIndex]
    ##                    #logging.debug("\t\t\t\t\t\t\tduplicateItemIndices now: %s" % str(duplicateItemIndices))
    ##        for anIndex in dupIndicies:
    ##            sortedListSize
##        else:
##            pass
    newLSizes = []
    logging.debug("\t\t\tsizes:")
    for aSizeKey in sizes.keys():
        #logging.debug("\t\t\t\t%s"% str(sizes[aSizeKey]) )
        if len(sizes[aSizeKey])>1:
            newLSizes.append([aSizeKey, sizes[aSizeKey]])
    logging.debug('\n\n\n\n')
    localLogging.debug('ready to print list of duplicate files!')
##    printListOfDuplicateFiles(sortedListSize, showZeroBytes)
    printListOfDuplicateFiles(newLSizes, showZeroBytes)
    return wastedSpace


def removeDuplicatesForHeuristic(sortedSizes):
    localLogging = logging
    for size in sortedSizes:
        #size should be format: [36, ['C:\\Users\\Alexander Riccio\\Documents\\t.txt']]
        try:
            if len(size[1]) < 2:
                #localLogging.debug('\t\tremoving sub-two element (%s[1]) list...\n' % (str(size[1])))
                sortedSizes.remove(size)
        except TypeError:
            localLogging.error('\t\titem "%s" is neither a sequence nor a mapping!' % (str(size)))
            sys.exit()
    return sortedSizes

def walkDirAndReturnQueueOfFiles(directoryToWalk):
    queueOfFiles = queue.Queue()
    logging.debug('Walking %s...' % (str(directoryToWalk)))
    for root, dirs, files in os.walk(directoryToWalk):
        '''move to:
            build queue of files -> build pool of processes -> start processes
            like I did in factah over summer, where mp_factorizer is passed nums(a list of numbers to factorize) and a number nprocs (how many processes)

        '''
        for fname in files:
            fullpath = os.path.abspath(os.path.join(root, fname))
            queueOfFiles.put(fullpath)
        queueOfFiles.put(False)
    return queueOfFiles


def walkDirAndReturnListOfFiles(directoryToWalk):
    ListOfFiles = []
    localOS = os
    logging.debug('Walking %s...' % (str(directoryToWalk)))
    for root, dirs, files in localOS.walk(directoryToWalk):
        '''move to:
            build queue of files -> build pool of processes -> start processes
            like I did in factah over summer, where mp_factorizer is passed nums(a list of numbers to factorize) and a number nprocs (how many processes)

        '''
        for fname in files:
            fullpath = localOS.path.abspath(localOS.path.join(root, fname))
            ListOfFiles.append(fullpath)
    return ListOfFiles


def main_method(heuristic, algorithm, stopOnFirstDiff, args, showZeroBytes):
    if args:
        arg0 = args[0]
        fileSizeList = []
        fileSizeDict = {}
        knownFiles = {}

        if os.path.isfile(arg0):
            logging.debug('%s is a file!' % (str(arg0)))
            w = Worker(algorithm)
            hw = w.compute(arg0)
            print("%s *%s" % (hw, arg0))
            knownFiles[hw] = arg0

        elif os.path.isdir(arg0):
            logging.debug('%s is a directory!!' % (str(arg0)))
            fileList = walkDirAndReturnListOfFiles(arg0)
            lenAllFiles = len(fileList)
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

            print('Heuristically identified %i possible duplicate files, from a set of %i files!' % (sum([len(size[1]) for size in sortedSizes]), lenAllFiles))

            fileHashes = []
            out_q = queue.Queue()

            aWorker = Worker(algorithm)
            logging.debug('Starting computation!')
            if heuristic is None:
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
                except KeyboardInterrupt:
                    sys.exit()

                for (fileFullPath, fileHashHex) in fileHashes:
                    try:
                        it = knownFiles.get(fileHashHex)
                        if it is None:
                            knownFiles[fileHashHex] = [fileFullPath]
                        else:
                            knownFiles[fileHashHex].append(fileFullPath)
                    except KeyboardInterrupt:
                        sys.exit()

            elif heuristic is not None:
                logging.debug('\tComputing with heuristic!')
                #each item in sortedSizes should be format: [36, ['C:\\Users\\Alexander Riccio\\Documents\\t.txt']]
                try:
                    if stopOnFirstDiff:
                        fileHashHexDict = {}
                        for aFileNameList in sortedSizes:
                            #instead of aFileNameList[0] (which is the size of the file) as the second argument to computMultipleByteArrays, should pass a chunk size!
                            result = aWorker.computeMultipleByteArrays(aFileNameList[1], aFileNameList[0], incremental=True)
                            ##localDictOfFileHashResults[key] = [hashlibSHA1, bytearray, fileObject.rb, bool]
                            logging.debug("\t\tknown files so far: %s" % str(knownFiles))
                            for aKey in result.keys():
##                                if result[aFileName][3]:
##                                    it = knownFiles.get(result[aFileName][2])
##                                    if it is None:
##                                        knownFiles[result[aFileName][2]] = [aFileName]
##                                    else:
##                                        knownFiles[result[aFileName][2]].append(aFileName)
##                                getIt = fileHashHexDict.get(result[aKey][0].hexdigest())
##                                if getIt is None:
##                                    fileHashHexDict[result[aKey][0].hexdigest()] = [result[aKey][2].name]
##                                else:
##                                    fileHashHexDict[result[aKey][0].hexdigest()].append(result[aKey][2].name)
                                logging.debug("\t\t\taFileName:           %s" % str(aKey))
                                logging.debug("\t\t\tresult[aFileName][2] %s" % str(result[aKey][2].name))
                                it = knownFiles.get((result[aKey][2]))
                                if it is None:
                                    knownFiles[(result[aKey][2].name)] = [aKey]
                                else:
                                    knownFiles[(result[aKey][2].name)].append(aKey)
##                                knownFiles = fileHashHexDict
                    else:
                        for aFileNameList in sortedSizes:
                            for thisHashFileName in aFileNameList[1]:
                                result = aWorker.computeByteArray(thisHashFileName, aFileNameList[0], incremental=True)
                                #maybe pass the size of file into computeByteArray, to then read that size file?
                                fileHashes.append(result)
                        for (fileFullPath, fileHashHex) in fileHashes:
                            try:
                                it = knownFiles.get(fileHashHex)
                                if it is None:
                                    knownFiles[fileHashHex] = [fileFullPath]
                                else:
                                    knownFiles[fileHashHex].append(fileFullPath)
                            except KeyboardInterrupt:
                                sys.exit()

                                
                    logging.debug('\tComputation complete!')
                except KeyboardInterrupt:
                    sys.exit()

        else:
            raise IOError("Specified file or directory not found!")

        wastedSpace = printDuplicateFilesAndReturnWastedSpace(knownFiles, stopOnFirstDiff, showZeroBytes)
        print('\n')
        if NO_HUMANFRIENDLY is None:
            print("%s bytes of wasted space!" % wastedSpace)
        elif NO_HUMANFRIENDLY is not None:
            print("%s of wasted space!" % humanfriendly.format_size(wastedSpace))

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
    for a in ['cumtime', 'cumulative', 'time']:
        try:
            stats.sort_stats(a)
            stats.print_stats(100)
            stats.print_callees(100)
            #stats.print_callers(10)
        except KeyError:
            pass
    os.remove(prof_file)



def main():
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage=usage)
    parser.add_option("--hash", dest="hashname", default="auto", help="select hash algorithm")
    parser.add_option("--heuristic", dest="heuristic", default=True, help="Attempt to hash ONLY files that may be duplicates. ON by default")
    parser.add_option("--debug", action='store_true', dest="isDebugMode", default=False, help="For the curious ;)")
    parser.add_option('--profile', action='store_true', dest='profile', default=False, help="for the hackers")
    parser.add_option("--stopFirstDiff", action='store_true', dest='stopOnFirstDiff', default=False, help="stops reading at first chunk that diverges")
    parser.add_option("--showZeroByteFiles", action='store_true', dest='showZeroBytes', default=False, help="shows files of size 0")
    logger = logging.getLogger('log')
    logging.basicConfig(level=logging.DEBUG)
    logging.warning("This is a VERY I/O heavy program. You may want to temporairily[TODO: sp?] exclude %s from anti-malware/anti-virus monitoring, especially for Microsoft Security Essentials/Windows Defender. That said, I've never seen Malwarebytes Anti-Malware have a performance impact; leave MBAM as it is." % (str(sys.executable)))
    (options, args) = parser.parse_args()

    #debugging = options.isDebugMode
    
    if options.isDebugMode:
        logging.basicConfig(level=logging.DEBUG)
        #logger.setLevel(logging.DEBUG)
        logging.debug('debug mode set!')
    else:
        logging.basicConfig(level=logging.WARNING)
        logging.debug('not debugging! If you see this, something is wrong.')
        
    if options.hashname == "auto":
        options.hashname = "sha1"
        logging.warning("'auto' as hash selected, so defaulting to 'sha1'\n")

    heuristic = options.heuristic
    algorithm = options.hashname
    showZeroBytes = options.showZeroBytes
    stopOnFirstDiff = options.stopOnFirstDiff
    if options.profile:
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

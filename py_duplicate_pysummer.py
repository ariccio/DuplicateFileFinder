#-*- coding:utf-8 -*-

## Author : JEANNENOT Stephane
## Mail : stephane.jeannenot@gmail.com
## Date : 13 May 2009 - First released on pypi (http://pypi.python.org/pypi)
## Date : 01 Feb 2013 - Some improvements and released on Google Code
## Date : 30 Jan 2014 - Release on Github
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
import copy
import io
NO_HUMANFRIENDLY = True
try:
    import humanfriendly#pip install humansize
except ImportError:
    NO_HUMANFRIENDLY = None
# TODO : replace optparse with argparse : will brake compatibility with Python < 2.7
from optparse import OptionParser, SUPPRESS_HELP

version = '1.0'


def multi_input(prompt):
    if sys.version_info.major < 3:
        return raw_input(prompt)
    else:
        return input(prompt)

class Worker():
    def __init__(self, hashname, rmode='rb', bufsize=16777216, name=None):#268435456 = 2^ 28#2097152
        self.hash_known  = ['md5', 'sha1', 'sha512']
        self.hash_length = {32:'md5', 40: "sha1", 128: "sha512"}
        self.__bufsize     = bufsize
        self.__hashname    = hashname
        self.__hashdata    = None
        self.__rmode       = rmode
        if name is not None:
            self.__name = name
        else:
            self.__name = "worker-%s" % self.__hashname

    def compute(self, fname, incremental=None):
        self.__fname = fname
        try:
            self.__hashdata = hashlib.new(self.__hashname)
        except ValueError:
            raise NotImplementedError("# %s : hash algorithm [ %s ] not implemented" % (self.__name, self.__hashname))
        #incremental = True
        except KeyboardInterrupt:
            sys.exit()
        try:
            logging.info('\tcompute opening %s' % ( str(self.__fname) ))
            with open(self.__fname, self.__rmode) as fhandle:
                if incremental is not None:
                    if NO_HUMANFRIENDLY is None:
                        logging.debug('\t\treading incrementally... (increments of: %s bytes)' % ( str(self.__bufsize) ) )
                    elif NO_HUMANFRIENDLY is not None:
                        logging.debug('\t\treading incrementally... (increments of: %s)' % ( humanfriendly.format_size(self.__bufsize) ) )

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

        logging.debug('\t\t\t\tfile: "%s" : %s' % (str(self.__fname), str(self.__hashdata.hexdigest())) )
        return (self.__fname, self.__hashdata.hexdigest())

    def computeByteArray(self, fname, fSize, incremental=None):
        #TODO: eliminate function call overhead associated with calling computeByteArray for EVERY goddamned file!
        localFName = fname
        localByteBuffer   = bytearray(fSize)
        localHashdata = hashlib.new('sha1')
        #incremental = True
        
        #pre-load globals!
##        localLogging = logging
##        localStr     = str
##        try:
##            localLogging.info('\tcompute opening %s' % ( localStr(self.__fname) ))
##            with io.open(self.__fname, 'rb') as fhandle:
##                if NO_HUMANFRIENDLY is None:
##                    localLogging.debug('\t\treading incrementally... (increments of: %s bytes)' % ( localStr(self.__bufsize) ) )
##                elif NO_HUMANFRIENDLY is not None:
##                    localLogging.debug('\t\treading incrementally... (increments of: %s)' % ( humanfriendly.format_size(self.__bufsize) ) )
##                totalData = 0
##                data = fhandle.readinto(self.__byteBuffer)
##                while data != 0:
##                    self.__hashdata.update(self.__byteBuffer[:data])
##                    data = fhandle.readinto(self.__byteBuffer)
##
##        except KeyboardInterrupt:
##            sys.exit()

##        localLogging.info('\tcompute opening %s' % ( localStr(self.__fname) ))
        try:
            with io.open(localFName, 'rb') as fhandle:
    ##            if NO_HUMANFRIENDLY is None:
    ##                localLogging.debug('\t\treading incrementally... (increments of: %s bytes)' % ( localStr(self.__bufsize) ) )
    ##            elif NO_HUMANFRIENDLY is not None:
    ##                localLogging.debug('\t\treading incrementally... (increments of: %s)' % ( humanfriendly.format_size(self.__bufsize) ) )
    ##            totalData = 0
                data = fhandle.readinto(localByteBuffer)
                while data != 0:
                    localHashdata.update(localByteBuffer[:data])
                    data = fhandle.readinto(localByteBuffer)
        except PermissionError:
            logging.warning("PermissionError while opening %s" % (str(localFName)))
##        localLogging.debug('\t\t\t\tfile: "%s" : %s' % (localStr(self.__fname), localStr(self.__hashdata.hexdigest())) )
        return (localFName, localHashdata.hexdigest())



def getFileSizeFromOS(theFileInQuestion):
    '''
    NEW (inspired by "C:\\Python27\\Lib\\genericpath.py".getsize(filename)) method:
    --------------------------------------------------------------------------------------------
    `os.stat(theFileInQuestion).st_size` returns some number followed by 'L' (<type 'long'>) in Python 2.x, some number without postfixed 'L' (<class 'int'>)
    e.g:
        os.stat("C:\\Users\\Alexander Riccio\\Documents\\GitHub\\DuplicateFileFinder\\README.md").st_size returns:
            Python 3.x: '42'
            Python 2.x: '42L'


    OLD ('backup') method:
    --------------------------------------------------------------------------------------------
    finding the filesize is a bitch. A call to os.stat("pathToSomeFile") returns (on Windows):
        See: "C:\\Python27\\Lib\\stat.py" for the code that's responsible for this behavior.
    nt.stat_result(st_mode=33206, st_ino=0L, st_dev=0, st_nlink=0, st_uid=0, st_gid=0, st_size=6253L, st_atime=1391494554L, st_mtime=1391507902L, st_ctime=1391494554L)

    Not exactly readable.

    What we want is st_size, which is 6253 (bytes). This may be less useful than you think, as with NTFS filesystem level compression the filesize ON DISK can be tremendously different.

    The return type of os.stat(), on Windows, is \'nt.stat_result\', so first I strify it.
    Then, accessing the string as a list, I grab everything between [inbetween?] the parenthesis (i.e. st_mode=...st_ctime=).
    Next, I split the remaining string with \', \' as the delimiter.
    I then split each item in (what used to be a str) the list with \'=\' as the delimiter.
    Now each item in the list is another (2 item) list (i.e. [[\'st_mode\', \'333206\'],...]).
    I\'m not sure if the position of st_size is constant, so I now iterate over the list (yes, slow linear search) until I find where it is, and store that location.
    I then store the second item at that position, i.e.:
             this one↓↓↓↓
    [[\'st_size\', \'6253L\'],...]
    Note the trailing L  ↑!
    I need to trim that trailing \'L\', else we'll run into problems later. So I store the list at [:-1]
    Now I intify our nicely trimmed integer-only str, and we\'re done!

    ...mostly - now it's time to display this information in a usuable mannner
    -------------------------------------------------------------------------------------------
    >>> (fd, path) = tempfile.mkstemp(); os.write(fd, 'aaaa'); os.close(fd); print os.stat(path).st_size; os.remove(path)
    4
    4

    '''
    try:
        fileSizeInBytes = int(os.stat(theFileInQuestion).st_size)

    except WindowsError as winErr:
        #logging.warning('Windows error %s while getting size of "%s" in printDuplicateFilesAndReturnWastedSpace!\n\tMay god have mercy on your soul.\n\n' % ( str(winErr.errno), str(theFileInQuestion)))
        if winErr.errno == 1920:
            if os.path.islink(theFileInQuestion):
                fileSizeInBytes = os.path.getSize(theFileInQuestion)
            else:
                fileSizeInBytes = 0
        elif winErr.errno == 2:
            logging.warning('Windows could not find %s, and thereby failed to find the size of said file.' % (str(theFileInQuestion)))
            fileSizeInBytes = 0
        else:
            logging.warning('Windows error %s while getting size of "%s"!\n\tMay god have mercy on your soul.\n\n' % ( str(winErr.errno), str(theFileInQuestion)))
            fileSizeInBytes = 0
        #fileSizeInBytes = 0
    return fileSizeInBytes

def printListOfDuplicateFiles(listOfDuplicateFiles):
    '''
    expects a list:
        item[0] = the SIZE of a file
        item[1] = a list of fileNAMES with that size
    '''
    if NO_HUMANFRIENDLY is None:
        for item in listOfDuplicateFiles:
            print("\n%s:" % (str(item[0])))
            for aFileName in item[1]:
                try:
                    print("\t%s" % (str(aFileName)))
                except UnicodeEncodeError:
                    logging.warning("\t\t\tfilename is evil! filename: ", aFileName)

    elif NO_HUMANFRIENDLY is not None:
        for item in listOfDuplicateFiles:
            try:
                item[0] = humanfriendly.format_size(item[0])
                print('\n%s:' % (str(item[0])))
                for aFileName in item[1]:
                    print('\t%s' % (str(aFileName)))
            except ValueError:
                logging.warning('\t\tError formatting item for human friendly printing!')
                logging.debug('\t\t\tItem: "%s" at fault!' % ( str(item) ))
                for i in range(len(item)):
                    indent = '\t' * (4 + i)
                    logging.debug('%sitem[%i]: %s' % (indent, i, str(item[i])))
                sys.exit()
            except UnicodeEncodeError:
                logging.warning('\t\tfilename is evil! filename: "%s"' % ( str(aFileName)) )



def printDuplicateFilesAndReturnWastedSpace(knownFiles):
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

    logging.debug('\tcalculating wasted space...')

    for key in knownFiles:
        aFile = knownFiles[key][0]
        fileSizeInBytes =  getFileSizeFromOS(aFile)

        if len(knownFiles[key]) > 1:
            wastedSpace += fileSizeInBytes * len(knownFiles[key])

            if fileSizeInBytes > 0:
                sizeOfKnownFiles[key] = fileSizeInBytes * len(knownFiles[key])
                logging.debug("\n\t\t\t%s:"%key)
                
            if NO_HUMANFRIENDLY is None:
                for aSingleFile in knownFiles[key]:
                    logging.debug("\t\t\t%s bytes\t\t\t%s" % (fileSizeInBytes, aSingleFile))

            elif NO_HUMANFRIENDLY is not None:
                for aSingleFile in knownFiles[key]:
                        logging.debug("\t\t\t%s\t\t\t%s" % (humanfriendly.format_size(fileSizeInBytes), aSingleFile))

    sortedSizeOfKnownFiles = sorted(sizeOfKnownFiles, key=knownFiles.__getitem__)
    sortedListSize = []

    for sortedHash in sortedSizeOfKnownFiles:
        sortedListSize.append([sizeOfKnownFiles.get(sortedHash), knownFiles.get(sortedHash)])

    sortedListSize.sort()

    printListOfDuplicateFiles(sortedListSize)

    return wastedSpace


def removeDuplicatesForHeuristic(sortedSizes):
    #deDupeNeeded = False
    for size in sortedSizes:
        #size should be format: [36, ['C:\\Users\\Alexander Riccio\\Documents\\t.txt']]
        try:
            if len(size[1]) < 2:
                logging.debug('\t\tremoving sub-two element (%s[1]) list...\n' % (str(size[1])))
                sortedSizes.remove(size)
                #deDupeNeeded = True
            #else:
                #logging.debug('\t\tNOT removing element of size %i for fileSize %s' % (len(size[1]), str(size[0])))
                #if len(size[1]) == 2:
                    #logging.debug('\t\t\telement: %s' % ( str(size[1]) ) )
        except TypeError:
            logging.error('\t\titem "%s" is neither a sequence nor a mapping!' % (str(size)))
            sys.exit()
##    return deDupeNeeded, sortedSizes
    return sortedSizes

def walkDirAndReturnQueueOfFiles(directoryToWalk):
    queueOfFiles = queue.Queue()
    logging.debug('Walking %s...' % ( str(directoryToWalk) ) )
    for root, dirs, files in os.walk(directoryToWalk):
        '''move to:
            build queue of files -> build pool of processes -> start processes
            like I did in factah over summer, where mp_factorizer is passed nums(a list of numbers to factorize) and a number nprocs (how many processes)

        '''
        for fname in files:
            fullpath = os.path.abspath(os.path.join(root, fname))
            #logging.debug("\tPutting %s in queueOfPathsToFilesToBeHashed"% (str(fullpath)))
            queueOfFiles.put(fullpath)
        queueOfFiles.put(False)
    return queueOfFiles


def walkDirAndReturnListOfFiles(directoryToWalk):
    ListOfFiles = []
    logging.debug('Walking %s...' % ( str(directoryToWalk) ) )
    for root, dirs, files in os.walk(directoryToWalk):
        '''move to:
            build queue of files -> build pool of processes -> start processes
            like I did in factah over summer, where mp_factorizer is passed nums(a list of numbers to factorize) and a number nprocs (how many processes)

        '''
        for fname in files:
            fullpath = os.path.abspath(os.path.join(root, fname))
            #logging.debug("\tPutting %s in queueOfPathsToFilesToBeHashed"% (str(fullpath)))
            ListOfFiles.append(fullpath)
    return ListOfFiles


def main_method(heuristic, algorithm, args):
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
            logging.debug('Found %i files!' % ( len(fileList) ))
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
##            reportData = []
##            for key in fileSizeDict:
##                try:
##                    reportData.append(str(('file of size: %s\n\t%s\n' % (str(key), str(fileSizeDict[key])) )))
##                except UnicodeEncodeError:
##                    logging.warning('Evil file path %s caused UnicodeEncodeError!' % ( str([ord(aChar) for aChar in str(fileSizeDict[key])]) ) )
##                    fileSizeDict[key] = []
                
            sortedSizes = []
            #sortedFileSizes = sorted(fileSizeDict, key=fileSizeDict.__getitem__)
            fileSizeList.sort()
            sortedFileSizes = fileSizeList
            for sortedSize in sortedFileSizes:
                sortedSizes.append([sortedSize, fileSizeDict.get(sortedSize)])
            #print(sortedSizes)
            sortedSizes.sort()
##            for size in sortedSizes:
##                print(size)
##            (deDupeNeeded, sortedSizes)  = removeDuplicatesForHeuristic(sortedSizes)
##            while deDupeNeeded:
##                (deDupeNeeded, sortedSizes) = removeDuplicatesForHeuristic(sortedSizes)
            logging.debug('Sorting a list of %i file sizes!' % (len(sortedSizes)))
            logging.debug('\tdeduplicating that list...')
            sortedSizes  = removeDuplicatesForHeuristic(sortedSizes)
            
            print('Heuristically identified %i possible duplicate files!' % ( sum([ len(size[1]) for size in sortedSizes ]) ) )

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
                            result = aWorker.compute(aPath, incremental = True)
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
                        #(fileFullPath, fileHashHex)  = queueOfFileHashes.get()
                    except KeyboardInterrupt:
                        sys.exit()

            elif heuristic is not None:
                logging.debug('\tComputing with heuristic!')
                try:
                    for aFileNameList in sortedSizes:
                        for thisHashFileName in aFileNameList[1]:
                            #result = aWorker.compute(thisHashFileName, incremental = True)
                            result = aWorker.computeByteArray(thisHashFileName, aFileNameList[0], incremental = True)
                            #maybe pass the size of file into computeByteArray, to then read that size file?
                            fileHashes.append(result)
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
                        #(fileFullPath, fileHashHex)  = queueOfFileHashes.get()
                    except KeyboardInterrupt:
                        sys.exit()
        else:
            raise IOError("Specified file or directory not found!")

        wastedSpace= printDuplicateFilesAndReturnWastedSpace(knownFiles)
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
    stats.sort_stats('time', 'calls')
    stats.print_title()
    stats.print_stats(1000)
    stats.print_callees(1000)
    stats.print_callers(1000)
    os.remove(prof_file)



def main():
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage=usage)
    parser.add_option("--hash", dest="hashname", default="auto", help="select hash algorithm")
    parser.add_option("--heuristic", dest="heuristic", default=None, help="Attempt to hash ONLY files that may be duplicates")
    parser.add_option("--debug", dest="isDebugMode", default=None, help="For the curious ;)")
    parser.add_option('--profile', action='store_true', dest='profile', default=False, help="for the hackers")
    logging.warning("This is a VERY I/O heavy program. You may want to temporairily[TODO: sp?] exclude %s from anti-malware/anti-virus monitoring, especially for Microsoft Security Essentials/Windows Defender. That said, I've never seen Malwarebytes Anti-Malware have a performance impact; leave MBAM as it is." % (str(sys.executable)))
    (options, args) = parser.parse_args()
    
    if options.isDebugMode is not None:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    if options.hashname == "auto":
        options.hashname = "sha1"
        logging.warning("'auto' as hash selected, so defaulting to 'sha1'\n")

    heuristic = options.heuristic
    algorithm = options.hashname

    if options.profile:
        def safe_main():
            try:
                main_method(heuristic, algorithm,args)
            except:
                pass
        _profile(safe_main)
    else:
        main_method(heuristic, algorithm,args)

if __name__ == "__main__":
    main()

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
import re
import math
import hashlib
import multiprocessing
NO_HUMANFRIENDLY = True
try:
    import humanfriendly#pip install humansize
except ImportError:
    NO_HUMANFRIENDLY = None
# TODO : replace optparse with argparse : will brake compatibility with Python < 2.7
from optparse import OptionParser

version = '1.0'


class Worker():
    def __init__(self, hashname, rmode='rb', bufsize=16777216, name=None):#268435456 = 2^ 28#2097152
        self.hash_known = ['md5', 'sha1', 'sha512']
        self.hash_length = { 32:'md5', 40: "sha1", 128: "sha512"}
        self.bufsize = bufsize
        self.hashname = hashname
        self.hashdata = None
        if name is not None:
            self.name = name
        else:
            self.name = "worker-%s" % self.hashname
        #logging.debug("%s started!" % self.name)
    def compute(self, fname, incremental = None):
        #[logging.debug("compute: %s" % ( str(arg))) for arg in args]
        try:
            self.hashdata = hashlib.new(self.hashname)

        except ValueError:
            raise NotImplementedError("# %s : hash algorithm [ %s ] not implemented" % (self.name, self.hashname))
        incremental = True
        try:
            with open(fname, 'rb') as fhandle:
                if incremental is not None:
                    data = fhandle.read(self.bufsize)                
                    while(data):
                        self.hashdata.update(data)
                        data = fhandle.read(self.bufsize)
                else:
                    data = fhandle.read()#this is ugly and will break for large files
                    self.hashdata.update(data)
        except IOError:
            logging.debug("whoops! IOError in Worker.compute")
            return ('','')
        
        #outputHashQueue.put(fname, self.hashdata.hexdigest())
        return (fname, self.hashdata.hexdigest())

def mCompute(job_q, __hashname, out_q, incremental = None, rmode='rb', bufsize=16777216, __name=None):
    __hash_known = ['sha1', 'sha512']
    __hash_length = { 40: "sha1", 128: "sha512"}
    __hashdata = None
    __incremental = True
    hashDatum = []
    __outDict = {}
    logging.debug("worker %s started!" % (str(__name)))
    while True:
        try:
            __hashdata = hashlib.new(__hashname)

        except ValueError:
            raise NotImplementedError("# %s : hash algorithm [ %s ] not implemented" % (__name, __hashname))
        
        try:
            __job = job_q.get_nowait()
            logging.debug("%s got job %s" % (str(__name), str(__job)))
            with open(__job, 'rb') as __fhandle:
                if __incremental is not None:
                    __data = __fhandle.read(bufsize)                
                    while(__data):
                        __hashdata.update(__data)
                        __data = __fhandle.read(bufsize)
                else:
                    __data = __fhandle.read()#this is ugly and will break for large files
                    __hashdata.update(__data)
                __outDict = {__job: __hashdata.hexdigest()}
                out_q.put(_outDict)
            
        except IOError:
            logging.debug("whoops! IOError in Worker.compute")
        except queue.Empty:
            logging.debug('queue.Empty!')
        except NameError:
            logging.debug('NameError!')
            logging.debug(sys.exc_info)
            os.abort()
        #return


def getFileSizeFromOS(theFileInQuestion):
    try:
        aFileStats = str(os.stat(theFileInQuestion))
        listAFileStats = aFileStats[aFileStats.index('(')+1 : aFileStats.index(')')].split(', ')

        for item in listAFileStats:
            listAFileStats[listAFileStats.index(item)] = item.split('=')

        for item in listAFileStats:
            if item[0] == 'st_size':
                sizeIndex = listAFileStats.index(item)
        fileSizeStr = listAFileStats[sizeIndex][1]
        if fileSizeStr[-1] == 'L':
            fileSizeInBytes = fileSizeStr[:-1]
        else:
            fileSizeInBytes = fileSizeStr
        try:
            fileSizeInBytes = int(fileSizeInBytes)
        except ValueError:#occurs when st_size == ''
            logging.debug("Whoops! ValueError in getFileSizeFromOS! This usually occurs when st_size == \'\'")
            logging.debug("\taFileStats       = %s" % aFileStats)
            logging.debug("\tlistAFileStats   = %s" % (str(listAFileStats)))
            logging.debug("\tsizeIndex        = %s" % (str(sizeIndex)))
            logging.debug("\tfileSizeStr      = %s" % fileSizeStr)
            logging.debug("\tfileSizeStr[:-1] = %s" % (str(fileSizeStr[:-1])))
            logging.debug("\tfileSizeInBytes:")
            logging.debug(fileSizeInBytes)
            fileSizeInBytes = 0
    except WindowsError:
        logging.debug("Windows error in printDuplicateFilesAndReturnWastedSpace! May god have mercy on your soul.")
        pass
    return fileSizeInBytes

def printDuplicateFilesAndReturnWastedSpace(knownFiles):
    '''
    prints duplicate files (as of now, only those > 0 bytes) in the form of

    someVeryLongHashOfADuplicateFile:
    numBytes bytes          pathToFile

    e.g.:

    f9f629771f00de09cb63f10a4dbae4d7a8f72544:
    59217687 bytes          C:\\Users\\Alexander Riccio\\Downloads\\testDir\\blah.pdf
    59217687 bytes          C:\\Users\\Alexander Riccio\\Downloads\\testDir\\blahBlah.pdf


    finding the filesize is a bitch. A call to os.stat("pathToSomeFile") returns (on Windows):

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

    
    If you have humanfriendly ("humansize") installed, the output will be more readable - i.e. 1 KB, not 1024.
    I also increment a counter, wastedSpace, by filesize over each file.
    '''
    wastedSpace = 0
    sizeOfKnownFiles = {}
    for key in knownFiles:
        aFile = knownFiles[key][0]
        try:
            aFileStats = str(os.stat(aFile))
            listAFileStats = aFileStats[aFileStats.index('(')+1 : aFileStats.index(')')].split(', ')

            for item in listAFileStats:
                listAFileStats[listAFileStats.index(item)] = item.split('=')

            for item in listAFileStats:
                if item[0] == 'st_size':
                    sizeIndex = listAFileStats.index(item)
            fileSizeStr = listAFileStats[sizeIndex][1]
            if fileSizeStr[-1] == 'L':
                fileSizeInBytes = fileSizeStr[:-1]
            else:
                fileSizeInBytes = fileSizeStr
            try:
                fileSizeInBytes = int(fileSizeInBytes)
            except ValueError:#occurs when st_size == ''
                logging.debug("Whoops! ValueError in printDuplicateFilesAndReturnWastedSpace! This usually occurs when st_size == \'\'")
                logging.debug("\taFileStats       = %s" % aFileStats)
                logging.debug("\tlistAFileStats   = %s" % (str(listAFileStats)))
                logging.debug("\tsizeIndex        = %s" % (str(sizeIndex)))
                logging.debug("\tfileSizeStr      = %s" % fileSizeStr)
                logging.debug("\tfileSizeStr[:-1] = %s" % (str(fileSizeStr[:-1])))
                logging.debug("\tfileSizeInBytes:")
                logging.debug(fileSizeInBytes)
                fileSizeInBytes = 0
                
            if len(knownFiles[key]) > 1:
                wastedSpace += fileSizeInBytes * len(knownFiles[key])
                if fileSizeInBytes > 0:
                    sizeOfKnownFiles[key] = fileSizeInBytes * len(knownFiles[key])
                    logging.debug("\n%s:"%key)
                    for aSingleFile in knownFiles[key]:
                        if NO_HUMANFRIENDLY is None:
                            logging.debug("%s bytes\t\t%s" % (fileSizeInBytes, aSingleFile))
                        elif NO_HUMANFRIENDLY is not None:
                            logging.debug("%s\t\t%s" % (humanfriendly.format_size(fileSizeInBytes), aSingleFile))
        except WindowsError:
            logging.debug("Windows error in printDuplicateFilesAndReturnWastedSpace! May god have mercy on your soul.")
            pass
    sortedSizeOfKnownFiles = sorted(sizeOfKnownFiles, key=knownFiles.__getitem__)
    sortedListSize = []
    for sortedHash in sortedSizeOfKnownFiles:
        sortedListSize.append([sizeOfKnownFiles.get(sortedHash), knownFiles.get(sortedHash)])

    sortedListSize.sort()
    if NO_HUMANFRIENDLY is not None:
        for item in sortedListSize:
            item[0] = humanfriendly.format_size(item[0])
    for item in sortedListSize:
        print("\n%s:" % (str(item[0])))
        for aFileName in item[1]:
            try:
                print("\t%s" % (str(aFileName)))    
            except UnicodeEncodeError:
                logging.debug("filename is evil! filename: ", aFileName)
                
    
    return wastedSpace#, sortedSizeOfKnownFiles

def fInit(q):
    '''eeeewwwwww this is disgusting
    http://stackoverflow.com/questions/3827065/can-i-use-a-multiprocessing-queue-in-a-function-called-by-pool-imap
    '''
    Worker.compute.outputHashQueue = q

def main():
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage=usage)
    parser.add_option("--hash", dest="hashname", default="auto", help="select hash algorithm")
    (options, args) = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG)
    #logging.basicConfig(level=logging.WARNING)
    if args:
        arg0 = args[0]
        knownFiles = {}
        if True:
            if options.hashname == "auto":
                options.hashname = "sha1"
                logging.warning("'auto' as hash selected, so defaulting to 'sha1'")

            w = Worker(options.hashname)

            if os.path.isfile(arg0):
                hw = w.compute(arg0)
                print("%s *%s" % (hw, arg0))
                knownFiles[hw] = arg0

            elif os.path.isdir(arg0):
                pathsToFilesToBeHashed = []
                for root, dirs, files in os.walk(arg0):
                    '''move to:
                        build queue of files -> build pool of processes -> start processes
                        like I did in factah over summer, where mp_factorizer is passed nums(a list of numbers to factorize) and a number nprocs (how many processes)

                    '''
                    
                    #queueOfFileHashes      = multiprocessing.Queue()
                job_q = multiprocessing.Queue()    
                for fname in files:
                    
                    fullpath = os.path.abspath(os.path.join(root, fname))
                    #logging.debug("putting %s in queueOfPathsToFilesToBeHashed"% (str(fullpath))) 
                    job_q.put(fullpath)
                    
                    #hw = w.compute(fullpath)
                fileHashes             = []
                # Each process will get 'chunksize' nums and a queue to put his out dict into
                out_q = multiprocessing.Queue()
                #worker(nums, out_q)
                procs = []
                nProcesses = 8
                
                for i in range(nProcesses):
                    logging.debug("starting %i...\tprocs = %s" % (i, str(procs)))
                    p = multiprocessing.Process( target=mCompute, args=(job_q, 'sha1', out_q, i))
                    #fname, hashname, incremental = None, rmode='rb', bufsize=16777216, name=None
                    procs.append(p)
                    p.start()
                logging.debug('All started! Waiting.....')

                for p in procs:
                    p.join()
                nextHash = out_q.get()
                while(nextHash):
                    fileHashes.append(nextHash)
                    nextHash = out_q.get()
                try:
                    for (fileFullPath, fileHashHex) in fileHashes:
                        
                        it = knownFiles.get(fileHashHex)
                        if it is None:
                            knownFiles[fileHashHex] = [fileFullPath]
                        else:
                            knownFiles[fileHashHex].append(fileFullPath)
                        #(fileFullPath, fileHashHex)  = queueOfFileHashes.get()
                except ValueError:
                    logging.debug(fileFullPath, fileHashHex)
            else:
                raise IOError("Specified file or directory not found")
        wastedSpace= printDuplicateFilesAndReturnWastedSpace(knownFiles)
        print('\n\n')
        if NO_HUMANFRIENDLY is None:
            print("%s bytes of wasted space!" % wastedSpace)
        elif NO_HUMANFRIENDLY is not None:
            print("%s of wasted space!" % humanfriendly.format_size(wastedSpace))           
        #print(sortedSizeOfKnownFiles)
if __name__ == "__main__":
    main()

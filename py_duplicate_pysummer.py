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
        self.hash_known  = ['md5', 'sha1', 'sha512']
        self.hash_length = { 32:'md5', 40: "sha1", 128: "sha512"}
        self.__bufsize     = bufsize
        self.__hashname    = hashname
        self.__hashdata    = None
        self.__rmode       = rmode
        if name is not None:
            self.__name = name
        else:
            self.__name = "worker-%s" % self.__hashname

    def compute(self, fname, incremental = None):
        self.__fname = fname
        try:
            self.__hashdata = hashlib.new(self.__hashname)
        except ValueError:
            raise NotImplementedError("# %s : hash algorithm [ %s ] not implemented" % (self.__name, self.__hashname))
        #incremental = True
        except KeyboardInterrupt:
            sys.exit()
        try:

            logging.debug('\tcompute opening %s' % ( str(self.__fname) ))
            with open(self.__fname, self.__rmode) as fhandle:
                if incremental is not None:
                    if NO_HUMANFRIENDLY is None:
                        logging.debug('\t\treading incrementally... (increments of: %s bytes)' % ( str(self.__bufsize) ) )
                    elif NO_HUMANFRIENDLY is not None:
                        logging.debug('\t\treading incrementally... (increments of: %s)' % ( humanfriendly.format_size(self.__bufsize) ) )

                    data = fhandle.read(self.__bufsize)                
                    while(data):
                        self.__hashdata.update(data)
                        data = fhandle.read(self.__bufsize)
                else:
                    logging.debug('\t\treading in one huge chunk...')
                    data = fhandle.read()#this is ugly and will break for large files
                    self.__hashdata.update(data)
        except IOError:
            logging.debug("whoops! IOError in Worker.compute")
        
        except KeyboardInterrupt:
            sys.exit()

        logging.debug('\t\t\t\tfile: "%s" : %s' % (str(self.__fname), str(self.__hashdata.hexdigest())) )
        return (self.__fname, self.__hashdata.hexdigest())
        
        

def getFileSizeFromOS(theFileInQuestion):
    '''
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

    '''
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

def printListOfDuplicateFiles(listOfDuplicateFiles):
    if NO_HUMANFRIENDLY is not None:
        for item in listOfDuplicateFiles:
            item[0] = humanfriendly.format_size(item[0])

    for item in listOfDuplicateFiles:
        print("\n%s:" % (str(item[0])))
        for aFileName in item[1]:
            try:
                print("\t%s" % (str(aFileName)))    
            except UnicodeEncodeError:
                logging.debug("\t\t\tfilename is evil! filename: ", aFileName)


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

                for aSingleFile in knownFiles[key]:

                    if NO_HUMANFRIENDLY is None:
                        logging.debug("\t\t\t%s bytes\t\t\t%s" % (fileSizeInBytes, aSingleFile))
                    elif NO_HUMANFRIENDLY is not None:
                        logging.debug("\t\t\t%s\t\t\t%s" % (humanfriendly.format_size(fileSizeInBytes), aSingleFile))
            
    sortedSizeOfKnownFiles = sorted(sizeOfKnownFiles, key=knownFiles.__getitem__)
    sortedListSize = []
    
    for sortedHash in sortedSizeOfKnownFiles:
        sortedListSize.append([sizeOfKnownFiles.get(sortedHash), knownFiles.get(sortedHash)])

    sortedListSize.sort()

    printListOfDuplicateFiles(sortedListSize)

    return wastedSpace


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
            logging.debug("\tPutting %s in queueOfPathsToFilesToBeHashed"% (str(fullpath))) 
            queueOfFiles.put(fullpath)
        queueOfFiles.put(False)
    return queueOfFiles

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
        if options.hashname == "auto":
            options.hashname = "sha1"
            logging.warning("'auto' as hash selected, so defaulting to 'sha1'\n")

        w = Worker(options.hashname)

        if os.path.isfile(arg0):
            hw = w.compute(arg0)
            print("%s *%s" % (hw, arg0))
            knownFiles[hw] = arg0

        elif os.path.isdir(arg0):

            job_q = walkDirAndReturnQueueOfFiles(arg0)                
            fileHashes = []
            out_q = queue.Queue()
            
            aWorker = Worker(options.hashname)
            logging.debug('Starting computation!')
            try:
                thisHashFileName = job_q.get()
                while thisHashFileName:
                    result = aWorker.compute(thisHashFileName, incremental = True)
                    out_q.put(result)
                    thisHashFileName = job_q.get()
                out_q.put(False)                
                #fileHashes = []

                nextHash = out_q.get()
                while(nextHash):
                    fileHashes.append(nextHash)
                    nextHash = out_q.get()

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
            raise IOError("Specified file or directory not found")

        wastedSpace= printDuplicateFilesAndReturnWastedSpace(knownFiles)
        print('\n')
        if NO_HUMANFRIENDLY is None:
            print("%s bytes of wasted space!" % wastedSpace)
        elif NO_HUMANFRIENDLY is not None:
            print("%s of wasted space!" % humanfriendly.format_size(wastedSpace))           



if __name__ == "__main__":
    main()

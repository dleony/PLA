#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import os, glob, sys, re, logging, getopt, locale, tarfile, time

# Directory in user HOME containing the instrumented commands
plaDirectory = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), \
                                                    '..'))
def findPLASVNDataDir(startDir = os.getcwd()):
    """
    Starging in the given a directory, search for a folder with name .pladata
    traversing the folders toward the root of the filesystem.

    This function is to be executed when the user runs a "svn commit" command
    from some location in a svn repository with .pladata at its root.
    """
    index = 0 # To limit the number of levels searched
    startDir = os.path.abspath(startDir) # Manage absolute paths

    # While levels remaining, the dir exists and there is no .pladata loop
    while (index < 20) and \
            os.path.exists(startDir) and \
            not os.path.exists(os.path.join(startDir, '.pladata')):
        startDir = os.path.abspath(os.path.join(startDir, '..'))
        index = index + 1
        
    # If .pladata was found, return it
    result = os.path.join(startDir, '.pladata')
    if os.path.exists(result):
        return os.path.abspath(result)

    # Nothing found
    return None

def createTarFile(fileList, toFile):
    """
    Creates a tar-gzip-compressed file with name as the second parameter and
    containing the files given as first parameter.
    """

    logMessage('createTarFile: Creating TGZ file ' + toFile)

    # If an empty list is given, forget about it 
    if fileList == []:
        logMessage("createTarFile: empty file list.")
        return

    # If the destination directory does not exist, forget about it
    if not os.path.exists(os.path.dirname(toFile)):
        logMessage("createTarFile: " + os.path.dirname(toFile) + \
                       ' not present.')
        return

    # Create the tar file
    tarFile = tarfile.open(name = toFile, mode = 'w:gz')

    # Loop over all the files and include them in the tar
    map(tarFile.add, fileList)
    tarFile.close()

    return

def instrument(dataDir, dataFile, prefix):

    # If no file is present in pladirectory, nothing to return
    if not os.path.exists(dataDir):
        PLABasic.logMessage(prefix + ': Disabled. Skipping')
        return []

    # If the file does not exist or it is empty, done
    if not os.path.exists(dataFile) or os.path.getsize(dataFile) == 0:
        return []

    return [dataFile]

def resetData(fileDir, fileName, prefix):
    """
    Given a directory and a file inside it, leave it at zero byte content. The
    prefix is used to print log messages.
    """
    
    # If the directory is not present, used disabled the monitoring
    if not os.path.exists(fileDir):
        logMessage(prefix + ': Disabled. Skipping')
        return

    # If the file does not exist or its size is zero, terminate
    if (not os.path.exists(fileName)) or (os.path.getsize(fileName) == 0):
        return

    # Open/close the file in write mode to reset its content
    logMessage(prefix + ': Removing ' + fileName)
    fobj = open(fileName, 'w')
    fobj.close()

def getUniqueFileName():
    """
    Return a unique file name taking the microseconds from the system.
    """
    return str(int(time.time() * 1000))


def logMessage(msg):
    global plaDirectory
    """
    Logging facility, it checks if there is a folder 'test' in the
    plaDirectory. If so, go ahead and log. If not, ignore the message. The test
    directory is supposed to be removed when deployed in the user machine.
    """

    if plaDirectory != None and \
            os.path.exists(os.path.join(plaDirectory, 'test')):
        print 'pla-' + msg

                          
def dumpException(e):
    """
    When an exception appears, its appearance in the screen should be subject to
    the log policy.
    """
    # Exception when updating, not much we can do, log a message if in
    # debug, and terminate.
    logMessage('----- SVN EXCEPTION ---- ')
    logMessage(str(e))

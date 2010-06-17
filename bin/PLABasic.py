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

def getUniqueFileName():
    return str(int(time.time() * 1000))


def logMessage(msg):
    if plaDirectory != None and os.path.exists(os.path.join(plaDirectory, 'test')):
        print 'pla: ' + msg

                          
def dumpException(e):
    # Exception when updating, not much we can do, log a message if in
    # debug, and terminate.
    logMessage('----- SVN EXCEPTION ---- ')
    logMessage(str(e))

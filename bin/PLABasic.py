#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import os, glob, sys, re, logging, getopt, locale, gzip, time

# Directory in user HOME containing the instrumented commands
plaDirectory = None

# Directory in the local SVN repository to store the information
localPLADirectory = None

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
    if os.path.exists(os.path.join(startDir, '.pladata')):
        return os.path.abspath(startDir)

    # Nothing found
    return None

def gzipFile(fromFile, toFile):

    if not os.path.exists(fromFile):
        return

    logMessage('Zipping ' + fromFile + ' into ' + toFile)

    if not os.path.exists(os.path.dirname(toFile)):
        return

    f_in = open(fromFile, 'rb')
    f_out = gzip.open(toFile, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()

    return

def getUniqueFileName():
    return str(int(time.time() * 1000))


def logMessage(msg):
    if plaDirectory != None and os.path.exists(os.path.join(plaDirectory, 'test')):
        print '[DBG] ' + msg

                          
def dumpException(e):
    # Exception when updating, not much we can do, log a message if in
    # debug, and terminate.
    logMessage('----- SVN EXCEPTION ---- ' + str(e))
    logMessage(e.args[0])
    for message, code in e.args[1]:
        logMessage('Code: ' + str(code) + ' Message: ' + str(message))

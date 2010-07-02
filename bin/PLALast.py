#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import os, datetime, subprocess, shutil

import PLABasic

dataDir = os.path.join(PLABasic.plaDirectory, 'tools', 'last')
dataFile = os.path.expanduser('~/.lastrc')
logPrefix = 'last'


def prepareDataFile(suffix):
    """ 
    Function that prepares a data file with the information of the last
    logins. It checks first if it is allowed to send this data, if the file is
    there and is not empty. If so, creates a duplicate adding the suffix and
    returns that file name.
    """

    global dataDir
    global dataFile
    global logPrefix

    # Log the execution of this function
    PLABasic.logMessage(logPrefix + ': prepare ' + dataFile)

    # If no file is present in pladirectory, nothing to return
    if not os.path.exists(dataDir):
        PLABasic.logMessage(logPrefix + ': Disabled. Skipping')
        return []

    # Prepare the file to catch the output of the last command
    toSendFileName = dataFile + '_' + suffix
    dataOut = open(toSendFileName, 'w')

    # Execute the last command and store its output
    try:
        command = ['/usr/bin/ĺast', '-F']
        PLABasic.logMessage(logPrefix + ': executing ' + ' '.join(command))
        givenCmd = subprocess.Popen(command, executable = '/usr/bin/last', \
                                        stdout = dataOut)
    except OSError, e:		  
        print 'File not found (PLA)'
        return []
    except ValueError, e:
        print 'Incorrect arguments (PLA)'
        return []

    # Wait for the process to terminate and get the output
    givenCmd.wait()

    # Close the data
    dataOut.close()

    # If the file is empty, done
    if os.path.getsize(toSendFileName) == 0:
        PLABasic.logMessage(logPrefix + ': No data to send')
        return []


    # Return the new file 
    return [toSendFileName]

def main(): 
    """
    Script to simply return the history file and reset its content
    """
    print prepareDataFile('bogus')

if __name__ == "__main__":
    main()

#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import os, datetime, subprocess, shutil

import PLABasic

dataDir = os.path.join(PLABasic.plaDirectory, 'tools', 'last')
dataFile = os.path.expanduser('~/.lastrc')
lastOutput = ''

def prepareDataFile(suffix):
    """ 
    Function that prepares a data file with the information of the last
    logins. It checks first if it is allowed to send this data, if the file is
    there and is not empty. If so, creates a duplicate adding the suffix and
    returns that file name.
    """

    global dataDir
    global dataFile
    global lastOutput

    logPrefix = 'last'

    # Log the execution of this function
    PLABasic.logMessage(logPrefix + ': prepare ' + dataFile)

    # If no file is present in pladirectory, nothing to return
    if not os.path.exists(dataDir):
        PLABasic.logMessage(logPrefix + ': Disabled. Skipping')
        return []

    # Execute the last command and store its output
    try:
        PLABasic.logMessage(logPrefix + ': executing /usr/bin/last')
        givenCmd = subprocess.Popen(['/usr/bin/last'], stdout = subprocess.PIPE)
    except OSError, e:		  
        print 'File not found (PLA)'
        return []
    except ValueError, e:
        print 'Incorrect arguments (PLA)'
        return []

    # Wait for the process to terminate and get the output
    (lastOutput, lastError) = givenCmd.communicate()

    # Create a duplicate of the data file with the suffix
    toSendFileName = dataFile + '_' + suffix

    # If the data file exist, create tmp file with the diff
    if os.path.exists(dataFile):
        try:
            PLABasic.logMessage(logPrefix + ': executing /usr/bin/diff')
            givenCmd = subprocess.Popen(['/usr/bin/diff', dataFile, '-'], \
                                            stdin = subprocess.PIPE,
                                            stdout = subprocess.PIPE)
        except OSError, e:		  
            print 'File not found (PLA)'
            return []
        except ValueError, e:
            print 'Incorrect arguments (PLA)'
            return []
        
        # Wait for the process to terminate and get the output
        (theDiff, lastError) = givenCmd.communicate(lastOutput)

    # Dump the difference as the data file to send
    dataOut = open(toSendFileName, 'w')
    dataOut.write(theDiff)
    dataOut.close()

    # If the file is empty, done
    if os.path.getsize(toSendFileName) == 0:
        PLABasic.logMessage(logPrefix + ': No data to send')
        return []


    # Return the new file 
    return [toSendFileName]

def resetData():
    """
    The execution of the command "last" needs to be stored in the dataFile.
    """
    # Store the output of last in the data file as future reference
    PLABasic.logMessage(logPrefix + ': creating ' + dataFile)
    dataOut = open(dataFile, 'w')
    dataOut.write(lastOutput)
    dataOut.close()

def main(): 
    """
    Script to simply return the history file and reset its content
    """
    print prepareDataFile('bogus')

if __name__ == "__main__":
    main()

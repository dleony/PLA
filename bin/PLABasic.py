#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import os, sys, tarfile, time, datetime, subprocess, shutil, ramdom

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

def executeAndLogExecution(dataDir, dataFile, prefix): 
    """
    Application to wrap the execution of whatever is in sys.argv and dump a file
    noting its execution in the dataFile.

    Returns the command status obtained from its execution
    """
    
    dateBegin = str(datetime.datetime.now())[:-7]
    # Execute the given command normally
    try:
        logMessage(prefix + ': executing ' + str(sys.argv))
        givenCmd = subprocess.Popen(sys.argv)
    except OSError, e:		  
        print 'File not found (PLA)'
        return 0
    except ValueError, e:
        print 'Incorrect arguments (PLA)'
        return 0
        
    # Wait for the process to terminate
    givenCmd.wait()

    # Store the return status to return when the script finishes.
    originalStatus = givenCmd.returncode
    logMessage(prefix + ': command status = ' + str(originalStatus))

    # If no file is present in pladirectory, no instrumentation
    if not os.path.exists(dataDir):
        logMessage(prefix + ': Disabled. Skipping')
        return originalStatus

    # Append the captured messages to the file with a separator
    dataOut = open(dataFile, 'a')

    # Dump a mark status and time/date
    dataOut.write(str(originalStatus) + ' ' \
                      + dateBegin + ' ' \
                      + str(datetime.datetime.now())[:-7] + ' ' \
                      + ' '.join(map(lambda x: '\'' + x + '\'', sys.argv)) \
                      + '\n')
    dataOut.close()

    return originalStatus

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

def prepareDataFile(dataDir, dataFile, logPrefix, suffix):
    """ 
    Function that prepares the given data file to be included in the tar. It
    checks first if it is allowed to send this data, if the file is there and is
    not empty. If so, creates a duplicate adding the suffix and returns that
    file name.
    """

    # Log the execution of this function
    logMessage(logPrefix + ': prepare ' + dataFile)

    # If no file is present in pladirectory, nothing to return
    if not os.path.exists(dataDir):
        logMessage(logPrefix + ': Disabled. Skipping')
        return []

    # If the file does not exist, done
    if not os.path.exists(dataFile):
        logMessage(logPrefix + ': ' + dataFile + ' not present')
        return []

    # If the file is empty, done
    if os.path.getsize(dataFile) == 0:
        logMessage(logPrefix + ': Empty data file')
        return []

    # Create a duplicate of the data file with the suffix
    duplicateFileName = dataFile + '_' + suffix
    try:
        shutil.copyfile(dataFile, duplicateFileName)
    except IOError, e:
        # If something went wrong, ignore this file
        logMessage(logPrefix + ': IOError when creating duplicate' \
                       + duplicatedFileName)
        dumpException(e)
        return []

    # Return the new file 
    return [duplicateFileName]

def resetData(fileDir, fileName, logPrefix, suffix):
    """
    Given a directory and a file inside it, removes the file adding the suffix
    (no longer needed) and leaves the file at zero byte content. The logPrefix
    is used to print log messages.
    """
    
    # Log the execution of this function
    logMessage(logPrefix + ': resetData ' + fileName)
    
    # If the directory is not present, used disabled the monitoring
    if not os.path.exists(fileDir):
        logMessage(logPrefix + ': Disabled. Skipping')
        return

    # If the file does not exist or its size is zero, terminate
    if (not os.path.exists(fileName)) or (os.path.getsize(fileName) == 0):
        return

    # Remove the duplicated file with the suffix
    tmpFileName = fileName + '_' + suffix
    try:
        logMessage(logPrefix + ': Removing ' + tmpFileName)
        os.remove(tmpFileName)
    except OSError, e:
        # If something went wrong, log
        logMessage(logPrefix + ': OSError when removing ' + tmpFileName)

    # Open/close the file in write mode to reset its content
    logMessage(logPrefix + ': Resetting ' + fileName)
    fobj = open(fileName, 'w')
    fobj.close()

def getUniqueFileName():
    """
    Return a unique file name taking the microseconds from the system and adding
    five random digits. If two users in the different virtual machines create a
    file at the same time, they will have the same temporary file and it will
    collide in the repository. By adding five extra irrelevant digits to the
    file name the chances of collisions are reduced to appropriate levels.
    """
    # Get disk occupation as seed for pseudo random number generator
    s = os.statvfs('/')
    random.seed((s.f_bavail * s.f_frsize) / 1024)

    return str(int(time.time() * 1000)) + '_' + \
        ''.join([str(random.randint(0, 9)) for n in range(0, 5)])

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

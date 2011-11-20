#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Copyright (C) 2010 Carlos III University of Madrid
# This file is part of the Adagio: Agile Distributed Authoring Toolkit

# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor
# Boston, MA  02110-1301, USA.
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import os, sys, tarfile, time, datetime, subprocess, shutil, random, re, pysvn

__all__ = ['bash', 'firefox', 'last', 'iwatch']

# Directory in user HOME containing the instrumented commands
plaDirectory = os.path.expanduser('~/.pladata')
stampFileName = os.path.join(plaDirectory, 'tools', '.lastexecution')

def findPLASVNDataDir(svnClient, startDir = os.getcwd()):
    """
    Starging in the given a directory, search for a folder with name .pladata
    traversing the folders toward the root of the filesystem.

    This function is to be executed when the user runs a "svn commit" command
    from some location in a svn repository with .pladata at its root.
    """
    index = 0 # To limit the number of levels searched
    startDir = os.path.abspath(startDir) # Manage absolute paths

    # While levels remaining, the dir exists and there is no .pladata loop
    found = False
    while (index < 20) and startDir != '/':
        try:
            # Ask for the url of directory/.pladata
            root_url = svnClient.root_url_from_path(os.path.join(startDir, 
                                                                 '.pladata'))
            # If successfully obtained, we are done
            found = True
            break 
        except pysvn.ClientError, e:
            # If an exception pops up, the dir does not exist or not under SVN,
            # keep iterating.
            pass

        startDir = os.path.abspath(os.path.join(startDir, '..'))
        index = index + 1

    # If .pladata was found, return it
    result = os.path.join(startDir, '.pladata')
    if found:
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
                       + duplicateFileName)
        dumpException(e)
        return []

    # Return the new file 
    return [duplicateFileName]

def removeTemporaryData(fileDir, fileName, logPrefix, suffix):
    """
    Given a directory and a file inside it, removes the temporary file created
    """
    
    # Name of the temporary file
    tmpFileName = fileName + '_' + suffix

    # Log the execution of this function
    logMessage(logPrefix + ': removeTemporaryData ' + tmpFileName)
    
    # If the directory is not present, used disabled the monitoring
    if not os.path.exists(fileDir):
        logMessage(logPrefix + ': Disabled. Skipping')
        return

    # If the file does not exist or its size is zero, terminate
    if not os.path.exists(tmpFileName):
        return

    # Remove the duplicated file with the suffix
    try:
        logMessage(logPrefix + ': Removing ' + tmpFileName)
        os.remove(tmpFileName)
    except OSError, e:
        # If something went wrong, log
        logMessage(logPrefix + ': OSError when removing ' + tmpFileName)

def resetData(fileDir, fileName, logPrefix):
    """
    Given a directory and a file inside it, resets the file leaving its content
    at zero bytes. The logPrefix is used to print log messages.
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
    """
    Logging facility, it checks if there is a folder 'test' in the
    plaDirectory. If so, go ahead and log. If not, ignore the message. The test
    directory is supposed to be removed when deployed in the user machine.
    """

    global plaDirectory

    if plaDirectory != None and \
            os.path.exists(os.path.join(plaDirectory, 'test')):
        print 'pla-' + msg

def setLastExecutionTStamp():
    """
    Return the last time data were collected. It is based on a file the time of
    which is the last time data was successfully logged.
    """
    global stampFileName

    if os.path.exists(stampFileName):
        os.utime(stampFileName, None)
    else:
        open(stampFileName, 'w').close()
    
def getLastExecutionTStamp():
    """
    Return the mtime of the file left as a mark for the last execution.
    """
    global stampFileName
    
    if not os.path.exists(stampFileName):
        return None

    return os.stat(stampFileName).st_mtime

def synchronizeFolders(source, destination, exclude = [], dryRun = False):
    """
    Function to synchronize two folders. The idea is that after the execution of
    this function, folder destination has exactly the same content as folder
    "source". The procedure works with two sets of files (recursively
    obtained). The files in the intersection are kept, the files in the set
    source - destination are copied and the files in destination - source are
    deleted.
    
    The exclude list contains a set of strings coding regular expressions to
    match against the file name. When a match is obtained, the file is skipped
    from processing.

    The dryRun parameter is to execute the function but disabling all file
    operations, just print the log messages.
    """

    if not os.path.isdir(source):
        logMessage('synchronizeFolder: ' + source + ' not found.')
        return

    if not os.path.isdir(destination):
        logMessage('synchronizeFolder: ' + destination + ' not found.')
        return

    # Obtain the list of all files and directories in both locations
    sourceSet = set(sum([l for l in 
                         [map(lambda x: os.path.join(db.replace(source, '.', 1),
                                                     x), dn + fn) 
                          for (db, dn, fn) in os.walk(source)]], []))
    
    destSet = set(sum([l for l in 
                       [map(lambda x: os.path.join(db.replace(destination, '.', 
                                                              1), x), dn + fn) 
                        for (db, dn, fn) in os.walk(destination)]], []))
    

    # Copy files/dirs in source and not in destination (sort to process files
    # first)
    for name in sorted(sourceSet - destSet, reverse = True):
        srcName = os.path.join(source, name)
        dstName = os.path.join(destination, name)

        # Check if file/dir matches any of the excluded
        if next((re.match(pattern, srcName) for pattern in exclude), None):
            logMessage('EXCLUDED: ' + srcName)
            continue

        # Processing a directory
        if os.path.isdir(srcName):
            # If the dir exists, copy only metadata
            if os.path.exists(dstName):
                logMessage('METACOPY: ' + srcName)
                if not dryRun:
                    shutil.copystat(srcName, dstName)
            # Else, create the dir
            else:
                logMessage('MKDIR: ' + dstName)
                if not dryRun:
                    os.makedirs(dstName)
            continue
        
        # Processing a regular file

        # Obtain the directory prefix of the file to create it if needed
        dirName = os.path.dirname(name)

        # Check if the dst directory exists
        if os.path.dirname(name) != '' and not os.path.exists(dirName):
            logMessage('MKDIR: ' + os.path.join(destination, dirName))
            if not dryRun:
                os.makedirs(os.path.join(destination, dirName))
        # Copy the file
        logMessage('COPY: ' + srcName + ' to ' + dstName)
        if not dryRun:
            shutil.copy2(srcName, dstName)

    # Delete files/dirs in destination not in source
    for name in destSet - sourceSet:
        rmName = os.path.join(destination, name)

        # Check if file/dir matches any of the excluded
        if next((re.match(pattern, rmName) for pattern in exclude), None):
            logMessage('EXCLUDED: ' + rmName)
            continue

        # If a dir, use the recursive delete function
        if os.path.isdir(rmName):
            logMessage('RMDIR: ' + rmName)
            if not dryRun:
                shutil.rmtree(rmName, ignore_errors = True)
        else:
            logMessage('REMOVE: ' + rmName)
            if not dryRun:
                os.remove(rmName)

    # Those files in both sets need to be checked
    for name in destSet & sourceSet:
        srcName = os.path.join(source, name)
        dstName = os.path.join(destination, name)

        # Check if file/dir matches any of the excluded
        if next((re.match(pattern, srcName) for pattern in exclude), None):
            logMessage('EXCLUDED: ' + srcName)
            continue

        # If both files/dirs have identical stats (uid, gid, length, ctime and
        # mtime), skip the operation
        if os.stat(srcName)[4:-1] == os.stat(dstName)[4:-1]:
            logMessage('SKIP: ' + srcName)
            continue

        # If it is a directory, copy only the metadata
        if os.path.isdir(srcName):
            logMessage('METACOPY: ' + srcName)
            if not dryRun:
                shutil.copystat(srcName, dstName)
            continue

        # If it is a file, simply copy the content
        logMessage('COPY: ' + srcName + ' to ' + dstName)
        if not dryRun:
            shutil.copy2(srcName, dstName)
        
def dumpException(e):
    """
    When an exception appears, its appearance in the screen should be subject to
    the log policy.
    """
    # Exception when updating, not much we can do, log a message if in
    # debug, and terminate.
    logMessage('----- SVN EXCEPTION ---- ')
    logMessage(str(e))

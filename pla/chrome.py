#!/usr/bin/python
# -*- coding: utf-8 -*-#
#
# Author: Derick Leony (dleony@it.uc3m.es)
#
import os, sys, datetime, ConfigParser, sqlite3, shutil, subprocess

sys.path.insert(0, 
                os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pla

dataDir = os.path.join(pla.plaDirectory, 'tools', 'chrome')
dataFile = os.path.join(dataDir, 'chrome')

_tmpFile = os.path.join('/tmp', 'History')

def prepareDataFile(suffix): 
    """
    Application to wrap the history of the Google Chrome browser. This
    script should be executed with each svn commit.
    """
    
    global dataDir
    global dataFile
    global _tmpFile

    epoch_offset = 11644473600000000
    pla.logMessage('chrome: prepare ' + dataFile)

    # If no file is present in pladirectory, no instrumentation
    if not os.path.exists(dataDir):
        pla.logMessage('chrome: Disabled. Skipping')
        return []

    # Copy the Google Chrome history database to the tmp directory, in order to
    # avoid lock issues.
    gchromeDir = os.path.expanduser('~/.config/google-chrome/Default')
    sqliteFile = os.path.join(gchromeDir, 'History')
    pla.logMessage('chrome: copy file ' + sqliteFile + ' to ' + _tmpFile)
    shutil.copyfile(sqliteFile, _tmpFile)
        
    # Get the timestamp for the last execution
    lastExecution = pla.getLastExecutionTStamp()
    pla.logMessage('Last execution: ' + str(lastExecution))

    date_clause = ''
    if lastExecution != None:
        date_clause = "AND (v.visit_time - " + str(epoch_offset) + ") > " + \
            str(int(lastExecution * 1000000))
        
    # Get the last activity from Google Chrome, through a query to the
    # history table
    conn = sqlite3.connect(_tmpFile)
    conn.row_factory = sqlite3.Row
    
    c = conn.cursor()
    query = """
          SELECT u.url, 
          DATETIME(CAST ((v.visit_time - """ + str(epoch_offset) + \
        """)/1000000.0 AS INTEGER), 'unixepoch', 'localtime') AS timestamp
          FROM   urls u, visits v
          WHERE  v.url = u.id
          """ + date_clause + " ORDER  BY v.visit_time "
    pla.logMessage('chrome: Query = ' + query)

    # Create a duplicate of the data file with the suffix
    toSendFileName = dataFile + '_' + suffix

    # Dump the data. Detect empty data because "rowcount" seems broken.
    dataOut = open(toSendFileName, 'w')
 
    # Boolean to capture if an alternative executable is needed
    use_executable = False
    try:
        # Try to execute the query
        c.execute(query)

        for row in c:
            dataOut.write(row['timestamp'] + ' ' + row['url'] + '\n')

        # Close the statement and the data file
        c.close()

        
    except sqlite3.DatabaseError, e:
        # Failed, this means the version of the sqlite executable is not
        # correct, use an alternative.
        pla.logMessage('Unable to read Chrome history with sqlite library')
        pla.logMessage('Resorting to included binary')
        use_executable = True

   # See if a seccond attempt to get the data using the executable is needed
    if use_executable:
        executable = os.path.join(pla.plaDirectory, 'bin', 'sqlite3')
        command =  [executable, _tmpFile, query]
        pla.logMessage('Executing ' + ' '.join(command))
        pla.logMessage('Output to ' + toSendFileName)
        try:
            givenCmd = subprocess.Popen(command, executable = executable,
                                        stdout = dataOut)
        except OSError, e:		  
            print 'Executable not found (OSError in PLA)', e
            return []
        except ValueError, e:
            print 'Incorrect arguments (PLA)', e
            return []
        
        # Wait for the process to terminate and get the output
        givenCmd.wait()

    # Close the data file
    dataOut.close()

    # Remove the profile copy form the tmp directory
    os.remove(_tmpFile)
    if os.path.getsize(toSendFileName) == 0:
        pla.logMessage('chrome empty data file detected. Removing')
        os.remove(toSendFileName)
        return []

    return [toSendFileName]

def main():
    """
    Script to store the history of chrome
    """
    
    prepareDataFile('bogus')

if __name__ == "__main__":
    main()

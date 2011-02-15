#!/usr/bin/python
# -*- coding: utf-8 -*-#
#
# Author: Derick Leony (dleony@it.uc3m.es)
#
import os, sys, datetime, ConfigParser, sqlite3, shutil

sys.path.insert(0, 
                os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pla

dataDir = os.path.join(pla.plaDirectory, 'tools', 'firefox')
dataFile = os.path.join(dataDir, 'firefox')

_tmpFile = os.path.join('/tmp', 'places.sqlite')

def prepareDataFile(suffix): 
    """
    Application to wrap the history of the Firefox browser. This
    script should be executed with each svn commit.
    """
    
    global dataDir
    global dataFile
    global _tmpFile

    pla.logMessage('firefox: prepare ' + dataFile)

    # If no file is present in pladirectory, no instrumentation
    if not os.path.exists(dataDir):
        pla.logMessage('firefox: Disabled. Skipping')
        return []

    # Copy the Firefox SQLite database to the tmp directory, in order to avoid
    # lock issues.
    ffoxDir = os.path.expanduser('~/.mozilla/firefox/')
        
    # Parse the ffox configuration
    config = ConfigParser.ConfigParser()
    config.read(os.path.join(ffoxDir, 'profiles.ini'))
    profileDir = os.path.join(ffoxDir, config.get('Profile0', 'Path'))
        
    sqliteFile = os.path.join(profileDir, 'places.sqlite')
    pla.logMessage('firefox: duplicating file ' + sqliteFile)
    shutil.copyfile(sqliteFile, _tmpFile)
        
    # Get the timestamp for the last execution
    lastExecution = pla.getLastExecutionTStamp()
    pla.logMessage('Last execution: ' + str(lastExecution))

    date_clause = ''
    if lastExecution != None:
        date_clause = "AND visit_date > " + str(int(lastExecution * 1000000))
        
    # Get the last activity from Firefox, through a query to the
    # history table
    conn = sqlite3.connect(_tmpFile)
    conn.row_factory = sqlite3.Row
    
    c = conn.cursor()
    query = """
          SELECT url, DATETIME(CAST (visit_date/1000000.0 AS INTEGER), 'unixepoch', 'localtime') AS timestamp
          FROM   moz_historyvisits h, moz_places p
          WHERE  h.place_id = p.id
          """ + date_clause + """ ORDER  BY visit_date """
    pla.logMessage('firefox: Query = ' + query)
    c.execute(query)

    # Create a duplicate of the data file with the suffix
    toSendFileName = dataFile + '_' + suffix

    # Dump the data. Detect empty data because "rowcount" seems broken.
    dataOut = open(toSendFileName, 'w')
    noData = True
    for row in c:
        dataOut.write(row['timestamp'] + ' ' + row['url'] + '\n')
        noData = False
        
    # Close the statement and the data file
    c.close()
    dataOut.close()

    # Remove the profile copy form the tmp directory
    os.remove(_tmpFile)
    if noData:
        pla.logMessage('firefox empty data file detected. Removing')
        os.remove(toSendFileName)
        return []

    return [toSendFileName]

def main():
    """
    Script to store the history of firefox
    """
    
    prepareDataFile('bogus')

if __name__ == "__main__":
    main()

#!/usr/bin/python
# -*- coding: utf-8 -*-#
#
# Author: Derick Leony (dleony@it.uc3m.es)
#
import os, sys, datetime, ConfigParser, sqlite3, shutil

import PLABasic

dataDir = os.path.join(PLABasic.plaDirectory, 'tools', 'firefox')
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

    PLABasic.logMessage('firefox: prepare ' + dataFile)

    # If no file is present in pladirectory, no instrumentation
    if not os.path.exists(dataDir):
        PLABasic.logMessage('firefox: Disabled. Skipping')
        return []

    # Copy the Firefox SQLite database to the tmp directory, in order to avoid
    # lock issues.
    ffoxDir = os.path.expanduser('~/.mozilla/firefox/')
        
    # Parse the ffox configuration
    config = ConfigParser.ConfigParser()
    config.read(os.path.join(ffoxDir, 'profiles.ini'))
    profileDir = os.path.join(ffoxDir, config.get('Profile0', 'Path'))
        
    sqliteFile = os.path.join(profileDir, 'places.sqlite')
    PLABasic.logMessage('firefox: duplicating file ' + sqliteFile)
    shutil.copyfile(sqliteFile, _tmpFile)
        
    # Get the timestamp for the last execution
    lastExecution = PLABasic.getLastExecutionTStamp()
    date_clause = ''
    if lastExecution != None:
        date_clause = "AND    visit_date > " + str(int(lastExecution * 1000000))
        
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
    PLABasic.logMessage('firefox: Query = ' + query)
    c.execute(query)

    # If nothing is selected, we are done
    if c.rowcount <= 0:
        return []

    # Create a duplicate of the data file with the suffix
    toSendFileName = dataFile + '_' + suffix

    # Dump the data
    dataOut = open(toSendFileName, 'w')
    for row in c:
        dataOut.write(row['timestamp'] + ' ' + row['url'] + '\n')
        
    # Close the statement and the data file
    c.close()
    dataOut.close()

    # Remove the profile copy form the tmp directory
    os.remove(_tmpFile)

    return [toSendFileName]
def main():
    """
    Script to store the history of firefox
    """
    
    prepareDataFile('bogus')

if __name__ == "__main__":
    main()

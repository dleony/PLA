#!/usr/bin/python
# -*- coding: utf-8 -*-#
#
# Author: Derick Leony (dleony@it.uc3m.es)
#
import os, sys, datetime, ConfigParser, sqlite3, shutil

import PLABasic

dataDir = os.path.join(PLABasic.plaDirectory, 'tools', 'firefox')
dataFile = os.path.join(dataDir, 'firefox')

dateBegin = str(datetime.datetime.now())[:-7]

def main(): 
    """
    Application to wrap the history of the Firefox browser. This
    script should be executed with each svn commit.
    """
    
    global dataDir
    global dataFile

    PLABasic.logMessage("firefox: plaDirectory = " + PLABasic.plaDirectory)
    PLABasic.logMessage("firefox: DataFile = " + dataFile)

    # If no file is present in pladirectory, no instrumentation
    doLogging = os.path.exists(dataDir)
    if doLogging:
        # Copy the Firefox SQLite database to the tmp directory, in order
        # to avoid lock issues.
        
        ffoxDir = os.path.join(PLABasic.plaDirectory, '../../.mozilla/firefox/')
        
        config = ConfigParser.ConfigParser()
        config.read(os.path.join(ffoxDir, 'profiles.ini'))
        profileDir = os.path.join(ffoxDir, config.get('Profile0', 'Path'))
        
        shutil.copyfile(os.path.join(profileDir, 'places.sqlite'), '/tmp/places.sqlite')
        
        # Get the timestamp for the last svn commit
        lastexec_file = os.path.join(PLABasic.plaDirectory, 'tools/.lastexecution')
        date_clause = ""
        if (os.path.exists(lastexec_file)):
            statinfo = os.stat(lastexec_file)
            init_timestamp = statinfo.st_mtime*1000000
            date_clause = "AND    visit_date > " + str(int(init_timestamp))
            print date_clause;
        
        # Get the last activity from Firefox, through a query to the
        # history table
        
        conn = sqlite3.connect('/tmp/places.sqlite')
        conn.row_factory = sqlite3.Row
    
        c = conn.cursor()
        c.execute("""
          SELECT url, DATETIME(CAST (visit_date/1000000.0 AS INTEGER), 'unixepoch', 'localtime') AS timestamp
          FROM   moz_historyvisits h, moz_places p
          WHERE  h.place_id = p.id
          """ + date_clause + """
          ORDER  BY visit_date
        """)

        # Dump a mark status and time/date
        dataOut = open(dataFile, 'a')

        dataOut.write('---- ' \
                          + dateBegin + ' ' \
                          + str(datetime.datetime.now())[:-7] + ' ' \
                          + ' '.join(map(lambda x: '\'' + x + '\'', sys.argv)) \
                          + '\n')
        
        for row in c:
            dataOut.write('---- ' \
                          + str(datetime.datetime.now())[:-7] + ' ' \
                          + ' ' + row['timestamp'] \
                          + ' ' + row['url'] \
                          + '\n')
            
        c.close()
            
        # Remove the profile copy form the tmp directory
        os.remove('/tmp/places.sqlite')
            
    else:
        PLABasic.logMessage(prefix + ': Disabled. Skipping')
        
if __name__ == "__main__":
    main()
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
import sys, os, getopt, locale, codecs, hashlib

# Fix the output encoding when redirecting stdout
if sys.stdout.encoding is None:
    (lang, enc) = locale.getdefaultlocale()
    if enc is not None:
        (e, d, sr, sw) = codecs.lookup(enc)
        # sw will encode Unicode data to the locale-specific character set.
        sys.stdout = sw(sys.stdout)

# Import the Adagio package
_dirName = os.path.dirname(__file__)
_dirName = os.path.abspath(os.path.join(_dirName, '..'))
sys.path.insert(0, _dirName)
sys.path.insert(0, os.path.join(_dirName, 'pla'))

import pla, pla.mysql

def main():
    """
    Script that given a matrix encoded in a CSV file with the following
    structure:
    
    EMTPY;Label1;Label2;...;LabelN
    UserID1;Value1;Value2;...;ValueN
    UserID2;Value1;Value2;...;ValueN
    ...
    UserIDM;Value1;Value2;...;ValueN

    and the parameters to connect to a relational database:
      - host
      - user
      - passwd
      - dbname
      
    connects to a CAM database and inserts in the RelatedentityMetadata table an
    entry for each element in the matrix with the following structure:

    INSERT INTO RelatedentityMetadata (metadata, metadataBinding, metadataHash,
                                       metadataType, relatedentityfk) VALUES

    (LabelI=ValueI, 
     'Plaintext', 
     HEXDIGEST, 
     TBD (no idea), 
     FK of UserID1 in other table)

    Script invocation:

    script [options] CSVFile CSVFile ...

    Options :

    -h hostname For DB connection (default localhost)
    
    -u username For DB connection
  
    -p passwd For DB connection (default is empty)
    
    -d dbname For DB connection

    -r When given, the query 'DELETE' is executed instead of INSERT if the one
     single row matches the condition

      metadata == LabelI=ValueI and relatedentityfk == UserID1 and 

    -n When given, the queries are printed instead of executed

    -s char Character to use as separator (default ';')

    Example:
    Insert (dry run)
    insertusermetadata.py -n -u user -p passwd -d dbname FILE.csv
    Insert
    insertusermetadata.py -u user -p passwd -d dbname FILE.csv
    Remove (dry run)
    insertusermetadata.py -r -n -u user -p passwd -d dbname FILE.csv
    Remove
    insertusermetadata.py -r -u user -p passwd -d dbname FILE.csv
    """

    # Default value for the options
    hostname = 'localhost'
    username = None
    passwd = ''
    dbname = None
    dryRun = False
    drop = False
    separator = ';'

    # Swallow the options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:u:p:d:rns:", [])
    except getopt.GetoptError, e:
        print str(e)
        sys.exit(2)

    # Parse the options
    for optstr, value in opts:
        # Hostname
        if optstr == "-d":
            dbname = value

        # dbname
        elif optstr == "-h":
            hostname = value

        # Dry run
        elif optstr == "-n":
            dryRun = True

        # Username
        elif optstr == "-p":
            passwd = value

        # Remove entries
        elif optstr == "-r":
            drop = True

        # Separator
        elif optstr == "-s":
            separator = value
        # Username
        elif optstr == "-u":
            username = value


    if args == []:
        print 'The script needs at least a file name'
        print main.__doc__
        sys.exit(1)

    cursor = None
    try:
        pla.mysql.connect(hostname, username, passwd, dbname)
    except Exception, e:
        print 'Unable to connect with the database'
        print str(e)
        sys.exit(1)
    cursor = pla.mysql.cursorObj

    # Cook up the right query
    if drop:
        mainQuery = """
            DELETE FROM RelatedentityMetadata
            WHERE metadata = %s and relatedentityfk = %s
                """
    else:
        mainQuery = """
            INSERT INTO RelatedentityMetadata 
            (metadata, metadataBinding, metadataHash,
            metadataType, relatedentityfk) VALUES
            (%s, %s, %s, %s, %s)
                """

    # Loop over all the given data files
    insertCount = 0
    skipCount = 0
    notNeededCount = 0
    multipleEntities = 0
    paramList = []
    for fileName in args:
        if not os.path.isfile(fileName):
            print '/* Only regular files are allowed. Skipping', fileName, '*/'
            continue

        print '/* Processing', fileName, '*/'
        
        dataIn = codecs.open(fileName, 'r')
        labels = dataIn.readline()[:-1].split(separator)[1:]
        print '/*  Labels:', ', '.join(labels), '*/'

        checkLength = len(labels) + 1
        lineNumber = 1
        for line in dataIn:
            fields = line[:-1].split(separator)
            if len(fields) != checkLength:
                print '/* Line', lineNumber, \
                    'with incorrect number of elements */'
                sys.exit(1)

            # Select the person with the ID
            pla.mysql.selectPerson(fields[0])

            if cursor.rowcount > 1:
                print '/* WARNING: ID', fields[0], \
                    'has more than one entity in Relatedentity. Skipping */'
                multipleEntities += 1
                continue

            if cursor.rowcount == 0:
                print '/* ID', fields[0], \
                    'not found in Relatedentity. Skipping */'
                skipCount += 1
                continue

            (entityID, mimetype, name, reference, entityType) = cursor.fetchone()

            # Insert for the found person, one metadata per label
            metadata = zip(labels, fields[1:])
            for (n, v) in metadata:
                
                # Metadata string to store
                toStore = n + '=' + v

                # Create the ID for the new element
                m = hashlib.sha1()
                m.update(entityID + ':' + toStore)
                
                # See if there is already some row with this data
                query = """
                    SELECT * FROM RelatedentityMetadata
                     WHERE metadata = %s and relatedentityfk = %s
                        """
                params = (toStore, entityID)
                cursor.execute(query, params)
                dataPresent = cursor.rowcount == 1
                
                # Cook up the right parameters
                if drop:
                    params = (toStore, entityID)
                else:
                    params = (toStore, 
                              'Plaintext',
                              m.hexdigest(),
                              '',
                              entityID)

                # Make sure the operation is needed
                if (drop and not dataPresent) or (not drop and dataPresent):
                    print '/* WARNING: Operation not required. Skipping. */'
                    notNeededCount += 1
                    continue

                # Store the params in the paramList
                paramList.append(params)

                insertCount += 1
            lineNumber += 1

    # Either print or execute the query
    if dryRun:
        for params in paramList:
            print mainQuery, params
    else:
        # Run the multirow insertion
        try:
            cursor.executemany(mainQuery, paramList)
            pla.mysql.dbconnection.commit()
        except:
            print '/* ERROR executing', mainQuery, '*/'
            pla.mysql.dbconnection.rollback()
            sys.exit(1)

    # Done. Close connection
    pla.mysql.disconnect()
    
    print '/* Done */'
    print '/* Summary: */'
    if drop:
        print '/*  - Deleted:', insertCount, '*/'
    else:
        print '/*  - Inserted:', insertCount, '*/'
    print '/*  - Not needed (already in table):', notNeededCount, '*/'
    print '/*  - Skipped IDs (not found):', skipCount, '*/'
    print '/*  - Multiple IDs:', multipleEntities, '*/'

if __name__ == "__main__":
    main()
    

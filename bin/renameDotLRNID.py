#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Copyright (C) 2010 Carlos III University of Madrid
# This file is part of the PLA: Personal Learning Assistant

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

# Import the pla package
_dirName = os.path.dirname(__file__)
_dirName = os.path.abspath(os.path.join(_dirName, '..'))
sys.path.insert(0, _dirName)
sys.path.insert(0, os.path.join(_dirName, 'pla'))

import pla, pla.mysql

def main():
    """
    Script that given a CSV file with the correspondence (personID,
    dotlrnUserID) and the parameters to connect to a CAM database, it renames
    the entites of type person so that the 'reference' field is changed based on
    the given correspondence.

    Script invokation

    script [options] UserNameTo.LRNID.csv

    Options :

    -d dbname For DB connection

    -h hostname For DB connection (default localhost)
    
    -n When given, the queries are printed instead of executed

    -p passwd For DB connection (default is empty)
    
    -r      Assume that CSV file contains (dotlrnUserID, personID)

    -s char Character to use as separator in the CSV (default ',')

    -t tablename Name of the table where the entities are stored

    -u username For DB connection
  
    Example

    script -u abel -p ????? -d 2010_AS_Pairs file.csv
    """

    # Default value for the options
    hostname = 'localhost'
    username = None
    passwd = ''
    dbname = None
    dryRun = False
    separator = ','
    tableName = 'relatedentity'
    reverseDictionary = False

    # Swallow the options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "d:h:np:rs:t:u:", [])
    except getopt.GetoptError, e:
        print str(e)
        sys.exit(2)


    # Parse the options
    for optstr, value in opts:
        # DB Name
        if optstr == "-d":
            dbname = value

        # Hostname
        elif optstr == "-h":
            hostname = value

        # Dry run
        elif optstr == "-n":
            dryRun = True

        # Passwd
        elif optstr == "-p":
            passwd = value

        # Reverse dictionary
        elif optstr == "-r":
            reverseDictionary = True

        # Separator
        elif optstr == "-s":
            separator = value

        # Table name
        elif optstr == "-t":
            tableName = value

        # Username
        elif optstr == "-u":
            username = value

    if len(args) != 1:
        print 'The script needs a file as last argument.'
        print main.__doc__
        sys.exit(1)

    if not os.path.isfile(args[0]):
        print 'The script needs a regular existing file as last argument.'
        print main.__doc__
        sys.exit(1)

    # Swallow the CSV file and turn into a dictionary
    lrnTOPerson = {}
    for line in codecs.open(args[0], 'r'):
        # Skip lines starting with #
        if line[0] == '#':
            continue

        fields = line[:-1].split(separator)
        
        # Skip lines without two fields
        if len(fields) != 2:
            continue

        if reverseDictionary:
            lrnTOPerson[fields[0]] = fields[1]
        else:
            lrnTOPerson[fields[1]] = fields[0]

    # Connect to the DB
    cursor = None
    try:
        pla.mysql.connect(hostname, username, passwd, dbname)
    except Exception, e:
        print 'Unable to connect with the database'
        print str(e)
        sys.exit(1)
    cursor = pla.mysql.cursorObj

    selectQuery = """
         SELECT id, reference FROM """ + tableName + """
         WHERE type = 'person'
        """

    alterQuery = """
         UPDATE """ + tableName + """
         SET reference = %s WHERE id = %s
       """

    # Get all entities of type person
    pla.mysql.executeTransaction(selectQuery, None, False)
    data = cursor.fetchall()

    dataPairs = []
    for (ref, eID) in data:
        newID = lrnTOPerson.get(eID)
        if newID == None:
            print '/*', eID, 'no found. Skipping. */'
            continue

        # Pile up the new pair for further execution
        print '/* Rename', eID, 'to', newID
        dataPairs.append((newID, ref))

    if len(dataPairs) == 0:
        print '/* no data to be modified */'
    else:        
        # Execute the query
        pla.mysql.executeTransaction(alterQuery, dataPairs, dryRun)

    # Done. Close connection
    pla.mysql.disconnect()

if __name__ == "__main__":
    main()
    

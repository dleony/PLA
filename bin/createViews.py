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

# Import the PLA package
_dirName = os.path.dirname(__file__)
_dirName = os.path.abspath(os.path.join(_dirName, '..'))
sys.path.insert(0, _dirName)
sys.path.insert(0, os.path.join(_dirName, 'pla'))

import pla, pla.mysql

def main():
    """
    Script that given a table and a column name, creates as many views of that
    table as distinct values are in that column.

    script [options] table column

    Options:

    -d dbname For DB connection

    -h hostname For DB connection (default localhost)
 
    -n When given, the queries are printed instead of executed.
    
    -p passwd For DB connection (default is empty)
    
    -r When given, remove the views instead of creating them

    -u username For DB connection
  
    Example:

    createViews.py -h localhost -u abel -p blah -d 2010_AS_Pairs 
                   relatedentity type

    """

    # Default value for the options
    hostname = 'localhost'
    username = None
    passwd = ''
    dbname = None
    dryRun = False
    drop = False

    # Swallow the options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "d:h:np:ru:", [])
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

        # Remove entries
        elif optstr == "-r":
            drop = True

        # Username
        elif optstr == "-u":
            username = value

    # Make sure two more args are given (tableName and columnName)
    if len(args) != 2:
        print 'The script needs table name and column name'
        print main.__doc__
        sys.exit(1)
    (tableName, columnName) = args

    createViewQuery = """
        CREATE OR REPLACE ALGORITHM = UNDEFINED VIEW `{0}` AS 
          SELECT * FROM `{1}` 
          WHERE {1}.{2} LIKE %s"""

    dropViewQuery = 'DROP VIEW IF EXISTS '

    # Open the connection to the DB
    cursor = None
    try:
        pla.mysql.connect(hostname, username, passwd, dbname)
    except Exception, e:
        print 'Unable to connect with the database'
        print str(e)
        sys.exit(1)
    cursor = pla.mysql.cursorObj
    
    rowValuesQuery = 'SELECT DISTINCT(' + columnName + ') FROM ' + tableName

    # Get the distinct values
    try:
        cursor.execute(rowValuesQuery)
    except Exception, e:
        print '/* ERROR executing', rowValuesQuery, (columnName, tableName), '*/'
        print str(e)
        pla.mysql.dbconnection.rollback()
        sys.exit(1)

    entityTypes = [x for (x,) in cursor.fetchall()]

    # If -r is given, drop the views and terminate
    if drop:
        viewNames = ', '.join(["`" + tableName + '_' + x  + "`"
                               for x in entityTypes])

        # Either execute the query or print it
        if dryRun:
            print dropViewQuery, viewNames
        else:
            try:
                cursor.execute(dropViewQuery + viewNames)
            except Exception, e:
                print '/* ERROR executing', dropViewQuery, viewNames, '*/'
                print str(e)
                sys.exit(1)
        return

    # Loop over each of the entity types and execute the query
    for entity in entityTypes:
        query = createViewQuery.format(tableName + '_' + entity,
                                       tableName,
                                       columnName)

        # Either execute or print the query
        if dryRun:
            print query, entity
        else:
            try:
                cursor.execute(query, (entity,))
                pla.mysql.dbconnection.commit()
            except Exception, e:
                print '/* ERROR executing', query, entity, '*/'
                print str(e)
                pla.mysql.dbconnection.rollback()
                sys.exit(1)
        
if __name__ == "__main__":
    main()

    

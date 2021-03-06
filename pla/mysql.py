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
import sys, MySQLdb, hashlib

host = None
user = None
passwd = None
dbname = None
dbconnection = None
cursorObj = None

select_entity_query = \
    """
SELECT * FROM relatedentity WHERE entityId = '%s' AND metadataId = '%s'
                            AND metadataReference = '%s' AND mimetype = '%s' 
                            AND name = '%s'
    """
insert_entity_query = \
    """INSERT INTO relatedentity (%s) VALUES (%s)"""

def connect(givenHost = None, givenUser = None, givenPasswd = None, 
            givenDB = None):
    """
    Connect to the given database and store the connection in the global
    variable dbconnection. Create also the cursorObj
    """

    global host
    global user
    global passwd
    global dbname
    global dbconnection
    global cursorObj
    
    # If not enough information is given, bomb out
    if givenUser == None or givenDB == None:
        raise ValueError('mysql: Need at least user AND db to connect to db')

    host = givenHost
    user = givenUser
    passwd = givenPasswd
    dbname = givenDB

    dbconnection = MySQLdb.connect(host=givenHost,
                                   user=givenUser,
                                   passwd=givenPasswd,
                                   db=givenDB)

    cursorObj = dbconnection.cursor()
    return

def disconnect():
    """
    Terminate the connection with the database
    """

    global host
    global user
    global passwd
    global dbname
    global dbconnection
    global cursorObj

    host = None
    user = None
    passwd = None
    dbconnection.close()
    dbconnection = None
    cursorObj = None

def find_or_add_entity(ent_id, md_id = None, md_ref = None, mime_type = None, 
                       name = None):
    """
    First search for an entity and if not found, insert it.
    """

    global select_entity_query
    global insert_entity_query
    global dbconnection
    global cursorObj

    # Translate Nones to NULLs
    if ent_id == None:
        ent_id = 'NULL'
    if md_id == None:
        md_id = 'NULL'
    if md_ref == None:
        md_ref = 'NULL'
    if mime_type == None:
        mime_type = 'NULL'
    if name == None:
        name = 'NULL'

    data_pack = (ent_id, md_id, md_ref, mime_type, name)
    # Execute the select query
    cursorObj.execute(select_entity_query, data_pack)

    row = cursorObj.fetchone()
    # If found, forget it
    if row != None:
        return

    cursosObj.execute(insert_entity_query, data_pack)
    
    return dbconnection.insert_id()

def selectPerson(personID):
    """
    Selects from the relatedentity table the person with the given ID
    """
    
    global cursorObj

    # Fetch the information about the entity with the userID
    query = """
        SELECT * FROM relatedentity WHERE type = 'person' and name = %s
            """
    cursorObj.execute(query, (personID,))

    return

def executeTransaction(query, values, dryRun):
    """
    Push the transction, and if failure, retract it and bomb out. If values is a
    list, the executemany function is used.
    """

    global cursorObj

    try:
        if values == None:
            if dryRun:
                print query
            else:
                cursorObj.execute(query)
        elif type(values) == list:
            if dryRun:
                for x in values:
                    print query, x
            else:
                cursorObj.executemany(query, values)
        else:
            if dryRun:
                print query, values
            else:
                cursorObj.execute(query, values)
        dbconnection.commit()
    except Exception, e:
        print '/* Error executing', query, '*/'
        print str(e)
        sys.exit(1)

    return

def getHexDigest(item):
    """
    Given an item, create an hex digest
    """
    m = hashlib.sha1()
    m.update(str(item))
    return m.hexdigest()

# Example
# execute SQL query using execute() method.
# cursorobj.execute("SELECT VERSION()")
# Fetch a single row using fetchone() method.
# data = cursorobj.fetchone()
# print "Database version : %s " % data
# disconnect from server
# db.close()

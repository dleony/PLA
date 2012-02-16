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
cursor_obj = None

select_entity_query = \
    """
SELECT id FROM relatedentity WHERE 
       entityId %s
       AND metadataId %s
       AND metadataReference %s
       AND mimetype %s
       AND name %s
    """

insert_entity_query = \
    """INSERT INTO relatedentity (entityId, metadataId, metadataReference,
                                  mimetype, name) 
                                 VALUES (%s, %s, %s, %s, %s)"""

select_event_query = \
    """
SELECT * FROM event WHERE name = %s AND datetime = %s
                            AND sharingLevel %s
    """

select_event_by_hash_query = """SELECT * FROM event WHERE hash = %s"""

insert_event_query = \
    """INSERT INTO event (datetime, name, sharinglevel, hash) 
                                 VALUES (%s, %s, %s, %s)"""

insert_eventrelatedentity_query = \
    """INSERT INTO eventrelatedentity (role, eventid, relatedentityid) 
                                 VALUES (%s, %s, %s)"""

select_relatedentities_query = \
    """SELECT role, relatedentityid FROM eventrelatedentity WHERE eventid = %s"""

def connect(givenHost = None, givenUser = None, givenPasswd = None, 
            givenDB = None):
    """
    Connect to the given database and store the connection in the global
    variable dbconnection. Create also the cursor_obj
    """

    global host
    global user
    global passwd
    global dbname
    global dbconnection
    global cursor_obj
    
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

    # Very important, if not, the default seems to be latin1
    dbconnection.set_character_set('utf8')

    cursor_obj = dbconnection.cursor()
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
    global cursor_obj

    host = None
    user = None
    passwd = None
    dbconnection.close()
    dbconnection = None
    cursor_obj = None

def find_entity(ent_id, md_id = None, md_ref = None, mime_type = None, 
                name = None):
    """
    Search for an entity with the given fields
    """
    global select_entity_query
    global cursor_obj

    query = select_entity_query % (("= %s", "IS %s")[ent_id is None], 
                                   ("= %s", "IS %s")[md_id is None],
                                   ("= %s", "IS %s")[md_ref is None],
                                   ("= %s", "IS %s")[mime_type is None],
                                   ("= %s", "IS %s")[name is None])
    
    cursor_obj.execute(query, (ent_id, md_id, md_ref, mime_type, name))

    row = cursor_obj.fetchone()

    # If found, return the ID
    if row != None:
        return row[0]

    return None

def find_or_add_entity(ent_id, md_id = None, md_ref = None, mime_type = None, 
                       name = None):
    """
    First search for an entity and if not found, insert it. Return a tuple with
    the (id, isNew?) to differentiate between find and add.
    """

    global insert_entity_query
    global dbconnection
    global cursor_obj

    # First search, if found, terminate
    entity_id = find_entity(ent_id, md_id, md_ref, mime_type, name)
    if entity_id != None:
        return (entity_id, False)

    # Insert the entity
    cursor_obj.execute(insert_entity_query, 
                      (ent_id, md_id, md_ref, mime_type, name))
    
    return (dbconnection.insert_id(), True)

def find_event_by_hash(hash_value):
    """
    Search an event by the hash value
    """
    
    global select_event_by_hash_query
    global cursor_obj

    cursor_obj.execute(select_event_by_hash_query, (hash_value,))

    return cursor_obj.fetchone()
    
def find_event(event_name, date_time, sharing_level):
    """
    Search for an event with the three given fields.
    """

    global select_event_query
    global cursor_obj

    # If two of the three parameters are not given, raise exception
    if event_name == None or date_time == None:
        raise ValueError('Event with NULL name or datetime in select_event')

    query = select_event_query % ('%s', '%s', 
                                  ("= %s", "IS %s")[sharing_level is None])
    
    data_pack = (event_name, 
                 date_time.strftime('%Y-%m-%d %H:%M:%S'), sharing_level)

    cursor_obj.execute(query, data_pack)

    return cursor_obj.fetchall()

def insert_event(event_name, date_time, sharing_level, event_hash):
    """
    Search for an event with the three given fields.
    """

    global insert_event_query
    global dbconnection
    global cursor_obj

    # If two of the three parameters are not given, raise exception
    if event_name == None or date_time == None:
        raise ValueError('Event with NULL name or datetime in insert_event')

    data_pack = (date_time.strftime('%Y-%m-%d %H:%M:%S'), event_name, 
                 sharing_level, event_hash)

    cursor_obj.execute(insert_event_query, data_pack)

    return dbconnection.insert_id()

def find_eventrelatedentity(event_id):
    """
    Given a event_id, get the related entities.
    """

    global select_relatedentities_query
    global cursor_obj

    # If the event_id is None, raise exception
    if event_id == None:
        raise ValueError('Event related identity with None event_id')

    cursor_obj.execute(select_relatedentities_query, (event_id, ))

    return cursor_obj.fetchall()

def find_identical_event(event, entities):
    """
    Insert an event with the following structure ONLY if it is not in the
    database already.
    
    event: (Event_Name, Datetime, SharingLevel)
    entities: [ List of pairs (Role, Entity) ])

    Event_Name: String
    DateTime: datetime
    SharingLevel: String
    Role: String

    And the entities are:
    (entityid, metadataid, metadataref, mimetype, name)

    All of them strings.

    Returns either None or the event id
    """
    
    (evname, dt, shl, entity_list) = event
    
    # Search for all the events with identical name, date and sharing level
    found_events = find_event(evname, dt, shl)

    # If no events with these data have been found, return
    if found_events == ():
        return None

    # Loop over all the candidates and check they point to the same entities
    for (evid, dt, name, sh, hash_value) in found_events:
        # Entities related to the considered event
        related_ent = find_eventrelatedentity(evid)

        # If the number of entities is different, this event is not what we are
        # looking for.
        if len(related_ent) != len(entities):
            continue

        # Translate into list of string, long
        related_ent = [(a, long(b)) for (a, b) in related_ent]
        
        # If sets are identical, found it
        if set(related_ent) == set(entities):
            # FOUND IT
            return evid
    return None

def insert_eventrelatedentity(triplet_list):
    """
    Insert a list of triplets (role, eventid, relatedentityid)
    """

    global insert_eventrelatedentity_query
    global dbconnection
    global cursor_obj

    cursor_obj.executemany(insert_eventrelatedentity_query,
                          triplet_list)

    return dbconnection.insert_id()

def executeTransaction(query, values, dryRun):
    """
    Push the transction, and if failure, retract it and bomb out. If values is a
    list, the executemany function is used.
    """

    global cursor_obj

    try:
        if values == None:
            if dryRun:
                print query
            else:
                cursor_obj.execute(query)
        elif type(values) == list:
            if dryRun:
                for x in values:
                    print query, x
            else:
                cursor_obj.executemany(query, values)
        else:
            if dryRun:
                print query, values
            else:
                cursor_obj.execute(query, values)
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

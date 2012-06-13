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
import sys, pymongo, datetime

host = None
user = None
passwd = None
dbname = None
dbconnection = None
database = None
cursor_obj = None

event_collection_name = 'events'
event_collection = None

user_collection_name = 'users'
user_collection = None

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
    global database
    global cursor_obj
    global event_collection_name
    global event_collection
    global user_collection_name
    global user_collection

    # If not enough information is given, bomb out
    if givenHost == None or givenDB == None:
        raise ValueError('mongo: Need at least host and db to connect')

    host = givenHost
    user = givenUser
    passwd = givenPasswd
    dbname = givenDB

    connect_URL = host
    user_str = ''
    if user != None and user != '':
        user_str = user
    if passwd != None and passwd != '':
        user_str = user_str + ':' + passwd
    if user_str != '':
        connect_URL = user_str + '@' + connect_URL

    dbconnection = pymongo.Connection(host=connect_URL)

    database = dbconnection[dbname]

    event_collection = database[event_collection_name]
    user_collection = database[user_collection_name]

    # event_collection.ensure_index([("datetime", pymongo.ASCENDING), 
    #                                ("user", pymongo.ASCENDING)])

    user_collection.ensure_index([("name", pymongo.ASCENDING)], unique = True)
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
    global event_collection
    global user_collection

    host = None
    user = None
    passwd = None
    dbconnection.disconnect()
    dbconnection = None
    cursor_obj = None
    event_collection = None
    user_collection = None

def insert_event(event):
    """
    The events are received as tuples with the following structure:
    
    (name, datetime, user, [(key1, value1), (key2, value2), ...])
    
    datetime is a datetime object, the rest strings.
    """

    global event_collection

    (event_name, dt, user, other_keys) = event

    user_id = find_or_add_user(user)

    event_data = [('name', event_name), ('datetime', dt), 
                  ('user', [{'_id': user_id}])]

    event_data.extend(other_keys)
    return event_collection.insert(dict(event_data), safe = True)

# 
# User related function
#
def find_or_add_user(user):
    """
    Find or insert user
    """

    global dbconnection
    global user_collection

    # Find
    found_obj = find_user(user)

    if found_obj != None:
        return found_obj['_id']

    # Add the given user
    return user_collection.insert({'name': user}, safe = True)

def find_user(user):
    """
    Find a user
    """

    global user_collection

    return user_collection.find_one({'name': user})

def update_user(user, pairs):
    """
    Given a user and a dictionary with pairs key, value, update the values in
    the given user. If the user parameter is simply an string, then it is turned
    into a spec with 'name' equal to that value. If user is a dictionary, it is
    directly passed to the update operation.
    """
    
    global user_collection

    if type(user) != dict:
        user = {'name': user}

    return user_collection.update(user, {"$set": pairs}, upsert = False, 
                                  safe = True)


################################################################################
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
    m.update(str(item), safe = True)
    return m.hexdigest()

def main():
    """
    Function to test how to connect to a mongo db
    """

    # Connect to the db
    connect(givenHost = '127.0.0.1', givenDB = 'as_2011')

    ev1 = [('name', 'name1'), 
           ('datetime', datetime.datetime(2012, 2, 29, 12, 12)),
           ('user', [{'id1': 'user1', 'apt1': 'v1', 'apt2': 'v2'}, {'id2': 'user2'}])]

    event_id = insert_event(ev1)
    
    print 'Inserted', event_id

    print find_event(ev1)

    # Disconnect to the db
    disconnect()

# Execution as script
if __name__ == "__main__":
    main()

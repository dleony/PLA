#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Module to dump events in different formats.
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
import sys, locale, codecs, getopt, os, anonymize, mysql, datetime, hashlib

import rule_manager, rules_common

# Fix the output encoding when redirecting stdout
if sys.stdout.encoding is None:
    (lang, enc) = locale.getdefaultlocale()
    if enc is not None:
        (e, d, sr, sw) = codecs.lookup(enc)
        # sw will encode Unicode data to the locale-specific character set.
        sys.stdout = sw(sys.stdout)

#
# Module prefix
#
module_prefix = 'event_output'

#
# Configuration parameters for this module
#
config_params = {
    'format': 'CSV',      # Format to dump the output.
                          # One of: "CSV", "CAM_DB", "mongo"
    # These parameters apply only to the CSV format
    'print_ordinal': '',  # Print ordinal as first column in CSV
    'output_file': '',    # File to write in CSV format
    # These parameter apply only to the DB format
    'db_host': '',        # Data base connection parameters
    'db_user': '',
    'db_passwd': '',
    'db_name': '',
    # These parameters apply to any format
    'from_date': '',      # Date from which to process events
    'until_date': '',     # Date until which to process events
    'exclude_users': ''   # Space separated list of user ids to exclude
}

event_counter = 0 # Number of events processed

entity_cache = {}

output_file = None

debug = 0

output_format = None

print_ordinal = False

exclude_users = set([])

csv_hash = set([])

def initialize(module_name):
    """
    Initialize the output process for events.
    """

    global debug
    global output_format
    global exclude_users

    # Get the level of debug
    debug = int(rule_manager.get_property(None, module_name, 'debug'))

    # Fetch format
    output_format = rule_manager.get_property(None, module_name, 'format')

    # Create set of users to exclude
    exclude_users = map(lambda x: anonymize.find_or_encode_string(x),
                        set(rule_manager.get_property(None, module_prefix,
                                                      'exclude_users').split()))

    # Make sure we initialize the anonymize features common to all methods
    anonymize.initialize()

    if output_format == 'CSV':
        init_csv(module_name)
    elif output_format == 'CAM_DB':
        init_cam_db(module_name)
    elif output_format == 'mongo':
        init_mongo_db(module_name)
    else:
        init_csv(module_name)

def execute(module_name):
    """
    Bogus function to perform the required task, but this module is an auxiliary
    one, thus, no task to execute.
    """
    return

def out(event_list):
    """
    Function that receives a list of event elements and dumps them depending on
    the value of the global variable output_format.

    The structure of the list elements is (all are nested lists):

    [Event_Name, Datetime, SharingLevel, [List of pairs [Role, Entity]]]

    Event_Name: String
    DateTime: datetime
    SharingLevel: String
    Role: String

    And the entities are:
    [entityid, metadataid, metadataref, mimetype, name]

    All of them strings.
    """

    global module_prefix
    global output_format
    global exclude_users

    # Check that the event has the minimum amount of info
    incorrect_event = next((x for x in event_list \
                                if (x[0] == None) or \
                                (x[0] == '') or \
                                (x[1] == None) or \
                                (x[3] == []) or \
                                (len(x[3]) == 0)), None)
    if incorrect_event:
        print >> sys.stderr, 'Incorrect event detected:'
        print_event(incorrect_event)
        sys.exit(1)

    # Loop over all the events and print them
    # for event in event_list:
    #     print_event(event)

    # Filter those events that have a user in the exclude_users
    new_list = []
    for event in event_list:
        (ev_name, dt, shl, ent_list) = event
        user_ent = next((x for x in ent_list if x[0] == 'user'), None)
        # Ignore if user entity with id in exlclude users
        if user_ent and (user_ent[1][0] in exclude_users):
            continue
        # append event to the filtered list
        new_list.append(event)

    event_list = new_list

    if output_format == 'CSV':
        out_csv(event_list)
    elif output_format == 'CAM_DB':
        out_cam_db(event_list)
    elif output_format == 'mongo':
        out_mongo_db(event_list)
    else:
        out_csv(event_list)

def flush():
    """
    Make sure all the transactions have been executed
    """

    global module_prefix

    # Fetch format
    output_format = rule_manager.get_property(None, module_prefix, 'format')

    if output_format == 'CSV':
        flush_csv(event_list)
    elif output_format == 'CAM_DB':
        flush_cam_db(event_list)
    elif output_format == 'mongo':
        flush_mongo_db(event_list)
    else:
        flush_csv(event_list)

################################################################################

def init_csv(module_name):
    """
    Initalize the dump procedure in CSV format.
    """

    global config_parmas
    global output_file
    global csv_hash
    global print_ordinal

    # Reset the set of hashes
    csv_hash = set([])

    # Set the output_file
    if rule_manager.get_property(None, module_name, 'output_file') == '':
        output_file = sys.stdout
    else:
        output_file = codecs.open(config_params['output_file'], 'w',
                                  encoding = 'utf-8')

    # Create the header to print as first line
    header = ["datetime", "type", "user", "application", "invocation",
              "aux1", "aux2"]

    # See if the first column should include the ordinal
    print_ordinal = rule_manager.get_property(None, module_name, \
                                                  'print_ordinal') == 'yes'
    if print_ordinal:
        header.index(0, 'n')

    # Print the first line of the CSV with the column names
    print >> output_file, ','.join(header)

def init_cam_db(module_name):
    """
    Initalize the CAM DM process
    """

    mysql.connect(rule_manager.get_property(None, module_name, 'db_host'),
                  rule_manager.get_property(None, module_name, 'db_user'),
                  rule_manager.get_property(None, module_name, 'db_passwd'),
                  rule_manager.get_property(None, module_name, 'db_name'))

    mysql.cursor_obj.execute('select max(datetime) from event')

def init_mongo_db(module_name):
    """
    Initialize the mongo db process
    """

    print 'To be written'
    sys.exit(1)

################################################################################
def out_csv(event_list):
    """
    Dump the given data in CSV format. See function out

    n,datetime,type,user,application,invocation,[opt1, opt2]

    """

    global module_prefix
    global event_counter
    global output_file
    global csv_hash
    global print_ordinal

    # Get some global variables
    if output_file == None:
        output_file = sys.stdout

    # Get the window date to process events
    (from_date, until_date) = rules_common.window_dates(module_prefix)

    # Loop over all the events in the list
    for event in event_list:
        # Calculate the event hash and see if it exists
        event_hash = hashlib.sha256(unicode(event).encode('utf-8')).hexdigest()

        if event_hash in csv_hash:
            # Event is already in the database, skip
            continue
        csv_hash.add(event_hash)

        # Split the event tuple
        (evname, dt, shl, entity_list) = event

        if dt < from_date or dt > until_date:
            # Ignore event because it is outside the given window
            continue

        event_counter += 1

        # Create the event with the first three entities
        out_line = [unicode(dt),
                    '"' + evname.replace('"', '""') + '"',
                    '"' + entity_list[0][1][0].replace('"', '""') + '"',
                    '"' + unicode(entity_list[1][1][0]).replace('"', '""') + '"',
                    '"' + unicode(entity_list[2][1][0]).replace('"', '""') + '"']

        # Put the event ordinal as the first column
        if print_ordinal:
            out_line.insert(0, unicode(event_counter))

        # Attach any remaining entity
        for entity in entity_list[3:]:
            field = unicode(entity[1][0]).replace('"', '""')
            out_line.append(u'"' + field.strip() + u'"')

        # Dump the line
        print >> output_file, ','.join(out_line)

def out_cam_db(event_list):
    """
    Dump the given data in CAM format. See function out
    """

    global entity_cache
    global module_prefix
    global event_counter

    # Get the window date to process events
    (from_date, until_date) = rules_common.window_dates(module_name)

    # Loop over the list of events
    for event in event_list:
        if event[1] < from_date or event[1] > until_date:
            # Ignore event because it is outside the given window
            continue

        # Calculate the event hash and see if it exists
        event_hash = hashlib.sha256(unicode(event).encode('utf-8')).hexdigest()

        if mysql.find_event_by_hash(event_hash) != None:
            # Event is already in the database, skip
            continue

        # First find if the entities are already present
        entities = []
        entity_list = event[3]
        new_entity = False
        for entity in entity_list:

            # Ask the entity cache
            entity_id = entity_cache.get(tuple(entity[1]))
            is_new = False

            if entity_id == None:
                # find the entity or add it to the database
                (entity_id, is_new) = mysql.find_or_add_entity(entity[1][0],
                                                               entity[1][1],
                                                               entity[1][2],
                                                               entity[1][3],
                                                               entity[1][4])
                entity_cache[tuple(entity[1])] = entity_id

            # accumulate pairs (role, entity_id)
            entities.append((entity[0], entity_id))

            # Detect if any of them is new
            new_entity |= is_new

        # Insert event and eventrelatedentities
        event_id = mysql.insert_event(event[0], event[1], event[2], event_hash)
        data_pack = [(role, event_id, entity_id) \
                         for (role, entity_id) in entities]
        mysql.insert_eventrelatedentity(data_pack)

        event_counter += 1

def out_mongo_db(event_list):
    """
    Dump the given data in mongoDB format. See function out
    """

    global entity_cache
    global module_prefix
    global event_counter

    print 'To be written'
    sys.exit(1)

################################################################################

def print_event(event):
    """
    Function to print an event. The structure of the event:

    (Event_Name, Datetime, SharingLevel,
      [ List of pairs (Role, Entity) ])

    Event_Name: String
    DateTime: datetime
    SharingLevel: String
    Role: String

    And the entities are:
    (id, metadataid, metadataref, mimetype, name)

    All of them strings.
    """

    print str(event[0]), str(event[1]), str(event[2])
    for role, entity in event[3]:
        print '  ',
        print str(role) + ': ' + str(entity[0]), str(entity[1]), str(entity[2]),
        print str(entity[3]), str(entity[4])


def main():
    """
    Function that receives a list of elements, each of them representing an
    event and outpus them in the format based on the value of the variable
    output_format.
    """

    global debug

    #######################################################################
    #
    # OPTIONS
    #
    #######################################################################
    # Swallow the options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "d", [])
    except getopt.GetoptError, e:
        print >> sys.stderr, 'Incorrect option.'
        print >> sys.stderr, main.__doc__
        sys.exit(2)

    # Parse the options
    for optstr, value in opts:
        # Debug option
        if optstr == "-d":
            debug = 1

    # Check that there are additional arguments
    if len(args) == 0:
        print >> sys.stderr, 'Script needs additional parameters'
        sys.exit(1)

    if debug:
        print >> sys.stderr, 'Options: ', args

    #######################################################################
    #
    # MAIN PROCESSING
    #
    #######################################################################

    print 'Script not prepared to be executed independently'

# Execution as script
if __name__ == "__main__":
    main()

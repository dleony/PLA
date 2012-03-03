#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Module to dump events in different formats. 
#
# The events are received as a list of pairs (key, value) with the following
# structure
#
# [ ('name', <event name>),    # Mandatory!
#   ('datetime', <date/time>), # Mandatory!
#   ('user', <user id>),       # Mandatory!
#   .....                    Additional pairs key,value
# ]
#
#    datetime is a datetime object, the rest strings.
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
                          # One of: "CSV", "mongo"
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


    The events are received as a list of pairs (key, value) with the following
    structure:

    [ ('name', <event name>),    # Mandatory!
      ('datetime', <date/time>), # Mandatory!
      ('user', <user id>),       # Mandatory!
      .....                    Additional pairs key,value
    ]

    datetime is a datetime object, the rest strings.
    """

    global module_prefix
    global output_format
    global exclude_users

    # Check that the event has the minimum amount of info
    incorrect_event = next((x for x in event_list \
                                if x[0][0] != 'name' or \
                                   x[1][0] != 'datetime' or \
                                   x[2][0] != 'user'), None)
    if incorrect_event:
        print >> sys.stderr, 'Incorrect event detected:'
        print_event(incorrect_event)
        sys.exit(1)

    # Loop over all the events and print them
    # for event in event_list:
    #     print_event(event)

    # Filter those events that have a user in the exclude_users list
    event_list = [x for x in event_list if not x[2][1] in exclude_users]

    if output_format == 'CSV':
        out_csv(event_list)
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

        if len(event) > 7:
            print >> sys.stderr, "Event longer than expected."
            print >> sys.stderr, print_event(event)
            sys.exit(1)

        # Calculate the event hash and see if it exists
        event_hash = hashlib.sha256(unicode(event).encode('utf-8')).hexdigest()

        if event_hash in csv_hash:
            # Event is already in the database, skip
            continue
        csv_hash.add(event_hash)

        dt = event[1][1] 
        # Ignore event because if outside the given window
        if dt < from_date or dt > until_date:
            continue

        event_counter += 1

        # Create the event with the first three entities and add the rest
        out_line = [unicode(dt),
                    '"' + event[0][1].replace('"', '""') + '"',
                    '"' + event[2][1].replace('"', '""') + '"']

        # Put the event ordinal as the first column
        if print_ordinal:
            out_line.insert(0, unicode(event_counter))

        # Attach any remaining entities
        for entity in event[3:]:
            field = unicode(entity[1]).replace('"', '""')
            out_line.append(u'"' + field.strip() + u'"')

        # Dump the line
        print >> output_file, ','.join(out_line)

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
    Function to print an event. Events are a list of pairs (key, value) with the
    following structure:

    [ ('name', <event name>),    # Mandatory!
      ('datetime', <date/time>), # Mandatory!
      ('user', <user id>),       # Mandatory!
      .....                    Additional pairs key,value
    ]

    datetime is a datetime object, the rest strings.
    """

    print str(event[1][1]), str(event[0][1]), str(event[2][1])
    
    for element in events[3:]:
        print '  ',
        print str(element[0]) + ': ' + str(event[element[1]])

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

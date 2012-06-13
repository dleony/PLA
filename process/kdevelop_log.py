#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import sys, locale, codecs, getopt, os, glob, re, datetime

import detect_new_files, rules_common, rule_manager, event_output, anonymize
import process_filters

#
# See update_events and event_output for the structure of the events
#

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
module_prefix = 'kdevelop_log'

#
# Configuration parameters for this module
#
config_params = {
    'files': '',          # Files to process
    'filter_file': '',    # File containing a function to filter events
    'filter_function': '' # Function to use to filter
    }

filter_function = None

debug = 0

def initialize(module_name):
    """
    Initialization function. Must be here always.

    """

    global filter_function
    global debug
    
    # Get the level of debug
    debug = int(rule_manager.get_property(None, module_name, 'debug'))

    filter_function = process_filters.initialize_filter(module_name)

    return

def execute(module_name):
    """
    Given a list of files with kdevelop logs, process all of them. 

    Process the files containing the events. Return True if no error is
    detected. The event have the form of:

0 2011-10-05 18:30:02 2011-10-05 18:30:15 '/usr/bin/kdevelop' 'numero.dat'

    Where the fields are:
      - status
      - start date/time
      - stop date/time
      - program name
      - file (optional)

    [('name', 'ide'), 
     ('datetime', dtime),
     ('user', anonymize(user)),
     ('application', 'kdevelop'),
     ('invocation', command)]

    """

    global filter_function

    # Get the window date to process events
    (from_date, until_date) = rules_common.window_dates(module_name)

    # Get the files to process, lines and mark lines
    (files, total_lines, mark_lines) = \
        rules_common.files_to_process(module_name)

    # Loop over all the files
    total_counter = 0
    for file_annotation in files:
        # Get the file name and (if it exists, the date of the last event)
        filename = file_annotation[0]
        last_event = datetime.datetime.min
        if len(file_annotation[1]) != 0:
            last_event = datetime.datetime.strptime(file_annotation[1][0], 
                                                        '%Y-%m-%d %H:%M:%S')
        new_last_event = last_event

        # Get the user id from the path to the file name
        user_id = filename.split('/')[-2]
        anon_user_id = anonymize.find_or_encode_string(user_id)

        data_in = codecs.open(filename, 'r', 'utf-8', errors = 'replace')

        line_number = 0
        for line in data_in:
            line_number += 1
            total_counter += 1
            
            if total_counter % mark_lines == 0:
                print >> sys.stderr, '+',
                sys.stderr.flush()

            # See if the user id appears in the command, if so, anonymize
            if re.search(user_id, line):
                line = re.sub(user_id, anon_user_id, line)

            # Chop the command line
            fields = line[:-1].split()

            # If something weird happened and there are no fields, ignoredumpt
            # the line
            if len(fields) < 6:
                print >> sys.stderr, 'WARNING: In file', filename
                print >> sys.stderr, 'Ignoring:', line
                continue

            try:
                dtime = datetime.datetime.strptime(' '.join(fields[1:3]).strip(), 
                                                   '%Y-%m-%d %H:%M:%S')
            except ValueError, e:
                print >> sys.stderr, 'WARNING: In file', filename
                print >> sys.stderr, 'Ignoring:', line
                continue
            
            if dtime <= last_event:
                # Event is older than what has been recorded in the
                # detect_new_files. skip
                continue

            if dtime < from_date or dtime > until_date:
                # Ignore event because it is outside the given window
                continue
            
            # If there is a filter function and returns None, skip this event
            if filter_function != None and filter_function(fields) == None:
                continue

            # Record the event with the highest datetime.
            if dtime > new_last_event:
                new_last_event = dtime

            cmd = fields[5][1:-1]
            if len(fields) == 7:
                cmd = cmd + ' ' + fields[6][1:-1]

            event = ('ide', dtime, anon_user_id,
                     [('program', 'kdevelop'), ('command', cmd)])

            try:
                event_output.out(event)
            except Exception, e:
                print 'Exception while processing', filename, ':', line_number
                print str(e)
                sys.exit(1)
            
        data_in.close()
        detect_new_files.update(None, module_name + '//' + filename, 
                                [new_last_event])

    print >> sys.stderr

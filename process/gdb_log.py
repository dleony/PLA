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
module_prefix = 'gdb_log'

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
    Given a list of files with gdb logs, process all of them. 

    Process the files containing the events. Return True if no error is
    detected. The event have the form of:

-BEGIN 2011-10-14 11:09:29 2011-10-14 11:15:06 /usr/bin/gdb prueba
r
where
q
-END

    [('name', 'debugger'), 
     ('datetime', dtime),
     ('user', anonymize(user)),
     ('application', 'gdb'),
     ('invocation, command),
     ('session_cmds', session commands),
     ('session_end', datime when session ended)]

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
        session_cmds = []
        for line in data_in:
            line_number += 1
            total_counter += 1
            
            if total_counter % mark_lines == 0:
                print >> sys.stderr, '+',
                sys.stderr.flush()

            # Skip the empty lines
            if line == '\n':
                continue

            # See if the user id appears in the command, if so, anonymize
            if re.search(user_id, line):
                line = re.sub(user_id, anon_user_id, line)

            # Chop the command line
            fields = line[:-1].split()

            # Beginning of log. Catch command invocation and dates
            if re.match('^\-BEGIN .+$', line):
                fields = line.split()
                try:
                    dtime = datetime.datetime.strptime(' '.join(fields[1:3]),
                                                           '%Y-%m-%d %H:%M:%S')
                    session_end = datetime.datetime.strptime(' '.join(fields[3:5]),
                                                             '%Y-%m-%d %H:%M:%S')
                    
                except ValueError, e:
                    print >> sys.stderr, 'WARNING: In file', filename
                    print >> sys.stderr, 'Ignoring:', line

                command = ' '.join(fields[5:])
                session_cmds = []
                continue

            # If not the end of an event, concatenate the line and keep looping
            if not re.match('^\-END$', line):
                session_cmds.append(line[:-1])
                continue
                
            # At this point we have the complete information about the event
            
            if dtime <= last_event:
                # Event is older than what has been recorded in the
                # detect_new_files. skip
                continue

            # If out of time window, ignore
            if dtime < from_date or dtime > until_date:
                continue
            
            # If there is a filter function and returns None, skip this event
            if filter_function != None and filter_function(fields) == None:
                continue

            # Record the event with the highest datetime.
            if dtime > new_last_event:
                new_last_event = dtime

            event = ('gdb', dtime, anon_user_id,
                     [('program', 'gdb'),
                      ('command', command),
                      ('session_cmds',  '"' + ';'.join(session_cmds) + '"'),
                      ('session_end', session_end)])

            try:
                event_output.out(event)
            except Exception, e:
                print 'Exception while processing', filename, ':', line_number
                print str(e)
                sys.exit(1)
            
        data_in.close()
        detect_new_files.update(None, filename, [new_last_event])

    print >> sys.stderr

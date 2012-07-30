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
module_prefix = 'svn_apache'

#
# Configuration parameters for this module
#
config_params = {
    'files': '',           # Files to process
    'filter_file': '',     # File containing a function to filter events
    'filter_function': '', # Function to use to filter
    'msg_length': '256',   # Maximum message size stored
    'process_commits': ''  # Boolean. If '', process commits, otherwise,
                           # ignore them
    }

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
    Given a list of files with svn apache logs, process all of them. The logs are produced with the following apache configuration commands:

    CustomLog [destionation log file]  "%t %u %{SVN-REPOS-NAME}e %{SVN-ACTION}e" env=SVN-ACTION

Sample:

    [03/Mar/2012:11:43:55 +0100] abel asteams-en update /Teams/Team_09 r3960 send-copyfrom-args

    For each line in the file, the following event structure is produced

    [('name', 'svn_' + svn_action), # Svn action is update, diff, etc. 
     ('datetime', dtime),
     ('user', anonymize(user)),
     ('repository', repository name),
     ('directory', directory) (optional), 
     ('revision', r??? (optional)),
     ('comment', (max 256 chars)) # Only if commit and repository given]
    """

    global filter_function

    # Get the level of debug
    debug = int(rule_manager.get_property(None, module_name, 'debug'))

    # Get the window date to process events
    (from_date, until_date) = rules_common.window_dates(module_name)

    # Get the files to process, lines and mark lines
    (files, total_lines, mark_lines) = \
        rules_common.files_to_process(module_name)

    # Get the flag to see if the commits need to be processed
    process_commits = rule_manager.get_property(None, module_name, 
                                               'process_commits') == ''

    # Loop over all the given args
    total_counter = 0
    for file_annotation in files:
        # Get the file name and (if it exists, the date of the last event)
        filename = file_annotation[0]
        last_event = datetime.datetime.min
        if len(file_annotation[1]) != 0:
            last_event = datetime.datetime.strptime(file_annotation[1][0], 
                                                        '%Y-%m-%d %H:%M:%S')
        new_last_event = last_event

        data_in = codecs.open(filename, 'r', encoding = 'utf8', 
                              errors = 'replace')
        old = ''
        counter = 0
        for line in data_in:

            # Advance counters and print progress character if needed
            counter += 1
            total_counter += 1
            if total_counter % mark_lines == 0:
                print >> sys.stderr, '+',
                sys.stderr.flush()

            # Chop line into fields
            line = line[:-1]
            fields = line.split()
            if len(fields) < 3:
                raise ValueError('Erroneous log line:' + line)

            # Get the event type to quickly detect if we need to skip it
            event_type = fields[4]
            if (not process_commits) and event_type == 'commit':
                continue;

            # Translate date time of the event and check if within process
            # interval
            dtime = datetime.datetime.strptime(fields[0][1:], 
                                               '%d/%b/%Y:%H:%M:%S')
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

            # Create the first three pairs of the event
            event = ('svn_' + event_type, dtime, anonymize.find_or_encode_string(fields[2]),
                     [('repository', fields[3])])


            # Structure of the different events
            # 
            # checkout-or-export /path r62 depth=infinity
            # commit harry r100
            # diff /path r15:20 depth=infinity ignore-ancestry
            # get-dir /trunk r17 text
            # get-file /path r20 props
            # get-file-revs /path r12:15 include-merged-revisions
            # get-mergeinfo (/path1 /path2)
            # lock /path steal
            # log (/path1,/path2) r20:90 discover-changed-paths revprops=()
            # replay /path r19
            # change-rev-prop r50 propertyname
            # rev-proplist r34
            # status /path r62 depth=infinity
            # switch /pathA /pathB@50 depth=infinity
            # unlock /path break
            # update /path r17 send-copyfrom-args
            if event_type == 'checkout-or-export':
                event[3].append(('revision', fields[6]))
                event[3].append(('location', fields[5]))
            if event_type == 'commit':
                event[3].append(('revision', fields[5]))
                # Fetch the log message if svn_client is not None
                if svn_client != None:
                    pass
            elif event_type == 'diff':
                event[3].append(('location', fields[5] + ' ' + fields[6]))
            elif event_type == 'get-dir' or event_type == 'get-file' or \
                    event_type == 'update':
                event[3].append(('revision', fields[6]))
                event[3].append(('location', fields[5]))
            elif event_type == 'get-file-revs':
                event[3].append(('revision', 'r' + fields[6].split(':')[1]))
                event[3].append(('location', fields[5]))
            elif event_type == 'lock' or event_type == 'unlock':
                event[3].append(('location', fields[5]))
            elif event_type == 'log':
                event[3].append(('location', fields[5]))

            event_output.out(event)

        data_in.close()
        detect_new_files.update(None, module_name + '//' + filename, 
                                [new_last_event])

    print >> sys.stderr

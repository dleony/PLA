#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import sys, locale, codecs, getopt, os, glob, re, datetime, calendar, pysvn
import fnmatch

import rules_common, rule_manager, event_output, anonymize, process_filters

#
# See update_events for the structure of the events
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
module_prefix = 'svn_log'

#
# Configuration parameters for this module
#
config_params = {
    'repository': '',      # Repository to process
    'repository_name': '', # String to identify the repository
    'files': '',           # Files to process
    'filter_file': '',     # File containing a function to filter events
    'filter_function': '', # Function to use to filter
    'from_date': '',       # Date from which to process events
    'until_date': '',      # Date until which to process events
    'msg_length': '256'    # Maximum message size stored
    }

# Additional global vars to be used
svn_client = None

# Comments to detect special events to be labeled differently from the rest.
svn_special_event_comment = [ 'PLA Automatic commit' ]
svn_special_event_names =   [ 'svn_pla_commit' ]

def initialize(module_name):
    """
    Initialization function. Must be here always.

    """

    global svn_client
    global filter_function
    global debug
    
    # Get the level of debug
    debug = int(rule_manager.get_property(None, module_name, 'debug'))

    filter_function = process_filters.initialize_filter(module_name)

    svn_client = pysvn.Client()
    svn_client.exception_style = 1

    return

def execute(module_name):
    """
    Process the files contained in the given repository.

    [('name', 'svn_commit'), 
     ('datetime', dtime),
     ('user', anonymize(user)),
     ('program', 'svn'),
     ('repository', repository name),
     ('comment', (max 256 chars))]
    """

    global svn_client
    global filter_function
    global svn_special_event_comment
    global svn_special_event_names

    # Get the level of debug
    debug = int(rule_manager.get_property(None, module_name, 'debug'))

    repository = rule_manager.get_property(None, module_name, 'repository')
    if repository == '':
        # No data available, no initialization done
        return

    repository_root = \
        svn_client.info2(repository, 
                         depth = pysvn.depth.empty)[0][1]['repos_root_URL']

    repository_name = rule_manager.get_property(None, module_name, 
                                                'repository_name')

    # Fetch all the files in the given repository
    dir_info = svn_client.list(repository, depth = pysvn.depth.immediates)

    # Select only those that are directories and match the given expression
    dir_info = [x[0]['repos_path'][1:] for x in dir_info \
                    if x[0]['kind'] == pysvn.node_kind.dir]
    source_dirs = fnmatch.filter(dir_info, 
                                 rule_manager.get_property(None, module_name, 
                                                           'files'))

    # Dump the dirs being processed
    if debug != 0:
        print >> sys.stderr, len(source_dirs), 'svndirs being processed.'

    # Get the window date to process events
    (from_date, until_date) = rules_common.window_dates(module_name)

    # Set the date/times to ask for the logs
    if from_date != None:
        seconds = calendar.timegm(from_date.utctimetuple())
        revision_start = pysvn.Revision(pysvn.opt_revision_kind.date,
                                        seconds)
    else:
        revision_start = pysvn.Revision(pysvn.opt_revision_kind.head)

    if until_date != None:
        seconds = calendar.timegm(until_date.utctimetuple())
        revision_end = pysvn.Revision(pysvn.opt_revision_kind.date,
                                      seconds)
    else:
        revision_end = pysvn.Revision(pysvn.opt_revision_kind.number, 0)

    msg_size = int(rule_manager.get_property(None, module_name, 'msg_length'))
    # Loop over the directories and collect al the logs
    all_logs = []
    for directory_name in source_dirs:

        # Slurp al the logs in the server
        all_logs.extend(svn_client.log(os.path.join(repository_root, 
                                                    directory_name),
                                       revision_start = revision_start,
                                       revision_end = revision_end))
        
    # Dump the dirs being processed
    if debug != 0:
        print >> sys.stderr, len(all_logs), 'logs being processed.'

    # Loop over all the log elements
    total_counter = 0
    mark_lines = len(all_logs) / 40 + 1
    for log_data in all_logs:

        # Count the logs to print the mark string on the screen
        total_counter += 1
        if total_counter % mark_lines == 0:
            print >> sys.stderr, '+',
            sys.stderr.flush()

        # Fetch the three important fields, author, date/time and msg
        anon_user_id = anonymize.find_or_encode_string(log_data['author'])
        dtime = datetime.datetime.fromtimestamp(log_data['date'])

        # How can be a substring of a specific length be obtained?
        msg = unicode(log_data['message'], 'utf-8')
        # This subsetting needs to be done after encoding to make sure the
        # string is broken in a safe location (and not in the mid of a utf-8
        # character).
        msg = msg[:msg_size]

        if dtime < from_date or dtime > until_date:
            # Ignore event because it is outside the given window
            continue
        
        # If there is a filter function and returns None, skip this event
        if filter_function != None and \
                filter_function([log_data['author'],
                                 log_data['date'],
                                 log_data['message']]) == None:
            continue

        try:
            special_idx = svn_special_event_comment.index(msg)
            event_name = svn_special_event_names[special_idx]
        except ValueError, e:
            event_name = 'svn_commit'

        event = [('name', event_name),
                 ('datetime', dtime),
                 ('user', anon_user_id),
                 ('program', 'svn'), 
                 ('repository', repository_name), 
                 ('comment', msg)]

        try:
            event_output.out([event])
        except Exception, e:
            print 'Exception while processing', module_name
            print str(e)
            sys.exit(1)
            
        
    print >> sys.stderr

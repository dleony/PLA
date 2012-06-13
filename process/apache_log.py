#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Script to process a CSV file with events downloaded from Apache
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
module_prefix = 'apache_log'

#
# Configuration parameters for this module
#
config_params = {
    'files': '',          # Files to process
    'filter_file': '',    # File containing a function to filter events
    'filter_function': '' # Function to use to filter
    }

# clf_re = re.compile(r'\s+'.join([
#             r'(?P<host>\S+)',                   # host %h
#             r'\S+',                             # indent %l (unused)
#             r'(?P<user>\S+)',                   # user %u
#             r'\[(?P<time>.+)\]',                # time %t
#             r'"(?P<request>.+)"',               # request "%r"
#             r'(?P<status>[0-9]+)',              # status %>s
#             r'(?P<size>\S+)']) + r'\s*\Z')

clf_re = re.compile(r'(\d+\.\d+\.\d+\.\d+)\s+(\S+)\s+(.+)\s+\[(.+)\] "(.+)" (\d+) (.*)')

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
    Given a list of files with Apache logs, process all of them. 

    [('name', 'visit_url'),
     ('datetime', dtime),
     ('user', anonymize(user)),
     ('application', browser?),
     ('url', URL), 
     ('ip', IP)]
            
    """

    global clf_re
    global filter_function

    # Get the window date to process events
    (from_date, until_date) = rules_common.window_dates(module_name)

    # Get the files to process, lines and mark lines
    (files, total_lines, mark_lines) = \
        rules_common.files_to_process(module_name)

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

            counter += 1
            total_counter += 1
            
            if total_counter % mark_lines == 0:
                print >> sys.stderr, '+',
                sys.stderr.flush()

            line = line[:-1]
            
            fields = clf_re.match(line).groups()

            if fields[2] == '':
                raise ValueError('Empty string' + line)

            # Translate date time of the event
            dtime = datetime.datetime.strptime(fields[3].strip()[:-6], 
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

            (method, url, protocol) = fields[4].split()

            event = ('visit_url', dtime,
                     anonymize.find_or_encode_string(fields[2]),
                     [('application', 'unknown'), 
                      ('url', url),
                      ('ip', fields[0])])
            
            event_output.out(event)

        data_in.close()
        detect_new_files.update(None, module_name + '//' + filename, 
                                [new_last_event])

    print >> sys.stderr

def main():
    """
    Script that given a file with CSV Apache events, it process them and
    prepares to be dumped.

    script [options] logfile logfile ...

    Options:

    -d Boolean to turn in debugging info

    Example:

    script -d srcFolder/f*

    The type of lines considered by this script are of the form (Common Log Format)

    
    IP - USER [DATETIME] "GET /CSS/as.css HTTP/1.1" 200 13089

    Some stats (operations, events, eventrelatedentity, relatedentity, time)

    Import from scratch: 101267, 303801, 7531 2m 19s
    Re-import same data:   -       -       -  
    """

    global config_params
    global module_prefix
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
            config_params['debug'] = '1'
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
    incorrect_file = next((x for x in args if not(os.path.isfile(x))), None)
    if incorrect_file != None:
        print >> sys.stderr, 'File', incorrect_file, 'not found'
        sys.exit(1)

    config_params['files'] = ' '.join(args)

    rule_manager.options = rule_manager.initial_config({})
    section_name = module_prefix
    try:
        rule_manager.options.add_section(section_name)
    except ConfigParser.DuplicateSectionError:
        pass

    for (vn, vv) in config_params.items():
        rule_manager.set_property(None, section_name, vn, vv,
                                  createRule = True, createOption = True)

    initialize(module_prefix)

    execute(module_prefix)

# Execution as script
if __name__ == "__main__":
    main()

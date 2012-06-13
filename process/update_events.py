#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Script to compute tf-idf for a set of documents
#
import sys, locale, codecs, getopt, os, ConfigParser

# Manage options
import rule_manager

# Manage file modification files
import detect_new_files

# Modules
import ldap_lookup, anonymize, event_output, moodle_log
import apache_log, vm_log, bash_log, firefox_log, kate_log, kdevelop_log
import gdb_log, gcc_log, valgrind_log, svn_log, svn_apache, embeddedq_log

#
# Here are the type of events that are considered. Each event has three fixed
# fields: name, datetime, and user. The rest of fields depend on the event.
#
# ----------------+-------------------------------------------------------------
# event name      | 
# ----------------+-------------------------------------------------------------
# lms_*           |(application, moodle), (community, Community), (ip, IP)
#                 |(resource, resource name)
# ----------------+-------------------------------------------------------------
# visit_url       |(application, browser|unknown), (url, URL), (ip, IP) (opt)
# ----------------+-------------------------------------------------------------
# bashcmd         |(program, program), (command, command)
# ----------------+-------------------------------------------------------------
# text_editor     |(program, kate), (command, command)
# ----------------+-------------------------------------------------------------
# ide (s/e)       |(program, kdevelop), (command, command)
# ----------------+-------------------------------------------------------------
# debugger        |(program, gdb), (command, command), 
#                 |(session_cmds, session commands (one per line)),
#                 |(session_duration, session duration in seconds)
# ----------------+-------------------------------------------------------------
# compile         |(program, gcc), (command, command), 
#                 |(messages, Message extract (optional))
# ----------------+-------------------------------------------------------------
# memory_profiler |(program, valgrind), (command, command), 
#                 |(messages, Message extract (optional))
# ----------------+-------------------------------------------------------------
# embedded_???    |
#                 |
# ----------------+-------------------------------------------------------------

# To REWRITE
#
# svn_commit      |(program, svn), (revision, revision), 
#                 |(comment, Message (up to 256 chars))
# ----------------+-------------------------------------------------------------
# svn_pla_commit  |(program, svn), (revision, revision), 
#                 |(comment, Message (up to 256 chars))
# ----------------+--------------------------------------------------------
#
# How about SVN events taken from Apache? Include!!!
#
#
# Fix the output encoding when redirecting stdout
if sys.stdout.encoding is None:
    (lang, enc) = locale.getdefaultlocale()
    if enc is not None:
        (e, d, sr, sw) = codecs.lookup(enc)
        # sw will encode Unicode data to the locale-specific character set.
        sys.stdout = sw(sys.stdout)

config_defaults = {
    'debug': '0',
    'file_modification_cache': '',
    'from_date': '',       # Date from which to process events
    'until_date': ''       # Date until which to process events
    }

modules = ['ldap_lookup', 'anonymize', 'event_output', 'moodle_log', 
           'apache_log', 'vm_log', 'bash_log', 'firefox_log', 'kate_log',
           'kdevelop_log', 'gdb_log', 'gcc_log', 'valgrind_log', 'svn_log',
           'svn_apache', 'embeddedq_log']

def load_defaults(configuration):
    """
    Traverse the list of modules and incorporate the default options:

    """
    global modules

    # Loop over all the modules
    for module_name in modules:
        section_name = eval(module_name + '.module_prefix')
        config_params = eval(module_name + '.config_params')

        try:
            configuration.add_section(section_name)
        except ConfigParser.DuplicateSectionError:
            pass

        for (vn, vv) in config_params.items():
            rule_manager.set_property(configuration, section_name, vn, vv,
                                      createRule = True, createOption = True)

    return

def main():
    """
    Read a configuration file and perform the different event updates. A list of
    the modules to execute can be given.

    script configfile [module module ...]

    Example:

    script update_events.cfg moodle_log apache_log

    """

    global config_defaults

    #######################################################################
    #
    # OPTIONS
    #
    #######################################################################
    args = sys.argv[1:]

    # Check that there are additional arguments
    if len(args) < 1:
        print >> sys.stderr, 'Script needs at least one parameter'
        sys.exit(1)

    if not os.path.exists(args[0]):
        print >> sys.stderr, 'File', args[0], 'not found.'
        sys.exit(1)

    # Initial options included in the global dictionary at the top of the
    # module.
    rule_manager.options = rule_manager.initial_config(config_defaults)

    # Traverse the modules and load the default values
    load_defaults(rule_manager.options)

    # Load the rules in the given configuration file
    rules = rule_manager.load_config_file(None, args[0], {})[1]

    # Initialize the file modification cache mechanism
    detect_new_files.initialize(\
        rule_manager.get_property(None, 
                                  'anonymize', 
                                  'file_modification_cache'), 
        True)

    # Traverse the sections and run first the "initialize" function and then
    # the "execute" function.
    for module_name in rules:
        module_prefix = module_name.split('.')[0]
        getattr(sys.modules[module_prefix], 'initialize')(module_name)

    for module_name in rules:
        module_prefix = module_name.split('.')[0]
        print >> sys.stderr, '### Execute' , module_name
        getattr(sys.modules[module_prefix], 'execute')(module_name)

    return

# Execution as script
if __name__ == "__main__":
    main()

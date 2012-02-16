#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import os, sys, tarfile, getopt, pysvn, re, fnmatch, codecs, locale, shutil
import datetime, glob

import rule_manager, event_output, anonymize, process_filters

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
module_prefix = 'vm_log'

#
# Configuration parameters for this module
#
config_params = {
    'repository': '',            # URL of the SVN repository
    'files': '',                 # Files to process
    'dst_dir': 'DEFAULT_DST_DIR' # Directory where to unpack the data
    }

# OPTIONS TO BE CONSIDERED
# username

# Additional global vars to be used
svn_client = None

def initialize(module_name):
    """
    Initialization function. Must be here always.
    """

    global svn_client
    global debug

    svn_client = pysvn.Client()
    svn_client.exception_style = 1 

    return

def execute(module_name):
    """
    Given a list of directories with vm logs, process all of them.
    """

    global svn_client

    # Get the level of debug
    debug = int(rule_manager.get_property(None, module_name, 'debug'))

    repository = rule_manager.get_property(None, module_name, 'repository')
    if repository == '':
        # No data available, no initialization done
        return

    repository_root = \
        svn_client.info2(repository, 
                         depth = pysvn.depth.empty)[0][1]['repos_root_URL']

    # Fetch all the files in the given repository
    dir_info = svn_client.list(repository, depth = pysvn.depth.immediates)

    # Select only those that are directories and match the given expression
    dir_info = [x[0]['repos_path'][1:] for x in dir_info \
                    if x[0]['kind'] == pysvn.node_kind.dir]
    source_dirs = fnmatch.filter(dir_info, 
                                 rule_manager.get_property(None, module_name, 
                                                           'files'))

    dst_dir = rule_manager.get_property(None, module_name, 'dst_dir')
    if dst_dir == '':
        print >> sys.stderr, 'VM_Logs: dst_dir is empty.'
        sys.exit(1)

    if not(os.path.exists(dst_dir)):
        os.makedirs(dst_dir)

    # Loop over all the directories
    for directory_name in source_dirs:
        # Calculate the dst full name
        (head, dst_tail) = os.path.split(directory_name)
        dst_full_name = os.path.join(dst_dir, dst_tail)

        # Fetch all the files in the given repository
        file_info = svn_client.list(os.path.join(repository_root, directory_name,
                                                 '.pladata'),
                                    depth = pysvn.depth.immediates)

        # Select only those that are directories and match the *.tgz pattern
        file_info = [x[0]['repos_path'][1:] for x in file_info \
                        if x[0]['kind'] == pysvn.node_kind.file]
        data_files = [x for x in file_info if re.search('[0-9]+_[0-9]+\.tgz$',
                                                       x)]

        if debug != 0:
            print >> sys.stderr, '  Dir', dst_tail, ':', len(data_files), 'files'

        # Loop over all the data files
        for data_file in data_files:
            # Separate file name from dir name
            (head_dir, file_name) = os.path.split(data_file)

            # Obtain the author that did the commit
            data_info = svn_client.info2(os.path.join(repository_root, 
                                                      data_file),
                                         depth = pysvn.depth.empty)
            author_id = data_info[0][1]['last_changed_author']

            # Create the path to the author dir and additional dirs if needed
            dst_author_dir = os.path.join(dst_full_name, author_id)
            if not os.path.exists(dst_author_dir):
                os.makedirs(dst_author_dir)
            dst_file = os.path.join(dst_author_dir, file_name)
            done_author_dir = os.path.join(dst_full_name, author_id, 'tgzs')
            if not os.path.exists(done_author_dir):
                os.makedirs(done_author_dir)

            # If the file has NOT been unpacked already, process it
            if os.path.exists(os.path.join(done_author_dir, file_name)):
                continue

            # Get a copy of the *.tgz from the repository with the export
            # command
            try:
                svn_client.export(os.path.join(repository_root, data_file),
                                  dst_file,
                                  recurse = False)
            except Exception, e:
                print >> sys.stderr, 'Error while exporting', data_file
                print >> sys.stderr, str(e)

            # Expand the data in the tar
            if unpack_tgz_file(dst_file, done_author_dir):
                print >> sys.stderr, 'Error while unpacking', data_file
                continue

            if debug != 0:
                print >> sys.stderr, '    ', dst_tail, 'expanded.'

################################################################################

def unpack_tgz_file(tar_file, done_dir):
    """
    Given a TGZ file unpack its content in a folder with name equal to the
    basename of the given file.

    If the file is foo.tgz, its content is expanded in the folder foo.

    returns True if there were any anomaly, False otherwise
    """

    tar_data = tarfile.open(tar_file, mode = 'r')
    tar_dst = os.path.splitext(tar_file)[0]

    # If dst exists, it is due to some previous error, nuke it
    if os.path.exists(tar_dst):
        shutil.rmtree(tar_dst)

    # Extract only those files that do not start with / and have no ".." in
    # their path.
    members = [m for m in tar_data.getmembers() \
                   if (not m.name.startswith('/')) and \
                   (m.name.find('../') == -1)]
    
    try:
        tar_data.extractall(tar_dst, members)
    except:
        print >> sys.stderr, 'Error while unpacking', tar_file
        return True

    if os.path.exists(os.path.join(tar_dst, 'home', 'teleco')):
        # Move all the data from tar_dst/home/teleco to tar_dst and remove home
        expanded_files = os.listdir(os.path.join(tar_dst, 'home', 'teleco'))
        
        map(lambda x: shutil.move(os.path.join(tar_dst, 'home', 'teleco', x),
                                  tar_dst), expanded_files)
        shutil.rmtree(os.path.join(tar_dst, 'home'))

        # Move all data in .pladata/tools/*/* to tar_dst/..
        tool_dir = os.path.join(tar_dst, '.pladata', 'tools')
        if os.path.exists(tool_dir):
            data_dirs = [d for d in os.listdir(tool_dir) 
                         if os.path.isdir(os.path.join(tool_dir, d))]
            for data_dir in data_dirs:
                data_files = os.listdir(os.path.join(tool_dir, data_dir))
                map(lambda x: shutil.move(os.path.join(tool_dir, data_dir, x), 
                                          os.path.join(tar_dst, '..', x)), 
                    data_files)

            shutil.rmtree(os.path.join(tar_dst, '.pladata'))

    # Rename .bash_* files to bash_* files
    map(lambda x: shutil.move(x, 
                              os.path.join(tar_dst, '..', 
                                           os.path.basename(x)[1:])),
        glob.glob(os.path.join(tar_dst, '.bash_history_*')))

    # Move tar file to user directory to mark it as "expanded"
    shutil.move(tar_file, done_dir)
    # Remove only if it exists, we could've gotten an empty tgz
    if os.path.exists(tar_dst):
        shutil.rmtree(tar_dst)

    return False

def main():
    """
    Script that given a URL of a SVN repository process the directories with VM
    data.

    script [options] dir dir ...

    Options:

    -d Boolean to turn in debugging info

    Example:

    script -d svn_root/Team_1 svn_root/Team_2

    """

    global config_params
    global module_prefix

    #######################################################################
    #
    # OPTIONS
    #
    #######################################################################
    debug = False

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
            debug = True

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
    config_params['files'] = ' '.join(args)

    config_params['repository'] = 'file://' + \
        os.path.abspath('../Sources/VM_Pairs/as/Groups')

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


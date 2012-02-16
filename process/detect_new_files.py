#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Copyright (C) 2010 Carlos III University of Madrid
# This file is part of PLA: Personal Learing Analytics

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
# Implements a module that detects if files have changed from the last time they
# were processed. The module uses a variable to identify a file storing the file
# names and date/time when the last modification was made. Applications then may
# ask if a file changed from the last time it was considered.
#
# This script is useful for applications that process large amounts of files
# through regular invocations. This module allows to remember when was the last
# time a file was processed and process only those that have changed.
# 
# Additionally, the file returns a tuple with additional values stored for the
# file that might be used for other purposes.

import sys, locale, codecs, getopt, os, atexit

# Fix the output encoding when redirecting stdout
if sys.stdout.encoding is None:
    (lang, enc) = locale.getdefaultlocale()
    if enc is not None:
        (e, d, sr, sw) = codecs.lookup(enc)
        # sw will encode Unicode data to the locale-specific character set.
        sys.stdout = sw(sys.stdout)

file_data = None # Dictionary to store pairs filename: (modification time, other
                 # data, ...)

persistent_file_name = None # File where to store the dictionary upon exit

minimum_difference = 0.005 # Minimum value for the difference to rule as modified

def save_file_data():
    """
    Save the information in the global file_data to the persistent_file_name
    """

    global file_data
    global persistent_file_name
    
    # If no persisten file given, nothing to do
    if persistent_file_name == None or file_data == None:
        return

    data_out = codecs.open(persistent_file_name, 'w', 'utf-8')
    for k, v in file_data.items():
        print >> data_out, k + ',' + ','.join(map(lambda x: str(x), list(v)))

    data_out.close()
    return

#
# Program the saving of the data in the dictionary
#
atexit.register(save_file_data);

def initialize(file_name, create = None):
    """
    Loads the information contained in the given file name as the initial values
    of the file detection mechanism. If the file does not exist and create is
    True, the file is then created. If not, an exception is produced.
    """
    
    global file_data
    global persistent_file_name

    if file_name == '':
	return

    persistent_file_name = file_name
    file_data = {}

    # If file does not exist, either create or raise exception
    if not os.path.exists(file_name):
        if create == True:
            data_out = codecs.open(file_name, 'w', 'utf-8')
            data_out.close()
            return
        raise ValueError('File ', file_name, ' not found and not allowed to create')

    data_in = codecs.open(file_name, 'r', 'utf-8')
    for line in data_in:
        # Ignore comments
        if line[0] == '#':
            continue
        line = line[:-1]

        fields = line.split(',')

        last_modification = float(fields[1])
        
        # Insert it in the global dictionary
        annotation = fields[2:]
        annotation.insert(0, last_modification)
        file_data[fields[0]] = tuple(annotation)
        
    return file_data

def needs_processing(data, file_name):
    """
    Given a dictionary with pairs filename: (modificationtime, etc...), check if
    the file needs processing. If so, returns a tuple with the stored
    information, None otherwise.
    """

    global file_data
    global minimum_difference

    # Use the module data pool if none given
    if data == None:
        data = file_data
        
    # File was modified, but no extra information is available
    if data == None:
        # Return something, although empty because there is nothing stored
        return (None,)

    # Get the time stored in the dictionary
    fdata = data.get(file_name)

    if fdata == None:
        # If there is no data stored, process the file
        return (None,)

    # File exists, but it was not modified, no need to process anything
    if (os.path.getmtime(file_name) - fdata[0]) <= minimum_difference:
        return None

    # If file_data not present, or file exists but older, it was modified.
    return fdata

def update(data, file_name, other_data = None):
    """
    Given a dictionary and a file name, update the modification time and the
    given file data
    """
    
    global file_data

    # Use the module data pool if none given
    if data == None:
        data = file_data
        
    if data == None:
        return

    if other_data == None:
        other_data = []

    # Update the mtime
    other_data.insert(0, os.path.getmtime(file_name))
    data[file_name] = tuple(other_data)

    return


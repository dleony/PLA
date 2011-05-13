#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Copyright (C) 2010 Carlos III University of Madrid
# This file is part of the PLA: Personal Learning Assistant

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
import sys, os, getopt, locale, codecs, hashlib

# Fix the output encoding when redirecting stdout
if sys.stdout.encoding is None:
    (lang, enc) = locale.getdefaultlocale()
    if enc is not None:
        (e, d, sr, sw) = codecs.lookup(enc)
        # sw will encode Unicode data to the locale-specific character set.
        sys.stdout = sw(sys.stdout)

# Import the pla package
_dirName = os.path.dirname(__file__)
_dirName = os.path.abspath(os.path.join(_dirName, '..'))
sys.path.insert(0, _dirName)
sys.path.insert(0, os.path.join(_dirName, 'pla'))

import pla, pla.mysql

def main():
    """
    Script that produces a visualization of the events that preceed in time a
    given event. The visualization is a matrix with the dimensions (time,
    activity of certain type). The parameters for this visualization are the
    following:

    1. Reference Event: The type of event from which to analyze the preceeding
       events. Possible values: Class (datetime), Forumpost

    2. Type of population to consider: The visualization should include data
       about certain population. Possible values: Class, Section, All,
       Person. There could also be a mix, for example, Personal profiles of a
       class.

    3. Select the event types to be displayed. The current collection is:
       - compiler
       - bashcommand
       - visitURL
       - text-editor-start/text-editor-end
       - memory-profiler-start/memory-profiler-end
       - system-start/system-end
       - ide-start/ide-end
       - debugger-start/debugger-end

       - gdb-command (single gdb command, irrelevant for this analysis


       There could be a procedure to probe the database, select the type of
       applications and from there offer the possibility to select/deselect.

     4. Width of the prelude. The amount of time where the prelude should be
     visualized. Possible values: Previous event of a type (class, forum post),
     fixed time frame.

     5. Discretization of the time dimension. It could be days/hours/months.

     6. Reliability threshold: Person profiles are tagged with metadata stating
     their reliability threshold (how many events were recived over the total
     number of operations over the version control system). Anything above .5 is
     acceptable, however, there should be a threshold to set.

     The visualization should distinguish columns such as the weekend (to
     account for a reduction in activity).

     The data matrix to create will have one row per event with the following
     columns:

     - Event ID
     - DateTime
     - Person
     - EventType (to be decided)
     - Section (metadata)
     - PairCoverage     

     The query to obtain his matrix is:

SELECT 
  ev.datetime, re.name as Person, 
  ev.name as EventType, 
  rem_program.metadata as Program, 
  rem_section.metadata as Section, 
  rem_pairnumber.metadata as Pair, 
  rem_paircoverage.metadata as PairCoverage
FROM 
  Event as ev,
  EventRelatedentity as ere, 
  Relatedentity as re,
  RelatedentityMetadata_Program as rem_program,
  RelatedentityMetadata_Section as rem_section,
  RelatedentityMetadata_PairNumber as rem_pairnumber,
  RelatedentityMetadata_PairCoverage as rem_paircoverage
WHERE
  ev.datetime > 'DATE BEGIN'
  AND ev.datetime < 'DATE END'
  AND ev.name != 'gdb-command'
  AND ere.eventid = ev.id 
  AND ere.role = 'user'
  AND ere.relatedentityid = re.id
  AND re.id = rem_program.relatedentityfk
  AND re.id = rem_section.relatedentityfk
  AND re.id = rem_pairnumber.relatedentityfk
  AND re.id = rem_paircoverage.relatedentityfk
  ;


assuming that the following views have been created:
    """

    # Default value for the options
    hostname = 'localhost'
    username = None
    passwd = ''
    dbname = None
    separator = ','

    # Swallow the options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "d:h:p:s:u:", [])
    except getopt.GetoptError, e:
        print str(e)
        sys.exit(2)

    # Parse the options
    for optstr, value in opts:
        # DB Name
        if optstr == "-d":
            dbname = value

        # Hostname
        elif optstr == "-h":
            hostname = value

        # Passwd
        elif optstr == "-p":
            passwd = value

        # Separator
        elif optstr == "-s":
            separator = value

        # Username
        elif optstr == "-u":
            username = value

    if args != []:
        print 'Ignoring the extra arguments'
    
    # Establish the connection with the database
    cursor = None
    try:
        pla.mysql.connect(hostname, username, passwd, dbname)
    except Exception, e:
        print 'Unable to connect with the database'
        print str(e)
        sys.exit(1)
    cursor = pla.mysql.cursorObj

if __name__ == "__main__":
    main()
    

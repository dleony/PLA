#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Copyright (C) 2010 Carlos III University of Madrid
# This file is part of the Adagio: Agile Distributed Authoring Toolkit

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
import sys, MySQLdb

host = None
user = None
passwd = None
dbname = None
dbconnection = None
cursorObj = None

def connect(givenHost = None, givenUser = None, givenPasswd = None, 
            givenDB = None):
    """
    Connect to the given database and store the connection in the global
    variable dbconnection. Create also the cursorObj
    """

    global host
    global user
    global passwd
    global dbname
    global dbconnection
    global cursorObj
    
    # If not enough information is given, bomb out
    if givenUser == None or givenDB == None:
        raise ValueError('mysql: Need at least user AND db to connect to db')

    host = givenHost
    user = givenUser
    passwd = givenPasswd
    dbname = givenDB

    dbconnection = MySQLdb.connect(host=givenHost,
                                   user=givenUser,
                                   passwd=givenPasswd,
                                   db=givenDB)

    cursorObj = dbconnection.cursor()
    return

def disconnect():
    """
    Terminate the connection with the database
    """

    global host
    global user
    global passwd
    global dbname
    global dbconnection
    global cursorObj

    host = None
    user = None
    passwd = None
    dbconnection.close()
    dbconnection = None
    cursosObj = None

# Example
# execute SQL query using execute() method.
# cursorobj.execute("SELECT VERSION()")
# Fetch a single row using fetchone() method.
# data = cursorobj.fetchone()
# print "Database version : %s " % data
# disconnect from server
# db.close()

#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import os, glob, sys, re, logging, getopt, locale, pysvn, subprocess

import PLABasic

def main(): 
    """
    Script to gather the bash history, compress the file. Returns a list of
    files that are to be compressed, added and committed.
    """
    pass

def instrument(plaDirectory, localSvnRoot):
    # If no file is present in pladirectory, nothing to return
    if not os.path.exists(os.path.join(plaDirectory, 'tools', 'bash')):
        PLABasic.logMessage("Bash instrumentation disabled. Skipping")
        return []

    historyFile = os.path.expanduser('~/.bash_history')
    if historyFile == '~/.bash_history' or not os.path.exists(historyFile):
        return []

    result = PLABasic.getUniqueFileName() + '.bash'
    PLABasic.gzipFile(historyFile, os.path.join(localSvnRoot, '.pladata', 
                                                result))

    return [result]

def resetData(plaDirectory, localSvnRoot):
    # Reset the history file
    fobj = open(fromFile, 'w')
    fobj.close()
    
    # pass

    
if __name__ == "__main__":
    main()

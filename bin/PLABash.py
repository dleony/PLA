#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import os

import PLABasic

__bashDataDir = os.path.join(plaDirectory, 'tools', 'bash')
__bashDataFile = os.path.expanduser('~/.bash_history')

def main(): 
    """
    Script to simply return the history file and reset its content
    """
    pass

def instrument():
    global __bashDataDir
    global __bashDataFile

    # If no file is present in pladirectory, nothing to return
    if not os.path.exists(__bashDataDir):
        PLABasic.logMessage("Bash.instrument: Disabled. Skipping")
        return []

    if __bashDataFile == '~/.bash_history' or not os.path.exists(__bashDataFile):
        return []

    # If the file is present, but it is empty (because we reset it, done
    if os.path.getsize(__bashDataFile) == 0:
        return []

    return [__bashDataFile]

def resetData():
    global __bashDataDir
    global __bashDataFile

    # If no file is present in pladirectory, nothing to return
    if not os.path.exists(__bashDataDir):
        PLABasic.logMessage("Bash.instrument: Disabled. Skipping")
        return []

    if __bashDataFile == '~/.bash_history' or not os.path.exists(__bashDataFile):
        return

    # If the file is present, but it is empty (because we reset it, done
    if os.path.getsize(__bashDataFile) == 0:
        return []

    # Reset the history file
    PLABasic.logMessage("Bash.instrument: Removing " + __bashDataFile)
    fobj = open(__bashDataFile, 'w')
    fobj.close()

if __name__ == "__main__":
    main()

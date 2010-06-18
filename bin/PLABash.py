#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import os

import PLABasic

dataDir = os.path.join(PLABasic.plaDirectory, 'tools', 'bash')
dataFile = os.path.expanduser('~/.bash_history')

def main(): 
    """
    Script to simply return the history file and reset its content
    """
    pass

if __name__ == "__main__":
    main()

#!/usr/bin/python
# -*- coding: UTF-8 -*-#
#
# Author: Abelardo Pardo (abelardo.pardo@uc3m.es)
#
import os, glob, sys, re, logging, getopt, locale, gzip, time

def gzipFile(fromFile, toFile):

    if not os.path.exists(fromFile):
        return

    print fromFile
    print toFile

    if not os.path.exists(os.path.dirname(toFile)):
        return

    f_in = open(fromFile, 'rb')
    f_out = gzip.open(toFile, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()

    return

def getUniqueFileName():
    return str(int(time.time() * 1000))



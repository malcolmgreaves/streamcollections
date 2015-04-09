#!/usr/local/bin/python

import sys

inBytes = sum(map(lambda x: int(x), sys.stdin.readlines()))

inMB = float(inBytes) / 1000000.0

print str(inMB) + " MB"


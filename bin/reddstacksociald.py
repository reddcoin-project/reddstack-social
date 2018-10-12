#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Reddstack Social
"""

import sys
import os

# Hack around absolute paths
current_dir = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.abspath(current_dir + "/../")

sys.path.insert(0, parent_dir)

from reddstacksocial.reddstacksociald import run_reddstacksociald

if __name__ == '__main__':
    run_reddstacksociald()

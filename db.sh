#!/bin/bash

if [ $# -eq 0 ]; then
    python3 database/database.py
else
    python3 database/database.py $1
fi

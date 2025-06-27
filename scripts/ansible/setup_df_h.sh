#!/bin/bash

# Navigate to home directory, exit on failure
cd "$HOME" || exit 1

# Check if directory df_h exists under $HOME
# Filters for non-hidden directories, strips ./ prefix, and looks for exact match (case-insensitive)
if find . -maxdepth 1 -type d ! -name '.*' -exec basename {} \; | grep -iq '^df_h$'; then
    echo "Directory df_h already exists under home directory $HOME"
else
    echo "Creating directory $HOME/df_h"
    mkdir "$HOME/df_h"
    echo "Directory df_h created successfully."
    echo "Copying script into df_h directory"
    cp /tmp/df_h.sh "$HOME/df_h/"
    echo "script successfully copied into df_h directory"
    #chmod +x df_h.sh
fi

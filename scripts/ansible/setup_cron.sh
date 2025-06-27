#!/bin/bash

# we define the cron job comment
CRON_COMMENT="#Every minute, spool the df -h output to a csv file to be used by the NFT Tablespace maintenance Python job"
# we add the cron line
CRON_JOB="* * * * * $HOME/df_h/df_h.sh 1>/tmp/df_output.out 2>&1"

# Check if the job already exists to prevent duplication
if crontab -l 2>/dev/null | grep -Fq "$CRON_JOB"; then
    echo "Cron job already exists. No changes made."
else
    # Backup current crontab
    crontab -l 2>/dev/null > /tmp/mycron.bak

    # Append new job
    {
        echo "$CRON_COMMENT"
        echo "$CRON_JOB"
    } >> /tmp/mycron.bak

    # Install new cron file
    crontab /tmp/mycron.bak
    echo "Cron job added successfully."
fi

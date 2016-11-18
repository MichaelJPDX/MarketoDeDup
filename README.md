# MarketoDeDup
Code challenge from Marketo

Author: Michael Holland
Date: November 2016

REQUIRES: forerunnerdb module

Reads a JSON file of "leads" from leads.json, checks for duplicates and consolidates any found according to entryDate
Stores the consolidated records in a Forerunner DB for future reference.

Leads record layout:
"_id",
"email",
"firstName",
"lastName",
"address",
"entryDate"

Run this JS using node, eg:
node marketoDeDup.js

Command-line options:

verbose	Print log output to console (in addition to log file)

show	Print results to console at end

Activity is logged to the file changeLog.txt

Data is stored by Forerunner in myleads-qualifiedLeads-metaData.fdb and myleads-qualifiedLeads.fdb

Formatted output is stored to Output is written to qualifiedLeads.json

To start from "scratch" be sure to remove changeLog.txt and *.fdb from your cloned project before testing!
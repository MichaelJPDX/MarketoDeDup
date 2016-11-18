/************************************************
marketoDeDup.js
Marketo Code Challenge
Process JSON file to remove duplicate records

Author: Michael Holland
Date:   November 16, 2016

Revisions:
	MH-20161116  Initial build
	MH-20161117  "bulletproofing" to check module
				 Add Command-line options

Reads a JSON-formated file of records and removes
dups. Records must have both a unique ID and email.
Last entryDate rules.

Record layout:
"_id",
"email",
"firstName",
"lastName",
"address",
"entryDate"
************************************************/

/****************************
** Some setup stuff
****************************/
var inputFile = "leads.json";
var outputFile = "qualifiedLeads.json";
var logFile = "changeLog.txt";
/***************************/

// Let's see if we can even run:
try {
	require.resolve("forerunnerdb");
} catch(e) {
	console.error("Required module forerunnerdb not found. Please run \nnpm install forerunnerdb\nand try running again.");
	process.exit(e.code);
}

// Initialize options and over-ride based on command line.
var verbose = false;
if (process.argv.indexOf("verbose") > 0) { verbose = true; }
var show = false;
if (process.argv.indexOf("show") > 0) { show = true; }

var fs = require("fs");
var ForerunnerDB = require("forerunnerdb");

// Create an object for logging stuff
var Logger = new Object();
Logger.fsHandle = fs.openSync(logFile, 'a');
Logger.message = function(oString) {
	//  Prepend output with date/time
	var d = new Date();
	//  Docs say I should be able to put the fsHandle in here, but computer says no
	fs.appendFileSync(logFile, d.toISOString() + ": " + oString + "\n"); 
	if (verbose) { console.log(d.toISOString() + ": " + oString); }
};
Logger.message("Processor START");

// Read in the DB using Forerunner
var frdb = new ForerunnerDB();
var db = frdb.db('myleads');
db.persist.dataDir(".");

// Add a collection to the database
var qLeads = db.collection('qualifiedLeads');

// Load any records from file system - the load function is async, so most of the work needs to be inside this.
qLeads.load(function (err) {
    if (!err) {
        Logger.message("Loaded saved records");
        
        // Read and parse input file
		var inFileContent = fs.readFileSync(inputFile);
		var jsonContent = JSON.parse(inFileContent);
		var leads = jsonContent.leads;
		//  loop through input, for each record, check if we have a match to existing records.
		for(var exKey in leads) {
			var results = qLeads.find({
			  '$or': [{ 
				  '_id' : leads[exKey]._id
				},{
				  'email' : leads[exKey].email
				}]
			});
			//console.log("results: " + JSON.stringify(results));
			if (results.length < 1) {
				// a record was not found, so append it.
				qLeads.insert(leads[exKey]);
				Logger.message("Adding record, id: " + leads[exKey]._id + " email: " + leads[exKey].email);
			} else {
				// check dates. Ignore older records, update records with newer input data
				var newDate = new Date(leads[exKey].entryDate);
				//  assuming here that find will only ever return ONE record.
				var currentRecordDate = new Date(results[0].entryDate);
				if (newDate.getTime() > currentRecordDate.getTime()) {
					//console.log("New time: " + newDate.getTime() + " old: " + currentRecordDate.getTime());
					// update the current record.
					// We're sure date is different, so start with that
					var logMsg = "Updating id: " + leads[exKey]._id + " new entry Date: " + leads[exKey].entryDate;
					qLeads.update({ 
						_id: results[0]._id 
					}, { 
						entryDate: leads[exKey].entryDate 
					});
					// check the rest of the fields -- there should be a way to make a function for this :/
					// First name
					if (leads[exKey].firstName != results[0].firstName) {
						logMsg += "\n\t\tFirst name from: " + results[0].firstName + " to: " + leads[exKey].firstName;
						qLeads.update({ _id: results[0]._id }, { firstName: leads[exKey].firstName });
					}
					// First name
					if (leads[exKey].lastName != results[0].lastName) {
						logMsg += "\n\t\tLast name from: " + results[0].lastName + " to: " + leads[exKey].lastName;
						qLeads.update({ _id: results[0]._id }, { 
							lastName: leads[exKey].lastName 
						});
					}
					// Check address
					if (leads[exKey].address != results[0].address) {
						logMsg += "\n\t\tAddress from: " + results[0].address + " to: " + leads[exKey].address;
						qLeads.update({ _id: results[0]._id }, { 
							address: leads[exKey].address 
						});
					}
					// Check email
					if (leads[exKey].email != results[0].email) {
						logMsg += "\n\t\tEmail from: " + results[0].email + " to: " + leads[exKey].email;
						qLeads.update({ _id: results[0]._id }, { email: leads[exKey].email });
					}
					// Lastly, it is possible for ID to change
					if (leads[exKey]._id != results[0]._id) {
						logMsg += "\n\t\tID from: " + results[0]._id + 
								  " to: " + leads[exKey]._id;
						qLeads.update({ 
							_id: results[0]._id 
						}, { $overwrite: { _id: leads[exKey]._id }
						});
					}
					Logger.message(logMsg);
				}
			}
		}  // END for loop on new records
		qLeads.save(function (err) {
			if (!err) {
				Logger.message("Output file saved. Buh bye");
				// even though we've saved the database, which is in JSON format,
				// it isn't pretty. So do another find and build a collection we can 
				// make pretty with a little stringify magic
				var outResults = qLeads.find({}, { 
					$orderBy: { 
						entryDate: 1  // descending date order
					}
				});
				var outputLeads = new Array();
				for (var i = 0; i < outResults.length; i++) {
					outputLeads.push(outResults[i]);
				}
				// And output to the output file above
				var jsonOutput = new Object;
				jsonOutput.leads = outputLeads;
				// Just overwrite the file
				fs.writeFileSync(outputFile, JSON.stringify(jsonOutput, null, 2));
				if (show) { console.log("Final output: " + JSON.stringify(jsonOutput, null, 2)); }
			}
		});
    }
});



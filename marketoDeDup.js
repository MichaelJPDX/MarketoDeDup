/******  Record layout
"_id",
"email",
"firstName",
"lastName",
"address",
"entryDate"
*********************/


/****************************
** Some setup stuff
****************************/
var inputFile = "leads.json";
var outputFile = "qualifiedLeads.json";
var logFile = "changeLog.txt";
/***************************/
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
};
Logger.message("Processor START");

// Read in the DB using loki
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
				//console.log("Adding record, id: " + leads[exKey]._id + " email: " + leads[exKey].email);
			} else {
				// check dates. Ignore older records, update records with newer input data
				var newDate = new Date(leads[exKey].entryDate);
				//  assuming here that find will only ever return ONE record.
				var currentRecordDate = new Date(results[0].entryDate);
				if (newDate.getTime() > currentRecordDate.getTime()) {
					// update the current record.
					// We're sure date is different, so start with that
					var logMsg = "Updating id: " + leads[exKey]._id + " new entry Date: " + leads[exKey].entryDate;
					qLeads.update({ _id: leads[exKey]._id }, { entryDate: leads[exKey].entryDate });
					Logger.message(logMsg);
				}
			}
		}  // END for loop on new records
		qLeads.save(function (err) {
			if (!err) {
				Logger.message("Output file saved. Buh bye");
			}
		});
    }
});



// Global variables
var dataAttributes = {
	"name": "n",
	"time": "t",
	"value": "v",
	"unit": "u"
}
var aggrAttributes = {
	"name": "n",
	"time": "t",
	"value": "v",
	"unit": "u"
}
var hideAttrs = ["url", "data", "retention", "format"];
var configFile = "conf/autogen_config.json";

// DO NOT CHANGE
var hdsURL;
var loginURL;
var logoutURL;
var entriesTable;
var columns = {};
var filesaver;
var abortExporting;


$(document).ready(function(){

	$.ajax({
		dataType: "json",
		url: configFile,
		success: function(json) {
			hdsURL = json.hdsEndpoint;
			
			if(json.authEnabled){
				switch(json.authProvider) {
					case 'cas':
					loginURL = json.authProviderURL + "/login?service=" + json.authServiceID;
					logoutURL =  json.authProviderURL + "/logout";
					break;
					default:
					bootstrapDialog({
						type: BootstrapDialog.TYPE_DANGER,
						closable: false,
						title: 'Unsupported authenticator',
						message: 'Authentication provider is not supported: ' + json.provider
					});
				}
				$("#login").text("Login");
				$("#login").attr("href", loginURL);
			}
			
			main();
		},
		error: function(e) {
			console.error(e);
			bootstrapDialog({
				type: BootstrapDialog.TYPE_DANGER,
				closable: false,
				title: 'Configuration Error: ' + e.status + ' ' + e.statusText,
				message: 'Unable to load configuration file: ' + configFile
			});
		}
	});

	setupModal();

});

function error_401(){
	$("#login").text("Login");
	$("#login").attr("href", loginURL);
	localStorage.removeItem('ticket');
	
	bootstrapDialog({
		type: BootstrapDialog.TYPE_WARNING,
		closable: true,
		title: '401 Unauthorized',
		message: 'No active session. Please login.',
		buttons: [{
			label: 'Login',
			action: function(dialog) {
				dialog.close();
				window.location = loginURL;
			}
		}]
	});
}

function setupModal(){
	// Datetime picker
	$('#datetimepickerStart').datetimepicker({
		format: "YYYY-MM-DDTHH:mm:ss"
	});
	$('#datetimepickerEnd').datetimepicker({
		useCurrent: false, //Important! See issue #1075
		format: "YYYY-MM-DDTHH:mm:ss"
	});
	$("#datetimepickerStart").on("dp.change", function (e) {
		$('#datetimepickerEnd').data("DateTimePicker").minDate(e.date);
	});
	$("#datetimepickerEnd").on("dp.change", function (e) {
		$('#datetimepickerStart').data("DateTimePicker").maxDate(e.date);
	});

	// Dropdown 
	$(".dropdown-menu li a").click(function(){
		$(this).parents('.dropdown').find('.dropdown-toggle').html($(this).text()+'<span class="caret"></span>');
	});
}

function main(){
	// Check support for HTML5 Local Storage
	if(typeof(Storage) == "undefined") {
		bootstrapDialog({
			type: BootstrapDialog.TYPE_DANGER,
			closable: false,
			title: 'Unsupported Browser',
			message: 'Please use:\n Chrome > 4.0\n IE > 8.0\n FireFox > 3.5\n Safari > 4.0\n Opera > 11.5',
		});
		return;
	}
	
	
	if(localStorage.getItem("ticket") != null) {
		$('#login').text("Logout");
		$("#login").attr("href", logoutURL);
		$("#login").on
	}
	
	var uri = new URI(window.location.href);
	if(uri.hasQuery("ticket")){
		console.log(uri);
		var query = URI.parseQuery(URI.parse(window.location.href).query);
		uri.removeSearch("ticket");
		location.replace(uri);
		console.log(query.ticket);
		
		// Store the ticket
		localStorage.setItem("ticket", query.ticket);
	}
	//console.log("Ticket:", localStorage.getItem("ticket"));
	
	getRegistry();
} // end main


function getRegistry(){
	var registry = [];

	// Recursively query all pages of registry starting from page
	function getRegistryPages(page){
		$(".spinner-h1").removeClass('hidden');
		var per_page = 100;
		$.ajax({
			type: "GET",
			headers: {'X-Auth-Token': localStorage.getItem("ticket")},
			url: hdsURL + "/registry?per_page=" + per_page + "&page=" + page,
			dataType:"json",
			success: function(res) {
				//console.log(res);

				if(res.total == 0){
					bootstrapDialog({
						type: BootstrapDialog.TYPE_INFO,
						closable: true,
						title: 'Empty registry',
						message: 'The registry contains no data sources.',
						buttons: [{
							label: 'Close',
							action: function(dialog){
								dialog.close();
							}
						}]
					});
					$(".spinner-h1").addClass('hidden');
					return;
				}

				$.merge(registry, res.entries);
				//console.log(registry);

				if(res.total>per_page*page){
					getRegistryPages(page+1);
				} else {
					fillTable(registry);
				}
			},
			error: function(e) {
				var err = jQuery.parseJSON(e.responseText);
				if (e.status == 401){
					error_401();
				} else if (e.responseText != "") {
					var err = jQuery.parseJSON(e.responseText);
					bootstrapDialog({
						type: BootstrapDialog.TYPE_WARNING,
						closable: true,
						title: 'Error ' + e.status + ': ' + e.statusText,
						message: err.message,
						buttons: [{
							label: 'Close',
							action: function(dialog){
								dialog.close();
							}
						}]
					});
				} else {
					bootstrapDialog({
						type: BootstrapDialog.TYPE_DANGER,
						closable: true,
						title: 'Error ' + e.status + ': ' + e.statusText,
						message: 'Request could not be initialized.',
						buttons: [{
							label: 'Close',
							action: function(dialog){
								dialog.close();
							}
						}]
					});
				}	
				$(".spinner-h1").addClass('hidden');
			}
		}); 
	}
	getRegistryPages(1);
}

function fillTable(entries){	

	var column = 0;
	
	// Set header
	$.each(entries[0], function(key, value){
		//console.log(key + ':' + value, $.type(value));

		switch($.type(value)) {
			case "array":
			console.warn("json array is currently not supported.");
			entry = "<th>" + key + "</th>";
			columns[key] = column++;	
			break;
		
			case "object":
		   	// Nested object
		   	/*
		   	entry = ""
		   	$.each(value, function(nKey, nValue){
		   		entry += "<th class='table-meta'>" + nKey + "</th>";
		   		columns[nKey] = column++;	
		   	});
		   	*/
		   	entry = "<th>" + key + "</th>";
		   	columns[key] = column++;
		   	break;
		
		   	default:
		   	entry = "<th>" + key + "</th>";
		   	columns[key] = column++;	
		} 
		$("#entries thead tr").append(entry);
	});

	console.log(JSON.stringify(columns, null, 4));

	function replacer(key, value) {
		if (key== "data") {
			return undefined;
		}
		return value;
	}

	// Fill data
	entries.forEach(function(entry) {
		tr = "<tr>";
		$.each(entry, function(key, value){
			switch($.type(value)) {
				case "array":
				tr += "<td><div style='line-height: 1.2em; font-size: 90%;''>" + JSON.stringify(value, replacer, 2) + "</div></td>";
				break;
	
				case "object":
			    // Nested object
			    /*
			    $.each(value, function(nKey, nValue){
			    	tr += "<td>" + nValue + "</td>";
			    });
			    */
			    tr += "<td><div style='line-height: 1.2em; font-size: 90%;''>" + JSON.stringify(value, null, 2) + "</div></td>";
			    break;
		
			    default:
			    tr += "<td>" + value + "</td>";
			}
		});
		tr += "</tr>";
		$("#entries tbody").append(tr);	
	});

	// Get index of hideAttrs for table config
	var hideAttrsIndx = [];
	hideAttrs.forEach(function(attr){
		hideAttrsIndx.push(columns[attr]);
	});

	// Configure table
	var filtersConfig = {
		base_path: 'lib/tablefilter/',
		alternate_rows: true,
		rows_counter: true,
		btn_reset: true,
		loader: true,
		mark_active_columns: true,
		highlight_keywords: true,
		//col_widths: [null, null, '30%'],
		extensions: [{
			name: 'sort',
			images_path: 'https://koalyptus.github.io/TableFilter/tablefilter/style/themes/'
		},
		{
			name: 'colsVisibility',
			at_start: hideAttrsIndx,
			text: 'Columns: ',
			enable_tick_all: false
		}
		]
	};
	entriesTable = new TableFilter('entries', filtersConfig);
	entriesTable.init();
	$(".spinner-h1").addClass('hidden');
}

function setupDataExportModal(){
	setProgressbarMain(-1);
	setProgressbarSub(-1);
	$("#dataExport .modalStat").text("(" + entriesTable.getFilteredDataCol(0).length + " sources)");

	var attrs = [];
	$.each(dataAttributes, function(key, value){
		attrs.push(key);					
	});
	$("#dataExport #sampleAttributes").text("Comma separated list: " + attrs.join(', '));
}

function setupAggrExportModal(){
	$("#aggrExport .modalStat").text("(" + entriesTable.getFilteredDataCol(0).length + " sources)");

	var i = columns["aggregation"];
	var allAggrs = entriesTable.getFilteredDataCol(i);
	console.log(allAggrs);

	var aggregates = {};

	allAggrs.forEach(function(aggrs){
		console.log(aggrs);
		
	});

	console.log(JSON.stringify(aggregates));
/*
	// for now assume that the same aggr is used for all sources (only 1 aggr)
	var aggrs = allAggrs[0];
	aggrs = JSON.parse(aggrs);
	var id = aggrs[0].id;
	var interval = aggrs[0].interval;
	var retention = aggrs[0].retention;
	var aggregates = aggrs[0].aggregates;
	console.log(aggregates);
	$("#aggrExport #sampleAttributes").text("Interval: " + interval + ", Retention: " + retention + ", Aggregates: " + aggregates.join(', '));
	*/
}

function abortExport(){
	abortExporting = true;
	if (typeof filesaver != 'undefined'){
		filesaver.abort();
	}
}

function exportData(){
	abortExporting = false;
	var valid = true;
	attributes = $('#dataExport #attributes').val();
	attributes = attributes.replace(/ /g,'').split(','); // remove whitespaces and split
	senmlKeys = [];
	attributes.forEach(function(attr) {
		if(!dataAttributes.hasOwnProperty(attr)){
			bootstrapDialog({
				type: BootstrapDialog.TYPE_DANGER,
				closable: true,
				title: 'Invalid Attribute',
				message: attr + ' is not a valid attribute.',
				buttons: [{
					label: 'Close',
					action: function(dialog){
						dialog.close();
					}
				}]
			});
			valid = false;
			return;
		}
		senmlKeys.push(dataAttributes[attr]);
	});
	if(!valid){
		return; 
	}

	$("#dataExport .export-btn").addClass('hidden');
	$("#dataExport .abort-btn").removeClass('hidden');
	$('#dataExport .close-btn').prop('disabled', true);

	var IDs = entriesTable.getFilteredDataCol(0);
	console.log(IDs);
	var start = $('#datetimepickerStart input').val();
	var end = $('#datetimepickerEnd input').val();
	timeFormat = $('#dataExport #timeFormat').text(); // if local var, it passes the button to recursive function!

	csvData = [];
	totalIDs = entriesTable.getFilteredDataCol(0).length;
	dfd = $.Deferred();
	processItems(IDs, start, end).then(function(){
		// all done
		console.log("all done");
		//console.log(csvData);
		saveCSV(csvData);
	}, function(reason){ // rejected / aborted
		console.warn(reason);
		$("#dataExport .export-btn").removeClass('hidden');
		$("#dataExport .abort-btn").addClass('hidden');
		$('#dataExport .close-btn').prop('disabled', false);
	});
}

function progressbarActive(state){
	if(state==true){
		$('.progress-main > .progress-bar').addClass('progress-bar-striped active');
	} else {
		$('.progress-main > .progress-bar').removeClass('progress-bar-striped active');
	}
}

function setProgressbarMain(value){
	value = Math.round(value*100);
	if(value<0){
		$('.progress-main > .progress-bar').css({'width': '0%', 'min-width': '0em'}).attr('aria-valuenow', value).text('0%');	
	} else {
		$('.progress-main > .progress-bar').css({'width': value+'%', 'min-width': '2em'}).attr('aria-valuenow', value).text(value+'%');
	}
}

function setProgressbarSub(value){
	value = value*100;
	$('.progress-sub > .progress-bar').css('width', value+'%').attr('aria-valuenow', value).text(value+'%');
}

// Recursively query data for all IDs
function processItems(IDs, start, end) {
	//console.log('called processItem');

	setProgressbarMain(1 - IDs.length/totalIDs);

	if(IDs.length == 0){
		// Done with all items
		dfd.resolve();
		console.log("finishing up");
		return;
	}
	id = IDs.shift(); // pop front

	var pageData = [];
	// Recursively query all pages starting from page
	function getDataPages(page){
		if(abortExporting){
			dfd.reject("aborted");
			return;
		}

		var per_page = 100;
		console.log("/data/" + id + "?start=" + start + "Z&end=" + end + "Z&per_page=" + per_page + "&page=" + page);

		$.ajax({
			type: "GET",
			headers: {'X-Auth-Token': localStorage.getItem("ticket")},
			url: hdsURL + "/data/" + id + "?start=" + start + "Z&end=" + end + "Z&per_page=" + per_page + "&page=" + page,
			dataType:"json",
			success: function(res) {
				//console.log(res);
				setProgressbarSub((per_page*page)/res.total)

				if(res.total != 0){
					for (var i = 0; i < res.data.e.length; i++) {
						var csvRow = new Array(attributes.length);
						$.each(res.data.e[i], function(key, value){
							if(key=="t"){
								if(timeFormat=="ISO 8601"){
									value = new Date(value*1000).toISOString();
								}
							}	
							csvRow[$.inArray(key, senmlKeys)] = value;
						});
						pageData.push(csvRow);
					}		
					//console.log(pageData);
				}

				if(res.total>per_page*page){
					// Process next page
					getDataPages(page+1);
				} else {
					var obj = {};
					obj[id] = pageData;
					csvData.push(obj);
					// Process next item
					processItems(IDs, start, end);
				}
			},
			error: function(e) {
				if (e.status == 401){
					error_401();
				} else if (e.responseText != "") {
					var err = jQuery.parseJSON(e.responseText);
					bootstrapDialog({
						type: BootstrapDialog.TYPE_WARNING,
						closable: true,
						title: 'Error ' + e.status + ': ' + e.statusText,
						message: err.message,
						buttons: [{
							label: 'Close',
							action: function(dialog){
								dialog.close();
							}
						}]
					});
				} else {
					bootstrapDialog({
						type: BootstrapDialog.TYPE_DANGER,
						closable: true,
						title: 'Error ' + e.status + ': ' + e.statusText,
						message: 'Request could not be initialized.',
						buttons: [{
							label: 'Close',
							action: function(dialog){
								dialog.close();
							}
						}]
					});
				}	
				dfd.reject(e.status);							
			}
		}); 
	}
	// Start with first page and call recursively till reaching last
	getDataPages(1); 
	return dfd.promise();
}


function saveCSV(data){
	progressbarActive(true);

	var mode = $('#dataExport #exportType').text();
	
	if (mode == "All sources in a single file") {

		var csvContent = "";
		for (var i = 0; i < data.length; i++) {

			$.each(data[i], function(key, value){
				value.forEach(function(infoArray, index){
					var dataString = infoArray.join(",");
					csvContent += index < value.length ? dataString+ "\n" : dataString;
				}); 
			});
		}
		var content = new Blob([csvContent], {type: "text/csv;charset=utf-8"});
		filesaver = saveAs(content, "export.csv");

	} else {

		var zip = new JSZip();
		for (var i = 0; i < data.length; i++) {

			$.each(data[i], function(key, value){
				var csvContentSingle= "";
				//if(value.length != 0){ // Store empty CSV?
					value.forEach(function(infoArray, index){
						var dataString = infoArray.join(",");
						csvContentSingle += index < value.length ? dataString+ "\n" : dataString;
					}); 
					zip.file(key+".csv", csvContentSingle);
				//}
			});
		}
		var content = zip.generate({type:"blob"});
		filesaver = saveAs(content, "export.zip");
	}

	// FileSaver callback
	filesaver.onwriteend = function() {
		$("#dataExport .export-btn").removeClass('hidden');
		$("#dataExport .abort-btn").addClass('hidden');
		$('#dataExport .close-btn').prop('disabled', false);
		progressbarActive(false);
	};
}

// Prevents multipe error popups
function bootstrapDialog(dialog){
	if($.isEmptyObject(BootstrapDialog.dialogs)) {
		BootstrapDialog.show(dialog);
	}
}


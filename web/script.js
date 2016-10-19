// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT
// script.js

// Global variables
// DO NOT CHANGE
var hdsURL;
var loginURL;
var logoutURL;
var serviceID;
var entriesTable;
var columns = {};
var filesaver;
var abortExporting;
var ISO8601;
var registryLoaded = false;


$(document).ready(function(){

	if(localStorage.getItem("no-chrome") == null) {
		var chrome = /chrome/i.test(navigator.userAgent);
		if (!chrome) {
			bootstrapDialog({
				type: BootstrapDialog.TYPE_PRIMARY,
				closable: false,
				title: 'Browser Compatibility',
				message: 'This website works best with latest versions of <a href="https://www.google.com/chrome/">Google Chrome</a> browser.',
				buttons: [{
					label: 'Ignore',
					cssClass: 'btn-warning',
					action: function (dialog) {
						dialog.close();
						localStorage.setItem("no-chrome", "yes");
						main();
					}
				}]
			});
			return;
		}
	}

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
	
	if(!DATA_EXPORT){
		$("#data-export-btn").hide()
	}
	if(!AGGR_EXPORT){
		$("#aggr-export-btn").hide()
	}

	main();
});

function loggedIn(){
	$("#login").text("Logout");
	$("#login").attr("onclick", "logout()");
}

function loggedOut(){
	$("#login").text("Login");
	$("#login").attr("onclick", '$("#loginModal").modal()');
}

function login(){
	$.ajax({
		method: "POST",
		url: loginURL,
		data: { username: $("#usr").val(), password: $("#psw").val() },
		success: function(data, status, xhr) {
			//console.log(JSON.stringify(xhr));
			var location = xhr.getResponseHeader('Location');
			var path = URI.parseQuery(URI.parse(location).path);
			var tgt = Object.keys(path)[0].split("/").pop();
			//console.log(tgt);

			$.ajax({
				method: "POST",
				url: loginURL + "/" + tgt,
				data: { service: serviceID },
				success: function(data, status, xhr) {;
					//console.log(JSON.stringify(xhr));
					var serviceTicket = xhr.responseText;
					//console.log(serviceTicket);
					localStorage.setItem("ticket", serviceTicket);
					$("#loginModal").modal('hide');
					loginSuccess();
					if(!registryLoaded){
						getRegistry();
					}
				},
				error: function(e) {
					loginError(e);
				}
			});	
		},
		error: function(e) {
			loginError(e);
		}
	});
}

function logout(){
	localStorage.removeItem('ticket');
	loggedOut();

	bootstrapDialog({
		type: BootstrapDialog.TYPE_INFO,
		closable: false,
		title: 'Logged out',
		message: 'You have been logged out. Local data will be cleared in 10 seconds.',
		buttons: [{
			label: 'Clear Now',
			action: function(dialog){
				location.reload();
			}
		}]
	});

	setTimeout(function(){
		location.reload();
	}, 10000);
}

function loginError(xhr){
	console.error(xhr);
	bootstrapDialog({
		type: BootstrapDialog.TYPE_DANGER,
		closable: true,
		title: 'Error: ' + xhr.status + ' ' + xhr.statusText,
		message: 'Unable to login: ' + xhr.responseText,
		buttons: [{
			label: 'Close',
			action: function(dialog){
				dialog.close();
			}
		}]
	});
}

function loginSuccess(){
	var dialog = new BootstrapDialog({
		type: BootstrapDialog.TYPE_SUCCESS,
		closable: true,
		title: 'Login',
		message: 'You have successfully logged in.',
	});

	dialog.realize();
	dialog.open();
	setTimeout(function(){
		dialog.close();
	}, 2000);

	loggedIn();
}

function error_401(){
	localStorage.removeItem('ticket');
	loggedOut();

	bootstrapDialog({
		type: BootstrapDialog.TYPE_WARNING,
		closable: true,
		title: '401 Unauthorized',
		message: 'No active session. Please login.',
		buttons: [{
			label: 'Login',
			cssClass: 'btn-primary',
			action: function(dialog) {
				dialog.close();
				$("#loginModal").modal();
			}
		}]
	});
	
}

function setupModal(id){
	// Datetime picker
	$(id + ' #datetimepickerStart').datetimepicker({
		format: "YYYY-MM-DDTHH:mm:ss"
	});
	$(id + ' #datetimepickerEnd').datetimepicker({
		useCurrent: false, //Important! See issue #1075
		showTodayButton: true,
		format: "YYYY-MM-DDTHH:mm:ss"
	});
	$(id + ' #datetimepickerStart').on("dp.change", function (e) {
		$(id + ' #datetimepickerEnd').data("DateTimePicker").minDate(e.date);
	});
	$(id + ' #datetimepickerEnd').on("dp.change", function (e) {
		$(id + ' #datetimepickerStart').data("DateTimePicker").maxDate(e.date);
	});

	// Dropdown
	$(id + ' .dropdown-menu li a').click(function(){
		$(this).parents('.dropdown').find('.dropdown-toggle').html($(this).text()+'<span class="caret"></span>');
	});
}

function main(){
	$.ajax({
		dataType: "json",
		url: CONFIG_FILE,
		success: function(json) {
			hdsURL = location.protocol + "//" + location.hostname + ":" + json.apiPort;

			if(json.authEnabled){
				serviceID = json.authServiceID;
				switch(json.authProvider) {
					case 'cas':
						loginURL = json.authProviderURL + "/v1/tickets";
						break;
					default:
						bootstrapDialog({
							type: BootstrapDialog.TYPE_DANGER,
							closable: false,
							title: 'Unsupported authenticator',
							message: 'Authentication provider is not supported: ' + json.provider
						});
				}

				if(localStorage.getItem("ticket") == null) {
					loggedOut();
					$("#loginModal").modal();
					return;
				} else {
					loggedIn();
				}
			}

			getRegistry();
		},
		error: function(e) {
			console.error(e);
			bootstrapDialog({
				type: BootstrapDialog.TYPE_DANGER,
				closable: false,
				title: 'Configuration Error: ' + e.status + ' ' + e.statusText,
				message: 'Unable to load configuration file: ' + CONFIG_FILE
			});
		}
	});
} // end main


function getRegistry(){
    spinner.show();

	var registry = [];

	// Recursively query all pages of registry starting from page
	function getRegistryPages(page){
		var path = "/registry?per_page=" + REG_PER_PAGE + "&page=" + page;
		console.log(path);
		$.ajax({
			type: "GET",
			headers: {'X-Auth-Token': localStorage.getItem("ticket")},
			url: hdsURL + path,
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
					spinner.hide();
					return;
				}

				$.merge(registry, res.entries);
				//console.log(registry);

				if(res.total>REG_PER_PAGE*page){
					spinner.progress((REG_PER_PAGE*page)/res.total);
					getRegistryPages(page+1);
				} else {
					registryLoaded = true;
					spinner.text("Loading the table...");
					setTimeout(function(){
						fillTable(registry);
                    }, 100);
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
						message: 'Request could not be initialized.\n' + hdsURL + path,
						buttons: [{
							label: 'Close',
							action: function(dialog){
								dialog.close();
							}
						}]
					});
				}
				spinner.hide();
			}
		});
	}
	getRegistryPages(1);
}

function fillTable(entries){

	var column = 0;

	// Set header
	var head = [], h = -1;
	head[++h] = '<thead><tr class="thead-tr">';
	$.each(entries[0], function(key, value){
		head[++h] = '<th>';
		head[++h] = key;
		head[++h] = '</th>';
		columns[key] = column++;
	});
	head[++h] = '</tr></thead>';
	$("#entries").prepend(head.join(''));

	//console.log(JSON.stringify(columns, null, 4));

	function replacer(key, value) {
		if (key== "data") {
			return undefined;
		}
		return value;
	}

	// Fill data
	var out = [], o = -1;
	out[++o] = '<tbody>';
	for(var row=0; row<entries.length; row++){
		out[++o] = '<tr>';
		$.each(entries[row], function(key, value){
			out[++o] = '<td>';
			switch($.type(value)) {
				case "array":
				out[++o] = JSON.stringify(value, replacer, 2);
				break;

				case "object":
				out[++o] = JSON.stringify(value, null, 2);
				break;

				default:
				out[++o] = value;
			}
			out[++o] = '</td>';
		});
		out[++o] = '</tr>';
	}
	out[++o] = '</tbody>';
	$("#entries").append(out.join(''));

	// Get index of HIDDEN_TABLE_COLUMNS for table config
	var HIDDEN_TABLE_COLUMNSIndx = [];
	HIDDEN_TABLE_COLUMNS.forEach(function(attr){
		HIDDEN_TABLE_COLUMNSIndx.push(columns[attr]);
	});

	// Configure table
	var filtersConfig = {
		base_path: 'lib/tablefilter/',
		alternate_rows: true,
		rows_counter: true,
		btn_reset: true,
		mark_active_columns: true,
		highlight_keywords: true,
		no_results_message: true,
		paging: true,
        results_per_page: ['Records: ', [10,25,50,100]],
        toolbar_target_id: 'externalToolbar',
        loader: true,
        loader_css_class: 'hidden', // default indicator
        on_show_loader: function(){ spinner.show(); },
        on_hide_loader: function(){ spinner.hide(); },
        //status_bar: true,
		//col_widths: [null, null, '30%'],
		extensions: [{
			name: 'sort',
			images_path: 'https://koalyptus.github.io/TableFilter/tablefilter/style/themes/'
		},
		{
			name: 'colsVisibility',
			at_start: HIDDEN_TABLE_COLUMNSIndx,
			text: 'Columns: ',
			enable_tick_all: false
		}
		]
	};
	entriesTable = new TableFilter('entries', filtersConfig);
	entriesTable.init();
}

function setupDataExportModal(){
	spinner.show();
	setTimeout(function(){
		setupModal('#dataExport');
    	setProgressbarMain(-1);
    	setProgressbarSub(-1);
    	$("#dataExport .modalStat").text(entriesTable.getFilteredDataCol(0).length + " sources")
    		.promise().done(function(){
    			spinner.hide();
    		});

    	var attrs = [];
    	$.each(DATA_ATTRIBUTES, function(key, value){
    		attrs.push(key);
    	});
		$("#dataExport #sampleAttributes").text("Comma separated list: " + attrs.join(', '));
		$("#dataExport #attributes").val(attrs.join(','));
	}, 100);
}

function setupAggrExportModal(){
	spinner.show();
	setTimeout(function(){
		setupModal('#aggrExport');
		setProgressbarMain(-1);
		setProgressbarSub(-1);
		$("#aggrExport .modalStat").text(entriesTable.getFilteredDataCol(0).length + " sources")
    		.promise().done(function(){
    			spinner.hide();
    		});

		var aggrCol = columns["aggregation"];
		var allAggrs = entriesTable.getFilteredDataCol(aggrCol);
		var IDs = entriesTable.getFilteredDataCol(0);
		//console.log(allAggrs);

		AggrsMap = {};
		var retentions = {};

		allAggrs.forEach(function(aggrs, i){
			aggrs = JSON.parse(aggrs);
			aggrs.forEach(function(aggr){
				//console.log(JSON.stringify(aggr));
				if(!AggrsMap.hasOwnProperty(aggr.id)){ // first occurance
					AggrsMap[aggr.id] = aggr;
					AggrsMap[aggr.id].sources = new Array(IDs[i]);
					retentions[aggr.id] = aggr.retention;
				} else {
					AggrsMap[aggr.id].sources.push(IDs[i]);
					if(retentions[aggr.id] != aggr.retention){
						// same aggregation but different retentions
						retentions[aggr.id] = "multiple durations";
					}
				}
			});
		});
		//console.warn(JSON.stringify(retentions));

		var attrs = [];
		$.each(AGGR_ATTRIBUTES, function(key, value){
			attrs.push(key);
		});

		$("#aggrExport #aggregations").empty();
		$.each(AggrsMap, function(aggrID, aggr){
			var retention = (retentions[aggrID]==''? '&infin;' : retentions[aggrID]);
			$("#aggrExport #aggregations").append('\
				<div id="'+aggrID+'" class="panel panel-primary">\
					<div class="panel-heading"><input class="checkboxPanel" type="checkbox" checked />\
					<em>'+aggr.aggregates.join(", ")+'</em> every <em>'+aggr.interval+'</em>. Retention: <em>'+retention+'</em>\
					<span class="badge pull-right">'+aggr.sources.length+' sources</span></div>\
					<div class="panel-body">Comma separated list: '+attrs.join(", ")+','+aggr.aggregates.join(", ")+'\
					<input class="attributes form-control" type="text" value="'+attrs.join(",")+','+aggr.aggregates.join(",")+'" /></div>\
				</div>\
			');
			$('#'+aggrID+' .checkboxPanel').click(function() {
				if($('#'+aggrID+' .checkboxPanel').is(':checked')){
					//$('#'+aggrID+' .checkboxPanel').prop('checked', false);
					$('#'+aggrID).removeClass('panel-default');
					$('#'+aggrID).addClass('panel-primary');
				} else {
					//$('#'+aggrID+' .checkboxPanel').prop('checked', true);
					$('#'+aggrID).removeClass('panel-primary');
					$('#'+aggrID).addClass('panel-default');
				}
			});
		});
		//console.log(JSON.stringify(AggrsMap));
	}, 100);
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
		if(!DATA_ATTRIBUTES.hasOwnProperty(attr)){
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
		senmlKeys.push(DATA_ATTRIBUTES[attr]);
	});
	if(!valid){
		return;
	}

	$("#dataExport .export-btn").addClass('hidden');
	$("#dataExport .abort-btn").removeClass('hidden');
	$('#dataExport .close-btn').prop('disabled', true);

	var IDs = entriesTable.getFilteredDataCol(0);
	//console.log(IDs);
	var start = $('#dataExport #datetimepickerStart input').val();
	var end = $('#dataExport #datetimepickerEnd input').val();
	start = (start==""?"":start+"Z");
	end = (end==""?"":end+"Z");
	var timeFormat = $('#dataExport #timeFormat').text(); // if local var, it passes the button to recursive function!
	ISO8601 = (timeFormat=="ISO 8601 Timestamp");

	csvData = {};
	totalIDs = entriesTable.getFilteredDataCol(0).length;
	Dfd = $.Deferred();
	processItems(IDs, start, end).then(function(){
		// all done
		console.log("all done");
		//console.log(csvData);
		progressbarActive(true);
		var zip = ($('#dataExport #exportType').text()=="One file per source (zipped)")? true : false;
		saveCSV(csvData, zip, function() {
			$("#dataExport .export-btn").removeClass('hidden');
			$("#dataExport .abort-btn").addClass('hidden');
			$('#dataExport .close-btn').prop('disabled', false);
			progressbarActive(false);
		});
	}, function(reason){ // rejected / aborted
		console.warn(reason);
		$("#dataExport .export-btn").removeClass('hidden');
		$("#dataExport .abort-btn").addClass('hidden');
		$('#dataExport .close-btn').prop('disabled', false);
	});
}

function exportAggr(){
	abortExporting = false;
	var valid = true;
	var totalChecked = 0;

	//console.log(JSON.stringify(AggrsMap));
	var items = []; // one element per source/aggregation

	$.each(AggrsMap, function(aggrID, aggr){

		var checked = $('#'+aggrID+' .checkboxPanel').is(':checked');
		if(checked){
			totalChecked++;
			var selectedAttributes = $('#'+aggrID+' .attributes').val().replace(/ /g,'').split(','); // remove whitespaces and split

			if(selectedAttributes.length<1 || selectedAttributes[0]==''){
				bootstrapDialog({
					type: BootstrapDialog.TYPE_DANGER,
					closable: true,
					title: 'No Attributes',
					message: 'No attributes specified for an aggregation.',
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

			var attrs = [];
			$.each(AGGR_ATTRIBUTES, function(key, value){
				attrs.push(key);
			});

			senmlKeys = [];
			selectedAttributes.forEach(function(attr){
				if($.inArray(attr, aggr.aggregates)==-1 && $.inArray(attr, attrs)==-1){
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
				if(AGGR_ATTRIBUTES.hasOwnProperty(attr)){
					senmlKeys.push(AGGR_ATTRIBUTES[attr]);
				} else {
					senmlKeys.push(attr);
				}
			});

			aggr.sources.forEach(function(sourceID){
				items.push({
					'aggrID': aggrID,
					'sourceID': sourceID,
					'attributes': senmlKeys
				});
			});
		}
    });
	if(!valid || totalChecked==0){
		return;
	}

	$("#aggrExport .export-btn").addClass('hidden');
	$("#aggrExport .abort-btn").removeClass('hidden');
	$('#aggrExport .close-btn').prop('disabled', true);

	var start = $('#aggrExport #datetimepickerStart input').val();
	var end = $('#aggrExport #datetimepickerEnd input').val();
	start = (start==""?"":start+"Z");
	end = (end==""?"":end+"Z");
	var timeFormat = $('#aggrExport #timeFormat').text(); // if local var, it passes the button to recursive function!
	ISO8601 = (timeFormat=="ISO 8601 Timestamp");

	CsvData = {};
	TotalItems = items.length;
	Dfd = $.Deferred();
	processAggrs(items, start, end).then(function(){
		// all done
		console.log("all done");
		//console.log(csvData);
		progressbarActive(true);

		if($('#aggrExport #exportType').text()=="One file per aggregation (zipped)"){
			var merged = {};
			$.each(CsvData, function(key, value){
				aggrID = key.split("_", 1)[0];
				if(!merged.hasOwnProperty(aggrID)){
					merged[aggrID] = value;
				} else {
					//merged[aggrID].push(value);
					$.merge(merged[aggrID], value);
				}
			});
			CsvData = merged;
		}

		saveCSV(CsvData, true, function() {
			$("#aggrExport .export-btn").removeClass('hidden');
			$("#aggrExport .abort-btn").addClass('hidden');
			$('#aggrExport .close-btn').prop('disabled', false);
			progressbarActive(false);
		});
	}, function(reason){ // rejected / aborted
		console.warn(reason);
		$("#aggrExport .export-btn").removeClass('hidden');
		$("#aggrExport .abort-btn").addClass('hidden');
		$('#aggrExport .close-btn').prop('disabled', false);
	});
}


// Recursively query aggr data
function processAggrs(items, start, end) {
	//console.log('called processItem');

	setProgressbarMain(1 - items.length/TotalItems);

	if(items.length == 0){
		// Done with all items
		Dfd.resolve();
		console.log("finishing up");
		return;
	}
	item = items.shift(); // pop front

	var pageData = [];
	// Recursively query all pages starting from page
	function getAggrPages(page){
		if(abortExporting){
			Dfd.reject("aborted");
			return;
		}

		var path = "/aggr/" + item.aggrID + "/" + item.sourceID + "?start=" + start + "&end=" + end + "&per_page=" + AGGR_PER_PAGE + "&page=" + page;
		console.log(path);

		$.ajax({
			type: "GET",
			headers: {'X-Auth-Token': localStorage.getItem("ticket")},
			url: hdsURL+path,
			dataType:"json",
			success: function(res) {
				//console.log(res);
				setProgressbarSub((AGGR_PER_PAGE*page)/res.total)

				if(res.total != 0){
					for (var i = 0; i < res.data.e.length; i++) {
						var csvRow = new Array(item.attributes.length);
						$.each(res.data.e[i], function(key, value){
							if((key=="ts" || key=="te") && ISO8601){
								value = new Date(value*1000).toISOString();
							}
							csvRow[$.inArray(key, item.attributes)] = value;
						});
						pageData.push(csvRow);
					}
					//console.log(pageData);
				}

				if(res.total>AGGR_PER_PAGE*page){
					// Process next page
					getAggrPages(page+1);
				} else {
					//var obj = {};
					//obj[item.aggrID + '_' + item.sourceID] = pageData;
					//CsvData.push(obj);
					CsvData[item.aggrID + '_' + item.sourceID] = pageData;
					// Process next item
					processAggrs(items, start, end);
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
						message: 'Request could not be initialized.\n' + hdsURL+path,
						buttons: [{
							label: 'Close',
							action: function(dialog){
								dialog.close();
							}
						}]
					});
				}
				Dfd.reject(e.status);
			}
		});
	}
	// Start with first page and call recursively till reaching last
	getAggrPages(1);
	return Dfd.promise();
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
		Dfd.resolve();
		console.log("finishing up");
		return;
	}
	id = IDs.shift(); // pop front

	var pageData = [];
	// Recursively query all pages starting from page
	function getDataPages(page){
		if(abortExporting){
			Dfd.reject("aborted");
			return;
		}

		var path = "/data/" + id + "?start=" + start + "&end=" + end + "&per_page=" + DATA_PER_PAGE + "&page=" + page;
		console.log(path);

		$.ajax({
			type: "GET",
			headers: {'X-Auth-Token': localStorage.getItem("ticket")},
			url: hdsURL+path,
			dataType:"json",
			success: function(res) {
				//console.log(res);
				setProgressbarSub((DATA_PER_PAGE*page)/res.total)

				if(res.total != 0){
					for (var i = 0; i < res.data.e.length; i++) {
						var csvRow = new Array(attributes.length);
						$.each(res.data.e[i], function(key, value){
							if(key=="t" && ISO8601){
								value = new Date(value*1000).toISOString();
							}
							csvRow[$.inArray(key, senmlKeys)] = value;
						});
						pageData.push(csvRow);
					}
					//console.log(pageData);
				}

				if(res.total>DATA_PER_PAGE*page){
					// Process next page
					getDataPages(page+1);
				} else {
					//var obj = {};
					//obj[id] = pageData;
					//csvData.push(obj);
					csvData[id] = pageData
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
						message: 'Request could not be initialized.\n' + hdsURL+path,
						buttons: [{
							label: 'Close',
							action: function(dialog){
								dialog.close();
							}
						}]
					});
				}
				Dfd.reject(e.status);
			}
		});
	}
	// Start with first page and call recursively till reaching last
	getDataPages(1);
	return Dfd.promise();
}


function saveCSV(data, zip, callback){

	if (zip) {
		var zip = new JSZip();
		$.each(data, function(key, value){
			var csvContentSingle= "";
			value.forEach(function(infoArray, index){
				var dataString = infoArray.join(",");
				csvContentSingle += index < value.length ? dataString+ "\n" : dataString;
			});
			//var filename = key.replace(/[^A-Za-z0-9._-]/g,'_');
			var filename = key;
			zip.file(filename+".csv", csvContentSingle);
		});
		var content = zip.generate({type:"blob"});
		filesaver = saveAs(content, "export.zip");
	} else {
		var csvContent = "";
		$.each(data, function(key, value){
			value.forEach(function(infoArray, index){
				var dataString = infoArray.join(",");
				csvContent += index < value.length ? dataString+ "\n" : dataString;
			});
		});
		var content = new Blob([csvContent], {type: "text/csv;charset=utf-8"});
		filesaver = saveAs(content, "export.csv");
	}

	// FileSaver callback
	filesaver.onwriteend = callback;
}

// Prevents multipe error popups
function bootstrapDialog(dialog){
	if($.isEmptyObject(BootstrapDialog.dialogs)) {
		BootstrapDialog.show(dialog);
	}
}

// Loading spinner
var spinner = spinner || (function () {
    var loading = "<div class='loadingSpinner'></div><div class='loadingText'></div>"
    return {
        show: function() {
            $("body").append(loading);
        },
        hide: function () {
            $(".loadingSpinner").remove();
            $(".loadingText").remove();
        },
        text: function (value) {
            $('.loadingText').text(value);
        },
        progress: function (value) { // value should be 0-1
            $('.loadingText').text(Math.round(value*100)+'%');
        },
    };
})();

// Show errors
window.onerror = function(msg, url, line) {

	bootstrapDialog({
		type: BootstrapDialog.TYPE_DANGER,
		closable: false,
		title: 'Internal Error',
		message: url + ":" + line + ": " + msg,
		buttons: [{
			label: 'Close',
			action: function (dialog) {
				dialog.close();
			}
		}]
	});

	var suppressErrorAlert = false;
	return suppressErrorAlert;
};
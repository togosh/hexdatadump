// Get all historical stake data

//CSV tables:
//- issued_cds  (stakeStarts)       // Desired: hex_staked>0 and shares>0
//- ended_cds  (stakeEnds)          // Desired: payout>0 and end_date is not null
//- hex_price  (HEXDailyStats API)
//- goodaccounted_cds
//- active_cds_summed

var CONFIG = require('./config.json');
var DEBUG = CONFIG.debug;

const HEX_SUBGRAPH_API_ETHEREUM = "https://api.thegraph.com/subgraphs/name/codeakk/hex";
const HEX_SUBGRAPH_API_PULSECHAIN = "https://graph.pulsechain.com/subgraphs/name/Codeakk/Hex";

const BLOCK_SUBGRAPH_API_ETHEREUM = "https://api.thegraph.com/subgraphs/name/blocklytics/ethereum-blocks";
const BLOCK_SUBGRAPH_API_PULSECHAIN = "https://graph.pulsechain.com/subgraphs/name/pulsechain/blocks";

const PULSEX_SUBGRAPH_API_PULSECHAIN = "https://graph.pulsechain.com/subgraphs/name/pulsechain/pulsex";

const http = require('http');
const https = require('https');
const express = require('express');
const path = require('path');
const fs = require('fs');
const schedule = require('node-schedule');

const { JSDOM } = require( "jsdom" );
const { window } = new JSDOM( "" );
const $ = require( "jquery" )( window );

var mongoose = require('mongoose');
var Schema = mongoose.Schema;

const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));
var fetchRetry = require('fetch-retry')(fetch, { 
  retryOn: async function(attempt, error, response) {
    if (attempt > 3) { return false; }

    if (error !== null) {
      console.log(`FETCH --- RETRY ${attempt + 1} --- ERROR --- ` + error.toString()); await sleep(500 * attempt);
      return true;
    } 

    if (response.status >= 400) {
      console.log(`FETCH --- RETRY ${attempt + 1} --- STATUS --- ` + response.status); await sleep(500 * attempt);
      return true;
    }

    try {
      var response2 = await response.clone().buffer();
      const json = JSON.parse(response2);

      if (json.errors && Object.keys(json.errors).length > 0) {
          if (json.errors[0].message) {
            console.log(`FETCH --- INTERNAL JSON ERROR --- ${attempt + 1} --- ` + json.errors[0].message); await sleep(500 * attempt);
            return true;
          }
      }
      
      return false;
    } catch (error) {
      console.log(`FETCH --- RETRY ${attempt + 1} --- JSON ---` + error.toString()); await sleep(500 * attempt);
      return true;
    }
  }
});

const FETCH_SIZE = 1048576;
var grabDataRunning_ETHEREUM = false;
var grabDataRunning_PULSECHAIN = false;
var lastUpdated_ETHEREUM = undefined;
var lastUpdated_PULSECHAIN = undefined;
var ETHEREUM = "ETHEREUM";
var PULSECHAIN = "PULSECHAIN";

var hostname = CONFIG.hostname;
//if (DEBUG){ hostname = '127.0.0.1'; }
if (DEBUG){ hostname = '127.0.0.1'; }

var httpPort = 80; 
//if (DEBUG){ httpPort = 3000; }
if (DEBUG){ httpPort = 3000; }
const httpsPort = 443;

var httpsOptions = undefined;
if(!DEBUG){ httpsOptions = {
	cert: fs.readFileSync(CONFIG.https.cert),
	ca: fs.readFileSync(CONFIG.https.ca),
	key: fs.readFileSync(CONFIG.https.key)
};}

//var ConnectionSchema = new Schema({
//	created: {
//    type: Date, 
//    required: true
//  },
//	ipaddress: {
//    type: String, 
//    required: true
//  }
//});
//
//const Connection = mongoose.model('Connection', ConnectionSchema);

const app = express();

app.use(function(req, res, next) {
	try {
    //if (!DEBUG && req.path === "/" && req.ip){
      //connections[req.ip] = Date.now();

      //const connection = new Connection({ 
      //	created: Date.now(),
      //	ipaddress: req.ip
      //});

      //connection.save(function (err) {
      //	if (err) return log(err);
      //});
    //}
	} catch (error) {
		log('APP ----- Connection ' + error);
	}

	next();
});

const httpServer = http.createServer(app);
var httpsServer = undefined;
if(!DEBUG){ httpsServer = https.createServer(httpsOptions, app);}

if(!DEBUG){ app.use((req, res, next) => 
{
  if(req.protocol === 'http') { 
    res.redirect(301, 'https://' + hostname); 
  }
  next(); 
}); }

app.use(express.static(path.join(__dirname, 'public')));

app.get("/", function(req, res){ res.sendFile('/index.html', {root: __dirname}); });

app.get("/" + CONFIG.urls.grabdata, function (req, res) {
  if (!grabDataRunning_ETHEREUM){ grabData(ETHEREUM); }
  if (!grabDataRunning_PULSECHAIN){ grabData(PULSECHAIN); }
  res.send(new Date().toISOString() + ' - Grab Data!');
});

httpServer.listen(httpPort, hostname, () => { log(`Server running at http://${hostname}:${httpPort}/`);});
if(!DEBUG){ httpsServer.listen(httpsPort, hostname, () => { 
    log('listening on *:' + httpsPort); 
  });
}

var io = undefined;
if(DEBUG){ io = require('socket.io')(httpServer);
} else { io = require('socket.io')(httpsServer, {secure: true}); }

//if(!DEBUG){
const rule30 = new schedule.RecurrenceRule();
rule30.hour = 0;
rule30.minute = 30;
rule30.tz = 'Etc/UTC';

const job30 = schedule.scheduleJob(rule30, function(){
  log('**** DAILY DATA TIMER 30!');
  if (!grabDataRunning_ETHEREUM){ grabData(ETHEREUM); }
  if (!grabDataRunning_PULSECHAIN){ grabData(PULSECHAIN); }
});
//}

var runGrabTokenHolders_ETHEREUM = false;
var runGrabTokenHolders_PULSECHAIN = false;

const jobTokenHolders = schedule.scheduleJob('* */6 * * *', function(){
  log('**** TOKEN HOLDER DATA TIMER 6 HOURS!');
  if (!runGrabTokenHolders_ETHEREUM) grabTokenHolders(ETHEREUM);
  if (!runGrabTokenHolders_PULSECHAIN) grabTokenHolders(PULSECHAIN);
});
/////////////////////////////////////////////////////////////////////////

//test()

async function grabData(network) {
    if (network == ETHEREUM){
      grabDataRunning_ETHEREUM = true;
    } else if (network == PULSECHAIN){
      grabDataRunning_PULSECHAIN = true;
    } else {
      log("ERROR: grabData() - Unsupported Network - Exit");
      return;
    }
    
    log("grabData() START --- " + network);

    var today = undefined;
    var blockNumber = undefined;
    var day = undefined
    var fullData = undefined;
    var prices = undefined;
    var stakeStarts = undefined;
    var stakeEnds = undefined;
    var stakeGoodAccountings = undefined;
    var stakesActive = undefined;
    
    try {
      day = 6; //703;

      //var blockNumber = await getEthereumBlock(day);
      //console.log(blockNumber);

      today = new Date();
      today.setUTCHours(0, 0, 0, 0);
      var todayTime = parseInt((today.getTime() / 1000).toFixed(0));
      var dayToday = ((todayTime - 1575417600) / 86400) + 2;
      console.log("dayToday: " + dayToday + " network: " + network);
      day = dayToday;

      blockNumber = await getEthereumBlock(network, day); //await getEthereumBlockLatest();
      console.log("network " + network + " - blockNumber " + blockNumber);

      if (network == ETHEREUM){
        fullData = await getHEXDailyStatsFullData();
        prices = fullData.map(a => a.priceUV2UV3).reverse();
        var pricesCSV = prices.join('\n');
        //console.log(pricesCSV);
        fs.writeFileSync('./public/hex_price.csv', pricesCSV);
        //return;
      } else if (network == PULSECHAIN){
        prices = await get_priceDataHistorical(network, blockNumber);
        prices = prices.map(a => a.priceUSD);
        prices.pop();
        var pricesCSV = prices.join('\n');
        //console.log(pricesCSV);
        fs.writeFileSync('./public/hex_price_PULSECHAIN.csv', pricesCSV);
      }
      
      console.time('get_stakeStartDataHistorical ' + network);
      stakeStarts = await get_stakeStartDataHistorical(network, blockNumber);
      console.log(stakeStarts);
      console.timeEnd('get_stakeStartDataHistorical ' + network);

      var stakeStartsList = [];
      stakeStarts.forEach(row => {
        var newTimestamp = Number(row.timestamp) + (Number(row.stakedDays)*24*60*60);
        var expectedEndDate = new Date(newTimestamp*1000);
        expectedEndDate.setUTCHours(0, 0, 0, 0);
        var expectedEndDateString = expectedEndDate.getUTCFullYear() + "-" + minTwoDigits(expectedEndDate.getUTCMonth() + 1) + "-" + minTwoDigits(expectedEndDate.getUTCDate());

        var startDate = (new Date(Number(row.timestamp) * 1000));
        var startDateString = startDate.getUTCFullYear() + "-" + minTwoDigits(startDate.getUTCMonth() + 1) + "-" + minTwoDigits(startDate.getUTCDate());

        //console.log(row.startDay + " " + Number(day - row.startDay) + " " + expectedEndDate.toUTCString().split(", ")[1]);

        var newRow = {
          CD_id:      row.id,
          //stake_id:   row.stakeId,
          address:    row.stakerAddr,

          hex_staked: Number(row.stakedHearts / 100000000),
          shares:     Number(row.stakeShares),

          start_date: startDateString, //(new Date(Number(row.timestamp) * 1000)).toUTCString().split(", ")[1], // Number(row.timestamp),
          //end_date:   null,
          expected_end_date: expectedEndDateString, //expectedEndDate.toUTCString().split(", ")[1], // Number(expectedEndDate.getTime() / 1000),

          stake_days:         Number(row.stakedDays),
          //actual_days_staked: row.stakeEnd ? Number(row.stakeEnd.servedDays) : Number(day - row.startDay),
          
          ////hex_price_when_issued:  prices[row.startDay - 1],
          //hex_price_when_ended:   null

          //start_day: Number(row.startDay)
        }
        stakeStartsList.push(newRow);
      });

      var stakeStartsCSV = convertCSV(stakeStartsList);
      //console.log(stakeStartsCSV);
      if (network == ETHEREUM){
        fs.writeFileSync('./public/issued_cds.csv', stakeStartsCSV);
      } else if (network == PULSECHAIN){
        fs.writeFileSync('./public/issued_cds_PULSECHAIN.csv', stakeStartsCSV);
      }
      

      //var stakeStartsList_Annual = stakeStartsList.filter(a => (a.start_date >= AAA && a.start_date < AAA);
      //var stakeStartsCSV_Annual = convertCSV(stakeStartsList_Annual);


      
      stakeEnds = stakeStarts.filter(a => a.stakeEnd);
      //console.log("stakeStartsEnded.length: " + stakeStartsEnded.length);

      var stakeEndsList = [];
      stakeEnds.forEach(row => {

        var endDay = new Date(row.stakeEnd.timestamp*1000);
        endDay.setUTCHours(0, 0, 0, 0);
        var unixtime = parseInt((endDay.getTime() / 1000).toFixed(0));
        //console.log(unixtime);
        var dayFind = ((unixtime - 1575417600) / 86400) + 3;
        //console.log("dayFind: " + dayFind);

        //console.log((new Date(row.stakeEnd.timestamp * 1000)).toUTCString().split(", ")[1]);

        var endDate = (new Date(Number(row.stakeEnd.timestamp) * 1000));
        var endDateString = endDate.getUTCFullYear() + "-" + minTwoDigits(endDate.getUTCMonth() + 1) + "-" + minTwoDigits(endDate.getUTCDate());

        var newRow = {
          CD_id:      row.id,
          //stake_id:   row.stakeId,
          //address:    row.stakerAddr,

          payout:     Number(row.stakeEnd.payout),
          penalty:    Number(row.stakeEnd.penalty),

          //hex_staked: Number(row.stakedHearts / 100000000),
          //shares:     Number(row.stakeShares),

          //start_date:         Number(row.timestamp),
          end_date:           endDateString, //(new Date(Number(row.stakeEnd.timestamp) * 1000)).toUTCString().split(", ")[1],
          //expected_end_date:  null,

          //stake_days:         Number(row.stakedDays),
          //actual_days_staked: Number(row.stakeEnd.servedDays),

          ////hex_price_when_issued:  prices[row.startDay - 1],
          ////hex_price_when_ended:   prices[dayFind - 1],

          //start_day: Number(row.startDay),
          //end_day: dayFind,
        };
        stakeEndsList.push(newRow);
      });
      //console.log(stakeEndsList);

      var stakeEndsCSV = convertCSV(stakeEndsList);
      //console.log(stakeEndsCSV);
      if (network == ETHEREUM){
        fs.writeFileSync('./public/ended_cds.csv', stakeEndsCSV);
      } else if (network == PULSECHAIN){
        fs.writeFileSync('./public/ended_cds_PULSECHAIN.csv', stakeEndsCSV);
      }



      stakeGoodAccountings = stakeStarts.filter(a => a.stakeGoodAccounting);
      console.log("stakeGoodAccountings.length: " + network + " - " + stakeGoodAccountings.length);

      var stakeGoodAccountingsList = [];
      stakeGoodAccountings.forEach(row => {

        var gaDayDate = new Date(row.stakeGoodAccounting.timestamp*1000);
        gaDayDate.setUTCHours(0, 0, 0, 0);
        var gaDayUnix = parseInt((gaDayDate.getTime() / 1000).toFixed(0));
        var gaDay = ((gaDayUnix - 1575417600) / 86400) + 3;

        var endDayDate = new Date(row.stakeGoodAccounting.timestamp*1000);
        endDayDate.setUTCHours(0, 0, 0, 0);
        var endDayUnix = parseInt((endDayDate.getTime() / 1000).toFixed(0));
        var endDay = ((endDayUnix - 1575417600) / 86400) + 3;

        var gaDate = (new Date(Number(row.stakeGoodAccounting.timestamp) * 1000));
        var gaDateString = gaDate.getUTCFullYear() + "-" + minTwoDigits(gaDate.getUTCMonth() + 1) + "-" + minTwoDigits(gaDate.getUTCDate());

        var endDate = null;
        var endDateString = "";
        if (row.stakeEnd){
          endDate = (new Date(Number(row.stakeEnd.timestamp) * 1000));
          endDateString = endDate.getUTCFullYear() + "-" + minTwoDigits(endDate.getUTCMonth() + 1) + "-" + minTwoDigits(endDate.getUTCDate());
        }

        var newRow = {
          CD_id:      row.id,
          stake_id:   row.stakeId,
          address:    row.stakerAddr,

          payout:     row.stakeGoodAccounting.payout ? (Number(row.stakeGoodAccounting.payout) / 100000000) : null,
          penalty:    row.stakeGoodAccounting.penalty ? Number(row.stakeGoodAccounting.penalty) : null,

          hex_staked: Number(row.stakedHearts / 100000000),
          shares:     Number(row.stakeShares),
          tshares:    Number(row.stakeShares) / 1000000000000,
          
          //row.stakeShares.substring(0, row.stakeShares.length - 12) + "." + row.stakeShares.substring(row.stakeShares.length - 12),

          //start_date:         Number(row.timestamp),
          end_date:          endDateString,  //(new Date(Number(row.stakeEnd.timestamp) * 1000)).toUTCString().split(", ")[1],
          ga_date:           gaDateString,
          //expected_end_date:  null,

          stake_days:         Number(row.stakedDays),
          //actual_days_staked: Number(row.stakeEnd.servedDays),

          ////hex_price_when_issued:  prices[row.startDay - 1],
          ////hex_price_when_ended:   prices[dayFind - 1],

          start_day: Number(row.startDay),
          end_day: endDay,
          ga_day: gaDay,

          block_number: row.stakeGoodAccounting.blockNumber,
          transaction_hash: row.stakeGoodAccounting.transactionHash,
        };
        stakeGoodAccountingsList.push(newRow);
      });
      //console.log(stakeGoodAccountingsList);

      var stakeGoodAccountingsCSV = convertCSV(stakeGoodAccountingsList);
      //console.log(stakeGoodAccountingsCSV);
      if (network == ETHEREUM){
        fs.writeFileSync('./public/goodaccounted_cds.csv', stakeGoodAccountingsCSV);
      } else if (network == PULSECHAIN){
        fs.writeFileSync('./public/goodaccounted_cds_PULSECHAIN.csv', stakeGoodAccountingsCSV);
      }

      try {
      stakesActive = stakeStarts.filter(a => (!a.stakeEnd && !a.stakeGoodAccounting));
      //console.log(stakesActive);

      var stakesActiveList = [];
      stakesActive.forEach(row => {
        var newRow = {
          address:    row.stakerAddr,
          shares:     Number(row.stakeShares),
        }
        stakesActiveList.push(newRow);
      });
      //console.log("stakesActiveList");
      //console.log(stakesActiveList);

      var res = Array.from(stakesActiveList.reduce(
        (m, {address, shares}) => m.set(address, (m.get(address) || 0) + shares), 
        new Map
      ), ([address, shares]) => ({address, shares}));
      //console.log("res");
      //console.log(res);

      res.sort((a, b) => (a.shares < b.shares) ? 1 : -1);

      var res2 = res.map(obj => ({ ...obj, bshares: obj.shares / 1000000000, tshares: obj.shares / 1000000000000 }));

      var stakesActiveCSV = convertCSV(res2);
      //console.log(stakesActiveCSV);
      if (network == ETHEREUM){
        fs.writeFileSync('./public/active_cds_summed.csv', stakesActiveCSV);
      } else if (network == PULSECHAIN){
        fs.writeFileSync('./public/active_cds_summed_PULSECHAIN.csv', stakesActiveCSV);
      }
      } catch (error){
        console.log("stakesActive ERROR:" + " " + network);
        console.log(error);
      }


      //var lastUpdated = "Day: " + day + ", Block Number: <a href='https://etherscan.io/block/" + blockNumber + "' target='_blank'>" + blockNumber + "</a>";
      var lastUpdated = {
        day: day,
        block: blockNumber
      };

      if (network == ETHEREUM){
        lastUpdated_ETHEREUM = lastUpdated;
        io.emit("lastUpdated_ETHEREUM", lastUpdated_ETHEREUM);
      } else if (network == PULSECHAIN){
        lastUpdated_PULSECHAIN = lastUpdated;
        io.emit("lastUpdated_PULSECHAIN", lastUpdated_PULSECHAIN);
      }

    } catch (error){
      log("grabData() ERROR - " + network + " - " + error);
    } finally {
      if (network == ETHEREUM){
        grabDataRunning_ETHEREUM = false;
      } else if (network == PULSECHAIN){
        grabDataRunning_PULSECHAIN = false;
      }
      log("grabData() END --- " + network);
    }
    
    try {
      if (network == ETHEREUM){
        if (!runGrabTokenHolders_ETHEREUM){ grabTokenHolders(ETHEREUM); }
      }  else if (network == PULSECHAIN){
        if (!runGrabTokenHolders_PULSECHAIN){ grabTokenHolders(PULSECHAIN); }
      }
    } catch (e){
      log("grabData() END TOKENHOLDERS --- " + network);
    }
}

let grabTokenHolders = async (network) => {
  if (network == ETHEREUM){
    runGrabTokenHolders_ETHEREUM = true;
  } else if (network == PULSECHAIN){
    runGrabTokenHolders_PULSECHAIN = true;
  } else {
    log("ERROR: grabTokenHolders() - Unsupported Network - Exit" + " - network: " + network);
    return;
  }

  log("grabTokenHolders() START --- " + network);

  var today = undefined;
  var blockNumber = undefined;
  
  try {
    let day = 0;

    today = new Date();
    today.setUTCHours(0, 0, 0, 0);
    var todayTime = parseInt((today.getTime() / 1000).toFixed(0));
    var dayToday = ((todayTime - 1575417600) / 86400) + 2;
    console.log("dayToday: " + network + " - " + dayToday);
    day = dayToday;

    blockNumber = await getEthereumBlock(network, day); //await getEthereumBlockLatest();
    console.log(blockNumber);

    console.time('get_totalTokenHolders - ' + network);
    let tokenHolders = await get_totalTokenHolders(network, blockNumber);
    console.log(tokenHolders);
    console.timeEnd('get_totalTokenHolders - ' + network);
 
    var tokenHoldersCSV = convertCSV(tokenHolders);
    //console.log(tokenHoldersCSV);

    if (network == ETHEREUM){
      fs.writeFileSync('./public/tokenHolders.csv', tokenHoldersCSV);
    } else if (network == PULSECHAIN){
      fs.writeFileSync('./public/tokenHolders_PULSECHAIN.csv', tokenHoldersCSV);
    }

  } catch (error){
    log("grabTokenHolders() ERROR - " + network + " - " + error);
  } finally {
    if (network == ETHEREUM){
      runGrabTokenHolders_ETHEREUM = false;
    } else if (network == PULSECHAIN){
      runGrabTokenHolders_PULSECHAIN = false;
    }
    log("grabTokenHolders() END");
  } 
}

function convertCSV(list){
  const replacer = (key, value) => value === null ? '' : value // specify how you want to handle null values here
  const header = Object.keys(list[0])
  const csv = [
    header.join(','), // header row first
    ...list.map(row => header.map(fieldName => JSON.stringify(row[fieldName], replacer)).join(','))
  ].join('\r\n')
  return csv;
}

async function getHEXDailyStatsFullData(){
  try {
    const resp = await fetch("https://hexdailystats.com/fulldata");
    const data = await resp.json();
    return data;
   } catch (err) {
     console.log("ERROR: " + err + "\n" + err.stack);
   }
}

async function getEthereumBlock(network, day){
  var API = "";
  if (network == ETHEREUM){
    API = BLOCK_SUBGRAPH_API_ETHEREUM;
  } else if (network == PULSECHAIN){
    API = BLOCK_SUBGRAPH_API_PULSECHAIN;
  } else {
    log("ERROR: getEthereumBlock() - Unsupported Network - Exit - " + network);
    return;
  }

  var startTime = 1575417600 + ((day - 2) * 86400);

  return await fetchRetry(API, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: `
      query {
        blocks(
          first: 1, 
          orderBy: timestamp, 
          orderDirection: asc, 
          where: {
            timestamp_gt: ` + startTime + `
          }
        ){
          id
          number
          timestamp
        }
      }` 
    }),
  })
  .then(res => res.json())
  .then(res => {
    var block = res.data.blocks[0];
    return block.number;
  });
}

async function getEthereumBlockLatest(network){
  var API = "";
  if (network == ETHEREUM){
    API = BLOCK_SUBGRAPH_API_ETHEREUM;
  } else if (network == PULSECHAIN){
    API = BLOCK_SUBGRAPH_API_PULSECHAIN;
  } else {
    log("ERROR: getEthereumBlockLatest() - Unsupported Network - Exit - " + network);
    return;
  }

  return await fetchRetry(API, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: `
      query {
        blocks(
          first: 1, 
          orderBy: timestamp, 
          orderDirection: desc, 
        ){
          id
          number
          timestamp
        }
      }` 
    }),
  })
  .then(res => res.json())
  .then(res => {
    var block = res.data.blocks[0];
    return block.number;
  });
}

let queryProcess = async (network, greaterThan, lessThan, blockNumber) => {
  return new Promise(async function (resolve) {
    let list = [];
    let run = true;
    while (run) {
      
      var data = await get_tokenHolders(network, greaterThan, lessThan, blockNumber);
  
      if (data.count <= 0) { break; }
  
      list = list.concat(data.list);
      greaterThan = Number(data.lastNumeralIndex);
      console.log(greaterThan); 
      
      await sleep(1000);
    }
  
    resolve(list);

  });
}

async function get_totalTokenHolders(network, blockNumber){

  let lastNumeralIndex = Number(await get_lastTokenHolderNumeralIndex(network, blockNumber));
  
  let chunksNum = 10;
  let chunkSize = Math.floor(lastNumeralIndex / chunksNum);
  let chunkRemainder = lastNumeralIndex % chunkSize;
  let chunks = [];
  let start = 0;
  let end = chunkSize;
  let promise = {};

  for(let i = 0; i < chunksNum; i++){
    promise = queryProcess(network, start, end, blockNumber);
    start += chunkSize;
    end += chunkSize;
    chunks.push(promise);
  }

  if(chunkRemainder > 0) {
    promise = queryProcess(network, lastNumeralIndex - chunkRemainder, lastNumeralIndex, blockNumber);
    chunks.push(promise);
  }

  let returnList = await Promise.all(chunks).then(lists => { 
    log('Promise.all -- **************************'); 
    let concacttedLists = [];
    for(let i = 0; i < lists.length; i++){
      concacttedLists = concacttedLists.concat(lists[i]);
    } 
    return concacttedLists;
  });
  
  return returnList;
}

async function get_tokenHolders(network, greaterThan, lessThan, blockNumber){
  var API = "";
  if (network == ETHEREUM){
    API = HEX_SUBGRAPH_API_ETHEREUM;
  } else if (network == PULSECHAIN){
    API = HEX_SUBGRAPH_API_PULSECHAIN;
  } else {
    log("ERROR: get_tokenHolders() - Unsupported Network - Exit - " + network);
    return;
  }

  return await fetchRetry(API, {
    method: 'POST',
    highWaterMark: FETCH_SIZE,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: `
      query {
        tokenHolders(first: 1000, orderBy: numeralIndex, 
          block: {number: ` + blockNumber + `},
          where: { 
            numeralIndex_gt: "` + greaterThan + `",
            numeralIndex_lte: "` + lessThan + `",
          }
        ) {
          numeralIndex
          holderAddress
          totalSent
          totalReceived
          tokenBalance
          createdTimeStamp
          createdBlocknumber
          createdHexDay
          lastModifiedHexDay
          lastModifiedTimeStamp
        }
      }` 
    }),
  })
  .then(res => res.json())
  .then(res => {
    var tokenHolderCount = Object.keys(res.data.tokenHolders).length;

    if (tokenHolderCount <= 0) {
      return {  
        count: 0
      };
    } 
    else {
    var list = res.data.tokenHolders;

    var lastNumeralIndex = res.data.tokenHolders[(tokenHolderCount - 1)].numeralIndex;

    var data = {  
      list: list,
      lastNumeralIndex: lastNumeralIndex,
    };

    return data;
  }});
}

async function get_lastTokenHolderNumeralIndex(network, blockNumber){
  var API = "";
  if (network == ETHEREUM){
    API = HEX_SUBGRAPH_API_ETHEREUM;
  } else if (network == PULSECHAIN){
    API = HEX_SUBGRAPH_API_PULSECHAIN;
  } else {
    log("ERROR: get_lastTokenHolderNumeralIndex() - Unsupported Network - Exit - " + network);
    return;
  }

  let $lastNumeralIndex = 0;
  return await fetchRetry(API, {
    method: 'POST',
    highWaterMark: FETCH_SIZE,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: `
      query {
        tokenHolders(first: 10, orderBy: numeralIndex, orderDirection: desc
          block: {number: ` + blockNumber + `},
          where: { 
            numeralIndex_gt: "` + $lastNumeralIndex + `",
          }
        ) {
          numeralIndex
          holderAddress
          totalSent
          totalReceived
          tokenBalance
          createdTimeStamp
          createdBlocknumber
          createdHexDay
          lastModifiedHexDay
          lastModifiedTimeStamp
        }
      }` 
    }),
  })
  .then(res => res.json())
  .then(res => {
    var tokenHolderCount = Object.keys(res.data.tokenHolders).length;

    if (tokenHolderCount <= 0) {
      return {  
        count: 0
      };
    } 
    else {
    var list = res.data.tokenHolders;

    var lastNumeralIndex = res.data.tokenHolders[(tokenHolderCount - 1)].numeralIndex;

    return lastNumeralIndex;
  }});
}

async function get_stakeStartDataHistorical(network, blockNumber){
  var $lastStakeId = 0;
  var list = [];

  while (true) {
    var data = await get_stakeStartsHistorical(network, $lastStakeId, blockNumber);

    if (data.count <= 0) { break; }

    list = list.concat(data.list);
    $lastStakeId = data.lastStakeId;
    console.log(network + " - " + $lastStakeId);

    await sleep(250);
  }

  return list;
}

async function get_stakeStartsHistorical(network, $lastStakeId, blockNumber){
  var API = "";
  if (network == ETHEREUM){
    API = HEX_SUBGRAPH_API_ETHEREUM;
  } else if (network == PULSECHAIN){
    API = HEX_SUBGRAPH_API_PULSECHAIN;
  } else {
    log("ERROR: get_stakeStartsHistorical() - Unsupported Network - Exit - " + network);
    return;
  }

  return await fetchRetry(API, {
    method: 'POST',
    highWaterMark: FETCH_SIZE,
    headers: { 'Content-Type': 'application/json' }, //stakeGoodAccounting: null, stakeEnd: null, 
    body: JSON.stringify({ query: `
      query {
        stakeStarts(first: 1000, orderBy: stakeId, 
          block: {number: ` + blockNumber + `},
          where: { 
            stakeId_gt: "` + $lastStakeId + `",
          }
        ) {
          id
          stakerAddr
          stakeId
          timestamp
          stakedHearts
          stakeShares
          stakeTShares
          stakedDays
          startDay
          endDay
          isAutoStake
          stakeEnd {
            id
            stakerAddr
            stakeId
            payout
            stakedHearts
            stakedShares
            timestamp
            penalty
            servedDays
            daysLate
            daysEarly
          }
          stakeGoodAccounting {
            id
            stakerAddr
            stakeId
            payout
            stakedHearts
            stakedShares
            timestamp
            penalty
            blockNumber
            transactionHash
          }
        }
      }` 
    }),
  })
  .then(res => res.json())
  .then(res => {
    var stakeCount = Object.keys(res.data.stakeStarts).length;

    if (stakeCount <= 0) {
      return {  
        count: 0
      };
    } 
    else {
    var list = res.data.stakeStarts;

    var lastStakeId = res.data.stakeStarts[(stakeCount - 1)].stakeId;

    var data = {  
      list: list,
      lastStakeId: lastStakeId,
    };

    return data;
  }});
}

async function get_priceDataHistorical(network, blockNumber){
  var $lastDate = 0;
  var list = [];

  while (true) {
    var data = await get_pricesHistorical(network, $lastDate, blockNumber);

    if (data.count <= 0) { break; }

    list = list.concat(data.list);
    $lastDate = data.lastDate;
    console.log(network + " - " + $lastDate);

    await sleep(250);
  }

  return list;
}

async function get_pricesHistorical(network, $lastDate, blockNumber){
  var API = "";
  if (network == ETHEREUM){
    log("ERROR: get_pricesHistorical() - Unsupported Network - Exit - " + network);
    return;
  } else if (network == PULSECHAIN){
    API = PULSEX_SUBGRAPH_API_PULSECHAIN;
  } else {
    log("ERROR: get_pricesHistorical() - Unsupported Network - Exit - " + network);
    return;
  }

  return await fetchRetry(API, {
    method: 'POST',
    highWaterMark: FETCH_SIZE,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: `
      query {
        tokenDayDatas (
          first: 1000,
          orderBy: date,
          block: {number: ` + blockNumber + `},
        where: {
          token: "0x2b591e99afe9f32eaa6214f7b7629768c40eeb39",
          date_gt: ` + $lastDate + `,
        })
        {
          date
          token { symbol }
          priceUSD
        }
      }` 
    }),
  })
  .then(res => res.json())
  .then(res => {
    var count = Object.keys(res.data.tokenDayDatas).length;

    if (count <= 0) {
      return {  
        count: 0
      };
    } 
    else {
    var list = res.data.tokenDayDatas;

    var lastDate = res.data.tokenDayDatas[(count - 1)].date;

    var data = {  
      list: list,
      lastDate: lastDate,
    };

    return data;
  }});
}

/*
query {
  tokenDayDatas (
    first: 20,
  	orderBy: date,
  	orderDirection: desc,
  where: {
    token: "0x2b591e99afe9f32eaa6214f7b7629768c40eeb39"
  })
  {
    date
    token { symbol }
    priceUSD
  }
}
*/

io.on('connection', (socket) => {
	log('SOCKET -- ************* CONNECTED: ' + socket.id + ' *************');
  socket.emit("lastUpdated_ETHEREUM", lastUpdated_ETHEREUM);
  socket.emit("lastUpdated_PULSECHAIN", lastUpdated_PULSECHAIN);
});

//////////////////////////////////////
//// HELPER 

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function log(message){
	console.log(new Date().toISOString() + ", " + message);
}

function minTwoDigits(n) {
  return (n < 10 ? '0' : '') + n;
}
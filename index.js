// Get all historical stake data
//// Created for Jose (https://www.reddit.com/user/hex_crypto_bull/)

//Three CSV tables:
//- issued_cds  (stakeStarts)       // Desired: hex_staked>0 and shares>0
//- ended_cds  (stakeEnds)          // Desired: payout>0 and end_date is not null
//- hex_price  (HEXDailyStats API)

var CONFIG = require('./config.json');
var DEBUG = CONFIG.debug;

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
      console.log(`FETCH --- RETRY ${attempt + 1} --- ERROR --- ` + error.toString()); await sleep(500);
      return true;
    } 

    if (response.status >= 400) {
      console.log(`FETCH --- RETRY ${attempt + 1} --- STATUS --- ` + response.status); await sleep(500);
      return true;
    }

    try {
      var response2 = await response.clone().buffer();
      const json = JSON.parse(response2);

      if (json.errors && Object.keys(json.errors).length > 0) {
          if (json.errors[0].message) {
            console.log(`FETCH --- INTERNAL JSON ERROR --- ${attempt + 1} --- ` + json.errors[0].message); await sleep(500);
            return true;
          }
      }
      
      return false;
    } catch (error) {
      console.log(`FETCH --- RETRY ${attempt + 1} --- JSON ---` + error.toString()); await sleep(500);
      return true;
    }
  }
});


const FETCH_SIZE = 1048576;
var grabDataRunning = false;
var lastUpdated = undefined;

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
  if (!grabDataRunning){ grabData(); }
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

io.on('connection', (socket) => {
	log('SOCKET -- ************* CONNECTED: ' + socket.id + ' *************');
  socket.emit("lastUpdated", lastUpdated);
});

//if(!DEBUG){
  const rule30 = new schedule.RecurrenceRule();
  rule30.hour = 0;
  rule30.minute = 30;
  rule30.tz = 'Etc/UTC';
  
  const job30 = schedule.scheduleJob(rule30, function(){
    log('**** DAILY DATA TIMER 30!');
    if (!grabDataRunning){ grabData(); }
  });
//}

/////////////////////////////////////////////////////////////////////////

//test()

var today = undefined;
var day = undefined
var blockNumber = undefined;
var fullData = undefined;
var prices = undefined;
var stakeStarts = undefined;
var stakeEnds = undefined;
var stakeGoodAccountings = undefined;
var stakesActive = undefined;

async function grabData() {
    grabDataRunning = true;
    log("grabData() START");
    
    try {
      day = 6; //703;

      //var blockNumber = await getEthereumBlock(day);
      //console.log(blockNumber);

      today = new Date();
      today.setUTCHours(0, 0, 0, 0);
      var todayTime = parseInt((today.getTime() / 1000).toFixed(0));
      var dayToday = ((todayTime - 1575417600) / 86400) + 2;
      console.log("dayToday: " + dayToday);
      day = dayToday;

      blockNumber = await getEthereumBlock(day); //await getEthereumBlockLatest();
      console.log(blockNumber);

      fullData = await getHEXDailyStatsFullData();

      prices = fullData.map(a => a.priceUV2UV3).reverse();
      var pricesCSV = prices.join('\n');
      //console.log(pricesCSV);
      fs.writeFileSync('./public/hex_price.csv', pricesCSV);
      //return;
      
      console.time('get_stakeStartDataHistorical');
      stakeStarts = await get_stakeStartDataHistorical(blockNumber);
      console.log(stakeStarts);
      console.timeEnd('get_stakeStartDataHistorical');

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
      fs.writeFileSync('./public/issued_cds.csv', stakeStartsCSV);

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
      fs.writeFileSync('./public/ended_cds.csv', stakeEndsCSV);



      stakeGoodAccountings = stakeStarts.filter(a => a.stakeGoodAccounting);
      console.log("stakeGoodAccountings.length: " + stakeGoodAccountings.length);

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
      fs.writeFileSync('./public/goodaccounted_cds.csv', stakeGoodAccountingsCSV);

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
      fs.writeFileSync('./public/active_cds_summed.csv', stakesActiveCSV);
      } catch (error){
        console.log("stakesActive ERROR:");
        console.log(error);
      }


      //lastUpdated = "Day: " + day + ", Block Number: <a href='https://etherscan.io/block/" + blockNumber + "' target='_blank'>" + blockNumber + "</a>";
      lastUpdated = {
        day: day,
        block: blockNumber
      };
      io.emit("lastUpdated", lastUpdated);

    } catch (error){
      log("grabData() ERROR - " + error);
    } finally {
      grabDataRunning = false;
      log("grabData() END");
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

async function getEthereumBlock(day){
  var startTime = 1575417600 + ((day - 2) * 86400);

  return await fetchRetry('https://api.thegraph.com/subgraphs/name/blocklytics/ethereum-blocks', {
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

async function getEthereumBlockLatest(){
  return await fetchRetry('https://api.thegraph.com/subgraphs/name/blocklytics/ethereum-blocks', {
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

async function get_stakeStartDataHistorical(blockNumber){
  var $lastStakeId = 0;
  var list = [];

  while (true) {
    var data = await get_stakeStartsHistorical($lastStakeId, blockNumber);

    if (data.count <= 0) { break; }

    list = list.concat(data.list);
    $lastStakeId = data.lastStakeId;
    console.log($lastStakeId);

    await sleep(250);
  }

  return list;
}

async function get_stakeStartsHistorical($lastStakeId, blockNumber){

  return await fetchRetry('https://api.thegraph.com/subgraphs/name/codeakk/hex', {
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
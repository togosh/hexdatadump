// Get all historical stake data
//// Created for Jose (https://www.reddit.com/user/hex_crypto_bull/)

//Three CSV tables:
//- issued_cds  (stakeStarts)       // Desired: hex_staked>0 and shares>0
//- ended_cds  (stakeEnds)          // Desired: payout>0 and end_date is not null
//- hex_price  (HEXDailyStats API)

const FETCH_SIZE = 1048576;

const fs = require('fs');

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

test()

async function test() {
    var day = 6; //703;

    var blockNumber = await getEthereumBlock(day);
    console.log(blockNumber);

    ////var today = new Date();
    //today.setUTCHours(0, 0, 0, 0);
    //var todayTime = parseInt((today.getTime() / 1000).toFixed(0));
    //var dayToday = ((todayTime - 1575417600) / 86400) + 2;
    //console.log("dayToday: " + dayToday);
    //day = dayToday;

    //var blockNumber = await getEthereumBlockLatest();
    //console.log(blockNumber2);

    var fullData = await getHEXDailyStatsFullData();

    var prices = fullData.map(a => a.priceUV2UV3).reverse();
    var pricesCSV = prices.join('\n');
    //console.log(pricesCSV);
    fs.writeFileSync('hex_price.csv', pricesCSV);
    //return;
    
    console.time('get_stakeStartDataHistorical');
    var { list } = await get_stakeStartDataHistorical(blockNumber);
    //console.log(list);
    console.timeEnd('get_stakeStartDataHistorical');

    var stakeStartsList = [];
    list.forEach(row => {
      var newTimestamp = Number(row.timestamp) + (Number(row.stakedDays)*24*60*60);
      var expectedEndDate = new Date(newTimestamp*1000);
      expectedEndDate.setUTCHours(0, 0, 0, 0);

      //console.log(row.startDay + " " + Number(day - row.startDay) + " " + expectedEndDate.toUTCString().split(", ")[1]);

        var newRow = {
          CD_id:      row.id,
          //stake_id:   row.stakeId,
          address:    row.stakerAddr,

          hex_staked: Number(row.stakedHearts / 100000000),
          shares:     Number(row.stakeShares),

          start_date: (new Date(Number(row.timestamp) * 1000)).toUTCString().split(", ")[1], // Number(row.timestamp),
          //end_date:   null,
          expected_end_date: expectedEndDate.toUTCString().split(", ")[1], // Number(expectedEndDate.getTime() / 1000),

          stake_days:         Number(row.stakedDays),
          actual_days_staked: row.stakeEnd ? Number(row.stakeEnd.servedDays) : Number(day - row.startDay),
          
          ////hex_price_when_issued:  prices[row.startDay - 1],
          //hex_price_when_ended:   null

          //start_day: Number(row.startDay)
        }
        stakeStartsList.push(newRow);
      }
    );

    var stakeStartsCSV = convertCSV(stakeStartsList);
    //console.log(stakeStartsCSV);
    fs.writeFileSync('issued_cds.csv', stakeStartsCSV);

    
    const stakeStartsEnded = list.filter(a => a.stakeEnd);
    //console.log("stakeStartsEnded.length: " + stakeStartsEnded.length);

    var stakeEndsList = [];
    stakeStartsEnded.forEach(row => {

      var endDay = new Date(row.stakeEnd.timestamp*1000);
      endDay.setUTCHours(0, 0, 0, 0);
      var unixtime = parseInt((endDay.getTime() / 1000).toFixed(0));
      //console.log(unixtime);
      var dayFind = ((unixtime - 1575417600) / 86400) + 3;
      //console.log("dayFind: " + dayFind);

      //console.log((new Date(row.stakeEnd.timestamp * 1000)).toUTCString().split(", ")[1]);

        var newRow = {
          CD_id:      row.id,
          //stake_id:   row.stakeId,
          //address:    row.stakerAddr,

          payout:     Number(row.stakeEnd.payout),
          penalty:    Number(row.stakeEnd.penalty),

          //hex_staked: Number(row.stakedHearts / 100000000),
          //shares:     Number(row.stakeShares),

          //start_date:         Number(row.timestamp),
          end_date:           (new Date(Number(row.stakeEnd.timestamp) * 1000)).toUTCString().split(", ")[1],
          //expected_end_date:  null,

          //stake_days:         Number(row.stakedDays),
          //actual_days_staked: Number(row.stakeEnd.servedDays),

          ////hex_price_when_issued:  prices[row.startDay - 1],
          ////hex_price_when_ended:   prices[dayFind - 1],

          //start_day: Number(row.startDay),
          //end_day: dayFind,
        };
        stakeEndsList.push(newRow);
      }
    );
    //console.log(stakeEndsList);

    var stakeEndsCSV = convertCSV(stakeEndsList);
    //console.log(stakeEndsCSV);
    fs.writeFileSync('ended_cds.csv', stakeEndsCSV);
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

  return {
    list: list
  }
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

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
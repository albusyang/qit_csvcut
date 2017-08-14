var readLine = require('lei-stream').readLine;
var writeLine = require('lei-stream').writeLine;

var outputPath = 'F:/historical_data/COMEX_AUFC1_n/';
var inputCsv = 'F:/待处理-新历史数据/bo.li@thomsonreuters.com-test-COMEX-N152291441-part002.csv';
var counter = 0;
var startTime = Date.now();
var output = null;
var forWriteLine = null;
var code = null;
var currentFileMonthYear = null;
var newCsvFile = '';
var codeMap = new Map();
codeMap.set('SAGcv1', 'SHFE_AGFC1');
codeMap.set('SAGcv2', 'SHFE_AGFC2');
codeMap.set('SHAUcv1', 'SHFE_AUFC1');
codeMap.set('SHAUcv2', 'SHFE_AUFC2');
codeMap.set('GCcv1', 'COMEX_AUFC1');
codeMap.set('SIcv1', 'COMEX_AGFC1');

// 每个文件的第一行
var firstLine = '#RIC,Current RIC,Date[G],Time[G],GMT Offset,Type,Ex/Cntrb.ID,Price,Volume,Market VWAP,Buyer ID,Bid Price,Bid Size,No. Buyers,Seller ID,Ask Price,Ask Size,No. Sellers,Qualifiers,Seq. No.,Exch Time,Block Trd,Floor Trd,PE Ratio,Yield,Base Price,Bid Imp. Vol,Ask Imp. Vol,Imp. Vol.,Prim Act.,Sec. Act.,Gen Val1,Gen Val2,Gen Val3,Gen Val4,Gen Val5,Crack,Top,Freight Pr.,Trd/Qte Date,Quote Time,Bid Tic,Tick Dir.,Div Code,Adj. Close,Prc TTE Flag,Irg TTE Flag,Prc SubMkt Id,Irg SubMkt Id,Div Ex Date,Div Pay Date,Div Amt.,Open,High,Low,Acc. Volume,Turnover,Imputed Cls,Volatility,Strike,Premium,Auc Price (unused),Auc Vol (unused),Mid Price,Fin Eval. Price,Prov Eval. Price,Percentage Change,Contract Physical Units,Miniumum quantity of a contract,Number of Physicals,12 Months EPS' + '\n';

var init = function () {
    console.log('start!');
    readLine(inputCsv).go(function (data, next) {
        counter++;
        if (counter % 100000 === 0) {
            printSpeedInfo();
        }
        getOneLine(data + '\r');
        next(); // 读取下一行
    }, function () {
        console.log('done. total %s lines, spent %sS', counter, msToS(getSpentTime()));
        output.end();
    });
}

var getOneLine = function (data) {
    var part = ''; //每个逗号间隔的数据
    var rowData = []; //存放一行数据
    if (data != null) {
        var i = 0;
        for (var j = 0; j < data.length; j++) {
            if (data[j] != ',') {
                part += data[j];
            } else if (data[j] == ',') {
                rowData[i] = part;
                i++;
                part = '';
            }
            if (data[j] == '\r') {
                rowData[i] = part;
                pickUp(rowData);
            }
        }
    }
}

var pickUp = function (data) {
    var curLineMonthYear = getMonthYear(data[2]); // 获取当前行的年月
    if (data[0] == '#RIC') {
        return;
        //forWriteLine = firstLine;
    } else {
        var tempCode = getTempCode(data[0]);
        // 第一次读取文件，code为空时
        if (code == null) {
            code = tempCode;
            currentFileMonthYear = curLineMonthYear;
            newCsvFile = outputPath + code + '-' + currentFileMonthYear + '.csv';
            forWriteLine = firstLine + data;
            output = writeLine(newCsvFile, {
                cacheLines: 100000
            });
            writeFile(forWriteLine);
            return;
        }
        if (code != tempCode) {
            currentFileMonthYear = curLineMonthYear;
            output.end(); // 品种交替，将上一个文件写好，开始新的
            code = tempCode;
            newCsvFile = outputPath + code + '-' + curLineMonthYear + '.csv';
            forWriteLine = firstLine + data;
            output = writeLine(newCsvFile, {
                cacheLines: 100000
            });
            writeFile(forWriteLine);
            return;
        }
        if (currentFileMonthYear != curLineMonthYear) {
            currentFileMonthYear = curLineMonthYear;
            output.end();
            code = tempCode;
            newCsvFile = outputPath + code + '-' + currentFileMonthYear + '.csv';
            forWriteLine = firstLine + data;
            output = writeLine(newCsvFile, {
                cacheLines: 100000
            });
            writeFile(forWriteLine);
            return;
        }
        writeFile(data);
    }
}

/*
*   写入文件函数
*   param： forWriteLine  用来写入的行
*/
var writeFile = function (forWriteLine) {
    if (forWriteLine != null) {
        output.write(forWriteLine);
        forWriteLine = null;
    }
}

/*
*   日期造型
*/
var getMonthYear = function (date) {
    var thereDate = '';    //获取原始日期
    for (var i = 0; i < 6; i++) {
        thereDate += date[i];
    }
    return thereDate;
}

var getTempCode = function (data) {
    if (codeMap.get(data) != null) {
        return codeMap.get(data);
    }
    var tempCodeHead = '';
    var tempCodeTail = '';
    var judge = false;
    for (var i = 0; i < data.length; i++) {
        if (data[i] == 'X') {
            continue;
        }
        if (data[i] == '=') {
            judge = true;
            continue;
        }
        if (judge == true) {
            if (tempCodeHead == '') {
                tempCodeHead = data[i];
            } else {
                tempCodeHead += data[i];
            }
        }
        if (judge == false) {
            if (tempCodeTail == '') {
                tempCodeTail = data[i];
            } else {
                tempCodeTail += data[i];
            }
        }
    }
    return tempCodeHead + '_' + tempCodeTail;
}

var msToS = function (v) {
    return parseInt(v / 1000, 10);
}
var getSpentTime = function () {
    return Date.now() - startTime;
}

var printSpeedInfo = function () {
    var t = msToS(getSpentTime());
    var s = counter / t;
    if (!isFinite(s)) {
        s = counter;
    }
    console.log('read %s lines, speed: %sL/S', counter, s.toFixed(0));
}

init();
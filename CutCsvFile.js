var readLine = require('lei-stream').readLine;
var writeLine = require('lei-stream').writeLine;

var inputCsv = '';
var output = null;
var forWriteLine = '';
// 每个文件的第一行
var firstLine = '\"' + 'CODE' + '\"' + ',' +
    '\"' + 'FIELD_GROUP' + '\"' + ',' +
    '\"' + 'FIELD_VALUES' + '\"' + ',' +
    '\"' + 'TIME_IN_LOCAL' + '\"' + ',' +
    '\"' + 'TIME_IN_GMT' + '\"' + ',' +
    '\"' + 'SOURCE_FROM' + '\"' + ',' +
    '\"' + 'CREATED_BY' + '\"' + ',' +
    '\"' + 'CREATED_TIME' + '\"' + ',' +
    '\"' + 'UPDATED_BY' + '\"' + ',' +
    '\"' + 'UPDATED_TIME' + '\"' + ',' +
    '\"' + 'TIME_IN_SECOND' + '\"';

var init = function () {
    newCsv = 'newcsvPath + newcsvName';
    output = writeLine(newCsv, {
        cacheLines:61000
    });
    readLine(inputCsv).go(function (data, next) {
        counter++;
        output.write(data);
        next();
    }, function () {
        output.end();
    });
}

var getOneLine = function (data) {
    var part = ''; //每个逗号间隔的数据
    var rowData = ''; //存放一行数据
    if (data != null) {
        var i = 0;
        for (var j = 0; j < data.length; j++) {
            if (data[j] != ',') {
                part += data[j];
            } else if (data[j] == ',') {
                rowData[i] = part;
                i++;
                part = '';
            } else if (data[j] == '\n') {
                rowData[i] = part;
                // pickUP
            }
        }
    }
}

var pickUp = function (data) {
    if (data[0] == '#RIC') {
        forWriteLine = firstLine;
    } else {
        
    }
}
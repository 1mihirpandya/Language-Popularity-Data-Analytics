
function line_data_to_dataset(data, lang){
    return {
        'label': lang,
        'backgroundColor': window.backgroundColors[lang],
        'borderColor': window.borderColors[lang],
        'fill': false,
        'data': data
    }
}

function make_line_config(labels, xlabel, datasets, title){
    return {
        type: 'line',
        data: {
            labels: labels,
            datasets: datasets
        },
        options: {
            responsive: true,
            title: {
                display: true,
                text: title
            },
            tooltips: {
                mode: 'point',
                intersect: false,
            },
            hover: {
                mode: 'nearest',
                intersect: true
            },
            scales: {
                xAxes: [{
                    display: true,
                    scaleLabel: {
                        display: true,
                        labelString: xlabel
                    }
                }],
                yAxes: [{
                    display: true,
                    scaleLabel: {
                        display: true,
                        labelString: 'Value'
                    }
                }]
            }
        }
    };
}

var MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

var DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

var DATES = []

var curDate = new Date(2016, 0, 1);
//https://stackoverflow.com/questions/23593052/format-javascript-date-to-yyyy-mm-dd
function formatDate(d){
    var month = ''+(d.getMonth()+1)
    var date = ''+(d.getDate())
    var year = ''+(d.getFullYear())
    if (month.length < 2) month = '0' + month;
    if (date.length < 2) date = '0' + date;
    return [month, date, year].join('/')
}

while(curDate.getFullYear() === 2016){
    DATES.push(formatDate(curDate))
    curDate.setDate(curDate.getDate()+1)
}

var date_to_index = {}
for(let i = 0; i < DATES.length; i++){
    date_to_index[DATES[i]] = i;
}

function get_date_config(date_data, chart_name){
    let date_data_fmt = {}
    for (let lang of LANGS){
        //https://stackoverflow.com/a/47700998/5187393
        date_data_fmt[lang] = [...new Array(366)];
    }
    for (let o of date_data){
        var date = o['date'];
        for(var key in o){
            if(key !== 'date'){
                date_data_fmt[key.trim()][date_to_index[date]] = o[key]
            }
        }
    }

    let date_ds = [] 

    for(var key in date_data_fmt){
        date_ds.push(line_data_to_dataset(date_data_fmt[key], key))
    }
        //console.log(date_ds)
    return make_line_config(DATES, 'Date', date_ds, chart_name)
}

function get_month_config(month_data, chart_name){
    var month_data_fmt = {}
    for (let lang of LANGS){
        //https://stackoverflow.com/a/47700998/5187393
        month_data_fmt[lang] = [...new Array(12)];
    }
    console.log(month_data_fmt)
    for (let o of month_data){
        var month = o['month'];
        for(var key in o){
            if(key !== 'month'){
                month_data_fmt[key][month-1] = o[key]
            }
        }
    }


    var month_ds = [] 

    for(var key in month_data_fmt){
        month_ds.push(line_data_to_dataset(month_data_fmt[key], key))
    }

    return make_line_config(MONTHS, 'Month', month_ds, chart_name)
}

function get_weekday_config(weekday_data, chart_name){
    var day_data_fmt = {}
    for (let lang of LANGS){
        //https://stackoverflow.com/a/47700998/5187393
        day_data_fmt[lang] = [...new Array(7)];
    }
    for (let o of weekday_data){
        var day = o['day'];
        for(var key in o){
            if(key !== 'day'){
                day_data_fmt[key][day] = o[key]
            }
        }
    }


    var weekday_ds = [] 

    for(var key in day_data_fmt){
        weekday_ds.push(line_data_to_dataset(day_data_fmt[key], key))
    }

    return make_line_config(DAYS, 'Day', weekday_ds, chart_name)
}

var dice_date_line_config = get_date_config(dice_date_data, 'Dice Daily')
var so_date_line_config = get_date_config(so_date_data, 'Stackoverflow Daily')
var gh_date_line_config = get_date_config(github_date_data, 'GitHub Daily')

var dice_month_line_config = get_month_config(dice_month_data, 'Dice Monthly')
var so_month_line_config = get_month_config(so_month_data, 'Stackoverflow Monthly')
var gh_month_line_config = get_month_config(github_month_data, 'GitHub Monthly')

var dice_weekday_line_config = get_weekday_config(dice_weekday_data, 'Dice Weekday')
var so_weekday_line_config = get_weekday_config(so_weekday_data, 'Stackoverflow Weekday')
var gh_weekday_line_config = get_weekday_config(github_weekday_data, 'GitHub Weekday')
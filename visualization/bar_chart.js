var tz_to_index = {'PST': 0, 'MST': 1, 'CST': 2, 'EST': 3}

function bar_data_to_dataset(data, lang){
    return {
        'label': lang,
        'backgroundColor': window.borderColors[lang],
        //'borderColor': window.borderColors[lang],
        'yAxisID': 'y-axis-1',
        'data': data
    }
}
function getBarChartConfig(timezone_data, title){

    var bar_data_fmt = {}
    //console.log(LANGS)
    for (let lang of LANGS){
        bar_data_fmt[lang] = [0, 0, 0, 0];
    }

    console.log(bar_data_fmt)
    for (let o of timezone_data){
        var time_zone = o['timezone'];
        for(var key in o){
            if(key !== 'timezone'){
                console.log(key)
                bar_data_fmt[key.trim()][tz_to_index[time_zone]] = o[key]
            }
        }
    }

    var bar_ds = [] 

    for(var key in bar_data_fmt){
        bar_ds.push(bar_data_to_dataset(bar_data_fmt[key], key))
    }

    var barChartData = {
        labels: ['PST', 'MST', 'CST', 'EST'],
        datasets: bar_ds
    };

   return {
        type: 'bar',
        data: barChartData,
        options: {
            responsive: true,
            title: {
                display: true,
                text: title
            },
            tooltips: {
                mode: 'point',
                intersect: true
            },
            scales: {
                 xAxes: [{
                        display: true,
                        scaleLabel: {
                            display: true,
                            labelString: 'Timezone'
                        }
                    }],
                yAxes: [{
                    type: 'linear', // only linear but allow scale type registration. This allows extensions to exist solely for log scale for instance
                    display: true,
                    position: 'left',
                    id: 'y-axis-1',
                }, /*{
                    type: 'linear', // only linear but allow scale type registration. This allows extensions to exist solely for log scale for instance
                    display: true,
                    position: 'right',
                    id: 'y-axis-2',
                    gridLines: {
                        drawOnChartArea: false
                    }
                }*/],
            }
        }
    }
}

var dice_bar_config = getBarChartConfig(dice_timezone_data, 'Dice Timezone')

var gh_bar_config = getBarChartConfig(github_timezone_data, 'GitHub Timezone')

var so_bar_config = getBarChartConfig(so_timezone_data, 'Stackoverflow Timezone')
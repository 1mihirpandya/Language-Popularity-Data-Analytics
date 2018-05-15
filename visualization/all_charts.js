window.onload = function() {
    var dice_pie_ctx = document.getElementById("dice-pie-chart").getContext('2d');
    window.dicePie = new Chart(dice_pie_ctx, dice_pie_config)
    var github_pie_ctx = document.getElementById("github-pie-chart").getContext('2d');
    window.ghPie = new Chart(github_pie_ctx, gh_pie_config)
    var so_pie_ctx = document.getElementById("so-pie-chart").getContext('2d');
    window.soPie = new Chart(so_pie_ctx, so_pie_config)


    var dice_date_line_ctx = document.getElementById('dice-date-line-graph').getContext('2d');
    window.diceDateLine = new Chart(dice_date_line_ctx, dice_date_line_config);
    var gh_date_line_ctx = document.getElementById('github-date-line-graph').getContext('2d');
    window.diceDateLine = new Chart(gh_date_line_ctx, gh_date_line_config);
    var so_date_line_ctx = document.getElementById('so-date-line-graph').getContext('2d');
    window.soDateLine = new Chart(so_date_line_ctx, so_date_line_config);

    var dice_month_line_ctx = document.getElementById('dice-month-line-graph').getContext('2d');
    window.diceMonthLine = new Chart(dice_month_line_ctx, dice_month_line_config);
    var gh_month_line_ctx = document.getElementById('github-month-line-graph').getContext('2d');
    window.ghMonthLine = new Chart(gh_month_line_ctx, gh_month_line_config);
    var so_month_line_ctx = document.getElementById('so-month-line-graph').getContext('2d');
    window.soMonthLine = new Chart(so_month_line_ctx, so_month_line_config);
    
    var dice_weekday_line_ctx = document.getElementById('dice-weekday-line-graph').getContext('2d');
    window.diceWeekdayLine = new Chart(dice_weekday_line_ctx, dice_weekday_line_config);
    var gh_weekday_line_ctx = document.getElementById('github-weekday-line-graph').getContext('2d');
    window.ghWeekdayLine = new Chart(gh_weekday_line_ctx, gh_weekday_line_config);
    var so_weekday_line_ctx = document.getElementById('so-weekday-line-graph').getContext('2d');
    window.soWeekdayLine = new Chart(so_weekday_line_ctx, so_weekday_line_config);

    var dice_bar_ctx = document.getElementById("dice-bar-chart").getContext('2d');
    window.diceBar = new Chart(dice_bar_ctx, dice_bar_config)
    var gh_bar_ctx = document.getElementById("github-bar-chart").getContext('2d');
    window.ghBar = new Chart(gh_bar_ctx, gh_bar_config)
    var so_bar_ctx = document.getElementById("so-bar-chart").getContext('2d');
    window.soBar = new Chart(so_bar_ctx, so_bar_config)
};
/*
document.getElementById('randomizeData').addEventListener('click', function() {
    config.data.datasets.forEach(function(dataset) {
        dataset.data = dataset.data.map(function() {
            return randomScalingFactor();
        });

    });

    window.myLine.update();
});

var colorNames = Object.keys(window.chartColors);

document.getElementById('addDataset').addEventListener('click', function() {
    var colorName = colorNames[config.data.datasets.length % colorNames.length];
    var newColor = window.chartColors[colorName];
    var newDataset = {
        label: 'Dataset ' + config.data.datasets.length,
        backgroundColor: newColor,
        borderColor: newColor,
        data: [],
        fill: false
    };

    for (var index = 0; index < config.data.labels.length; ++index) {
        newDataset.data.push(randomScalingFactor());
    }

    config.data.datasets.push(newDataset);
    window.myLine.update();
});

document.getElementById('addData').addEventListener('click', function() {
    if (config.data.datasets.length > 0) {
        var month = MONTHS[config.data.labels.length % MONTHS.length];
        config.data.labels.push(month);

        config.data.datasets.forEach(function(dataset) {
            dataset.data.push(randomScalingFactor());
        });

        window.myLine.update();
    }
});

document.getElementById('removeDataset').addEventListener('click', function() {
    config.data.datasets.splice(0, 1);
    window.myLine.update();
});

document.getElementById('removeData').addEventListener('click', function() {
    config.data.labels.splice(-1, 1); // remove the label first

    config.data.datasets.forEach(function(dataset) {
        dataset.data.pop();
    });

    window.myLine.update();
});

*/
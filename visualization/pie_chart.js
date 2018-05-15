function get_pie_config(pie_data, title){
    var pie_ds = []
    var pie_labels = []
    var pie_bg = []
    var pie_border = []
    for(var lang in pie_data){
        pie_ds.push(pie_data[lang])
        pie_labels.push(lang)
        pie_bg.push(window.backgroundColors[lang])
        pie_border.push(window.borderColors[lang])
    }

    return {
        type: 'doughnut',
        data: {
            datasets: [{
                data: pie_ds,
                backgroundColor: pie_bg,
                borderColor: pie_border,
                //label: 'lol'
            }],
            labels: pie_labels
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
        }
    };
}

var dice_pie_config = get_pie_config(dice_pie_data, 'Dice Language')
var so_pie_config = get_pie_config(so_pie_data, 'Stackoverflow Language')
var gh_pie_config = get_pie_config(github_pie_data, 'Github Language')
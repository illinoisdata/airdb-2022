var dataAmount = [0.1, 1, 5, 10];

$(document).ready(function() {
    // get dataset
    $.ajax({
        url: "/dataset",
        type: "GET",
        success: function(dataset) {
            dataset.forEach(function(item, index) {
                $("#dataset-dropdown").append($(new Option(item, index)));
            })
        }
    });
    buildProfileChart();
});

// reset upon dataset change
$("#dataset-dropdown").change(function() {
    d3.select("#diy-diagram").selectAll("*").remove();
    d3.select("#airindex-diagram").selectAll("*").remove();
})

// built each layer input
$("#diy-layer").on('change', function() {
    $("#layer-input").children().not(":first-child").remove();
    for (let i = $(this).val(); i > 0; i--) {
        var parent = $("<div>", {id: "layer-" + i, "class": "row pt-3"});
        var layer = $("<div>", {"class": "col-2 text-center"});
        layer.append(i);
        var funcDiv = $("<div>", {"class": "col-5"});
        var dropdown = $("<select>", {"class": "custom-select"});
        dropdown.append('<option selected></option>')
        dropdown.append($(new Option("Linear", "linear")));
        dropdown.append($(new Option("Step", "step")));
        funcDiv.append(dropdown);
        var precisionDiv = $("<div>", {"class": "col-5"});
        precisionDiv.append('<input type="text" class="form-control">');
        parent.append(layer);
        parent.append(funcDiv);
        parent.append(precisionDiv);
        $("#layer-input").append(parent);
    }
});

// build storage profile
$("#profile-time").on('change', function() {
    $('#time-value').html($(this).val() + 's');
    buildProfileChart();
})

$("#profile-storage").on('change', function() {
    $('#storage-value').html($(this).val() + 'GB/s');
    buildProfileChart();
})

function buildProfileChart() {
    // get time data
    var latency = parseFloat($("#profile-time").val());
    var bandwidth = parseFloat($("#profile-storage").val());
    time = dataAmount.map(d => latency + d / bandwidth);
    // remove the previous chart
    var prevChart = Chart.getChart("profile-chart");
    if (prevChart) {
        prevChart.destroy();
    }
    // create new chart
    const labels = dataAmount.map(d => d + "GB");
    const data = {
        labels: labels,
        datasets: [{
            label: 'Storage Profile',
            data: time,
            borderColor: 'rgb(255, 165, 0)',
        }]
    }
    const config = {
        type: 'line',
        data: data,
        options: {
            scales: {
                y: {
                    ticks: {
                        callback: function(value, index, ticks) {
                            return value + " s";
                        }
                    }
                }
            }
        }
    };
    const chart = new Chart($("#profile-chart"), config);
}

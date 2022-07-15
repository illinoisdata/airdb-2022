var data = [
    {
    "storage": 1,
    "time": 1.5
    },
    {
    "storage": 2,
    "time": 1.5
    },
    {
    "storage": 3,
    "time": 2
    },
    {
    "storage": 4,
    "time": 3
    }
];

$(document).ready(function() {
    var container = d3.select("#profile-chart");
    var metadata = container.node().getBoundingClientRect();
    var x = d3.scaleBand().domain(["1B", "1KB", "1MB", "1GB"]).range([0, metadata.width - 50]);
    var y = d3.scaleBand().domain(["1s", "1ms", "1us", "1ns"]).range([0, metadata.height - 40]);
    var x_axis = d3.axisBottom().scale(x);
    var y_axis = d3.axisLeft().scale(y);
    container.append("g")
        .attr("transform", "translate(40," + (metadata.height - 30) + ")")
        .call(x_axis);
    container.append("g")
        .attr("transform", "translate(40, 10)")
        .call(y_axis)
        .selectAll("text")
        .style("text-anchor", "end")
        .style("dominant-baseline", "auto");
});

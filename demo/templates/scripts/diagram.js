var data = [
  {
  "text": "piecewise linear, 336 B",
  "color": "orange",
  "line": "line"
  },
  {
  "text": "piecewise step, 28.6 KB",
  "color": "black",
  "line": "arrow"
  },
  {
  "text": "data layer, 1.6 GB",
  "color": "black",
  "line": "none"
  }
];

$(document).ready(function() {
  createDiagram("#diy-diagram", data);
  createDiagram("#airindex-diagram", data);
});

function createDiagram(id, data) {
  
  var container = d3.select(id);

  // rectangle
  var rectangle = container
    .selectAll("rect")
    .data(data)
    .enter()
    .append("rect")
    .attr("y", function(d, i) {
        return 10 * (i + 1) + "%";
    })
    .attr("stroke", function(d) {
        return d.color;
    });

  // text
  container.selectAll('text')
    .data(data)
    .enter()
    .append('text')
    .text(function(d) {
      return d.text;
    })
    .attr("x", "50%")
    .attr("y", function(d, i) {
        return 2.5 + 10 * (i + 1) + "%";
    });
  
  // line
  container.selectAll('line')
    .data(data)
    .enter()
    .append('line')
    .attr('id', function(d, i) {
      return "line-" + i;
    }); 
  data.map(function(d, i) {
    if (d.line !== "none") {
      container.select('#line-' + i)
        .attr("x1", "50%")
        .attr("y1", 5 + 10 * (i + 1) + "%")
        .attr("x2", "50%")
        .attr("y2", 10 + 10 * (i + 1) + "%");
    }
    if (d.line === "arrow") {
      container.select('#line-' + i)
        .attr("marker-end", "url(#arrow-head)");
    }
  })

}
    
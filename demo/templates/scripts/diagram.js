$(document).ready(function() {
  $("#diy-button").click(function() {
    onClickForDiagram("#diy-loader", "#diy-diagram");
  });
  $("#airindex-button").click(function() {
    onClickForDiagram("#airindex-loader", "#airindex-diagram");
  });
});

function onClickForDiagram(loader, id) {
  $.ajax({
    beforeSend: function() {
      $(loader).removeClass('d-none');
    },
    complete: function() {
      $(loader).addClass('d-none');
    },
    url: "/diagram",
    type: "GET",
    success: function(data) {
      createDiagram(id, data);
    }
  });
}

function createDiagram(id, data) {
  
  var container = d3.select(id);

  // rectangle
  var rectangle = container
    .selectAll("rect")
    .data(data)
    .enter()
    .append("rect")
    .attr("y", function(d, i) {
        return 50 * (i + 1);
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
        return 12.5 + 50 * (i + 1);
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
        .attr("y1", 25 + 50 * (i + 1))
        .attr("x2", "50%")
        .attr("y2", 50 + 50 * (i + 1));
    }
    if (d.line === "arrow") {
      container.select('#line-' + i)
        .attr("marker-end", "url(#arrow-head)");
    }
  })

}
    
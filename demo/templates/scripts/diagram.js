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
    url: "/tune",
    type: "GET",
    success: function(data) {
      createDiagram(id, combineInput(data));
    }
  });
}

function combineInput(data) {
  var numLayer = $("#diy-layer").val();
  var finalInput = [];
  var idx = 0;
  for (let i = numLayer; i > 0; i--) {
    finalInput[idx] = "piecewise " + $(`#layer-${i} select`).val() + ", " + data[Math.floor(idx / 2)];
    finalInput[idx + 1] = "\u0394 <= " + $(`#layer-${i} input`).val() + "B";
    idx = idx + 2;
  }
  finalInput[idx] = "data layer, " + data[data.length - 1];
  return finalInput;
}

function createDiagram(id, data) {
  
  var container = d3.select(id);

  // rectangle
  var rectangle = container
    .selectAll("rect")
    .data(data)
    .enter()
    .append("rect")
    .attr("width", function(d, i) {
      if (i % 2 == 0) {
        return "70%";
      } else {
        return "40%";
      }
    })
    .attr("x", function(d, i) {
      if (i % 2 == 0) {
        return "15%";
      } else {
        return "30%";
      }
    })
    .attr("y", function(d, i) {
      return 50 * (i + 1);
    })
    .attr("stroke", function(d, i) {
      if (i == 0 || i % 2 != 0) {
        return "orange";
      } else {
        return "black";
      }
    });

  // text
  container.selectAll('text')
    .data(data)
    .enter()
    .append('text')
    .text(function(d) {
      return d;
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
    if (i != data.length - 1) {
      container.select('#line-' + i)
        .attr("x1", "50%")
        .attr("y1", 25 + 50 * (i + 1))
        .attr("x2", "50%")
        .attr("y2", 50 + 50 * (i + 1));
    }
    if (i % 2 != 0) {
      container.select('#line-' + i)
        .attr("marker-end", "url(#arrow-head)");
    }
  })

}
    
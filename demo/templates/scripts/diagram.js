$(document).ready(function() {
  $("#diy-button").click(function() {
    onClickForDiagram("#diy-loader", "#diy-diagram", "#diy-time");
  });
  $("#airindex-button").click(function() {
    onClickForDiagram("#airindex-loader", "#airindex-diagram", "#airindex-time");
  });
});

function onClickForDiagram(loader, id, timeId) {
  var success = true;
  if ($('select[id="dataset-dropdown"')[0].selectedIndex === 0) {
    success = false;
  }
  let input = {
    "dataset": $("#dataset-dropdown").find(":selected").text(),
    "latency": parseInt($("#profile-time").val()),
    "bandwidth": parseInt($("#profile-storage").val()),
    "affine":  $("#affine-check").is(":checked")
  }
  var postUrl;
  if (id.includes("diy")) {
    if ($('input[name=select-radio]:checked').val() === "Custom") {
      let numberOfLayers = $("#diy-layer").val();
      let funcTypes = []
      let deltas = []
      for (let i = numberOfLayers; i > 0; i--) {
        if ($(`#layer-${i} select`)[0].selectedIndex === 0) {
          success = false;
        }
        funcTypes.push($(`#layer-${i} select`).val());
        if ($(`#layer-${i} input`).val().trim() === "") {
          success = false;
        }
        deltas.push(parseInt($(`#layer-${i} input`).val()));
      }
      input["func"] = funcTypes;
      input["delta"] = deltas;
      postUrl = "/diyCustom";
    } else {
      postUrl = "/diyBTree";
    }
  } else {
    postUrl = "/airindex";
  }
  if (success) {
    $.ajax({
      beforeSend: function() {
        $(loader).removeClass('d-none');
      },
      complete: function() {
        $(loader).addClass('d-none');
      },
      url: postUrl,
      type: "POST",
      data: JSON.stringify(input),
      contentType: "application/json",
      success: function(data) {
        // data: list [datasetsize, func, delta, layersize, time_ns]
        createDiagram(id, combineInput(data));
        setLookupTime(timeId, data[4]);
      }
    });
  } else {
    alert("Please fill out all the fields.");
  }
}

function convert_data_size_to_string(size_in_b) {
  if (size_in_b > 1073741824) {
    return (size_in_b / 1073741824).toPrecision(3) + "GB";
  } else if (size_in_b > 1048576) {
    return (size_in_b / 1048576).toPrecision(3) + "MB";
  } else if (size_in_b > 1024) {
    return (size_in_b / 1024).toPrecision(3) + "KB";
  } else {
    return size_in_b + "B";
  }
}

function combineInput(output) {
  let dataset_size = output[0];
  var functions = output[1];
  var delta = output[2];
  var data = output[3];
  var finalInput = [];
  for (let i = 0; i < functions.length; i++) {
    finalInput[2 * i] = "piecewise " + functions[i] + ", " + convert_data_size_to_string(data[i]);
    finalInput[2 * i + 1] = "\u0394 <= " + delta[i] + "B";
  }
  finalInput.push("data layer, " + convert_data_size_to_string(dataset_size));
  return finalInput;
}

function createDiagram(id, data) {
  
  d3.select(id).selectAll("*").remove();
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

function convert_time_to_string(time_ns) {
  if (time_ns > 1e9) {
    return (time_ns / 1e9).toPrecision(3) + "s";
  } else if (time_ns > 1e6) {
    return (time_ns / 1e6).toPrecision(3) + "ms";
  } else if (time_ns > 1e3) {
    return (time_ns / 1e3).toPrecision(3) + "us";
  } else {
    return time_ns + "ns";
  }
}

function setLookupTime(id, time_ns) {
  $(id + " br")[0].nextSibling.nodeValue = convert_time_to_string(time_ns);
}

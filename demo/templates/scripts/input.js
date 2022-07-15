var dataset = [
    "books_800M_uint64",
    "fb_200M_uint64",
    "osm_cellids_800M_uint64",
    "wiki_ts_200M_uint64"
];

$(document).ready(function() {
    dataset.forEach(function(item, index) {
        $("#dataset-dropdown").append($(new Option(item, index)));
    })
});

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
})

var dataset = [
    "wiki_ts_200M_uint64",
    "books_800M_uint64",
    "fb_200M_uint64",
    "osm_cellids_800M_uint64",
    "wiki_ts_200M_uint64"
];

$(document).ready(function() {   
    dataset.forEach(function(item, index) {
        $("#dataset").append($(new Option(item, index)));
    })
});

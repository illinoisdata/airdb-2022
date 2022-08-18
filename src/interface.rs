use pyo3::prelude::*;
use std::collections::HashMap;

#[pyfunction]
fn get_dataset() -> PyResult<Vec<String>> {
    let dataset: Vec<String> = 
        ["books_800M_uint64".to_owned(),
        "fb_200M_uint64".to_owned(),
        "osm_cellids_800M_uint64".to_owned(),
        "wiki_ts_200M_uint64".to_owned()].to_vec();
    Ok(dataset)
}

#[pyfunction]
fn tune_data(param: HashMap<String, Vec<String>>) -> PyResult<HashMap<String, Vec<String>>> {
    let mut output: HashMap<String, Vec<String>> = HashMap::new();
    output.insert("func".to_owned(), param["func"].to_owned());
    output.insert("delta".to_owned(), param["delta"].to_owned());
    let data: Vec<String> = 
        ["336 B".to_owned(),
        "28.6 KB".to_owned(),
        "6.5 MB".to_owned(),
        "1.6 GB".to_owned()].to_vec();
    output.insert("data".to_owned(), data);
    Ok(output)
}

#[pymodule]
#[pyo3(name = "airindex")]
fn interface(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_dataset, m)?)?;
    m.add_function(wrap_pyfunction!(tune_data, m)?)?;
    Ok(())
}

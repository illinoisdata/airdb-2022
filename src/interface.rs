use pyo3::prelude::*;

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
fn tune_data(data: Vec<String>) -> PyResult<Vec<String>> {
    Ok(data)
}

#[pymodule]
#[pyo3(name = "airindex")]
fn interface(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_dataset, m)?)?;
    m.add_function(wrap_pyfunction!(tune_data, m)?)?;
    Ok(())
}

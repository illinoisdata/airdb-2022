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
fn tune_diy_custom(_dataset: String, _affine: bool, _latency: u64, _bandwidth: f64, func: Vec<String>, delta: Vec<u64>) -> PyResult<Vec<PyObject>> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let datasetsize_b = 1717987000;
    let layersize: Vec<u64> = [336, 29286, 6815744].to_vec();
    let time_ns: u64 = 12345678;
    Ok(vec![datasetsize_b.to_object(py), func.to_object(py), delta.to_object(py), layersize.to_object(py), time_ns.to_object(py)])
}

#[pyfunction]
fn tune_diy_btree(_dataset: String, _affine: bool, _latency: u64, _bandwidth: f64) -> PyResult<Vec<PyObject>> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let datasetsize_b = 1717987000;
    let func: Vec<String> = 
        ["Step".to_owned(),
        "Linear".to_owned(),
        "Linear".to_owned()].to_vec();
    let delta: Vec<u64> = [4096, 2048, 1024].to_vec();
    let layersize: Vec<u64> = [236, 19286, 7815744].to_vec();
    let time_ns: u64 = 1234;
    Ok(vec![datasetsize_b.to_object(py), func.to_object(py), delta.to_object(py), layersize.to_object(py), time_ns.to_object(py)])
}

#[pyfunction]
fn tune_airindex(_dataset: String, _affine: bool, _latency: u64, _bandwidth: f64) -> PyResult<Vec<PyObject>> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let datasetsize_b = 1717987000;
    let func: Vec<String> = 
        ["Linear".to_owned(),
        "Step".to_owned(),
        "Step".to_owned()].to_vec();
    let delta: Vec<u64> = [1024, 2048, 4096].to_vec();
    let layersize: Vec<u64> = [436, 39286, 9815744].to_vec();
    let time_ns: u64 = 123;
    Ok(vec![datasetsize_b.to_object(py), func.to_object(py), delta.to_object(py), layersize.to_object(py), time_ns.to_object(py)])
}

#[pymodule]
#[pyo3(name = "airindex")]
fn interface(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_dataset, m)?)?;
    m.add_function(wrap_pyfunction!(tune_diy_custom, m)?)?;
    m.add_function(wrap_pyfunction!(tune_diy_btree, m)?)?;
    m.add_function(wrap_pyfunction!(tune_airindex, m)?)?;
    Ok(())
}

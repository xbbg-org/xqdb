mod arrow;
pub mod connector;
pub mod error;

use crate::arrow::{ArrowSeries, ArrowTable};
use crate::connector::{
    generate_j6_ipc_msg, read_j6_binary_table, XqdbConnector, XqdbQLambda, XqdbQOperator,
};
use error::{XqdbAuthError, XqdbError, XqdbIOError};
use pyo3::prelude::*;

#[pymodule]
fn xqdb(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<XqdbConnector>()?;
    m.add_class::<XqdbQOperator>()?;
    m.add_class::<XqdbQLambda>()?;
    m.add_class::<ArrowTable>()?;
    m.add_class::<ArrowSeries>()?;
    m.add("XqdbError", py.get_type::<XqdbError>())?;
    m.add("XqdbIOError", py.get_type::<XqdbIOError>())?;
    m.add("XqdbAuthError", py.get_type::<XqdbAuthError>())?;
    m.add_function(wrap_pyfunction!(read_j6_binary_table, m)?)?;
    m.add_function(wrap_pyfunction!(generate_j6_ipc_msg, m)?)?;
    Ok(())
}

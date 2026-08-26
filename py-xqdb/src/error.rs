use polars::error::PolarsError;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::PyErr;
use thiserror::Error;
use xqdb::errors;

#[derive(Debug, Error)]
pub enum PyXqdbError {
    #[error(transparent)]
    XqdbErr(#[from] errors::XqdbError),

    #[error(transparent)]
    PolarsErr(#[from] PolarsError),

    #[error(transparent)]
    PythonErr(#[from] PyErr),
}

impl From<PyXqdbError> for PyErr {
    fn from(err: PyXqdbError) -> PyErr {
        use PyXqdbError::*;
        match err {
            XqdbErr(e) => match &e {
                errors::XqdbError::IOError(_)
                | errors::XqdbError::FailedToConnectErr(_)
                | errors::XqdbError::NotConnectedErr() => XqdbIOError::new_err(e.to_string()),
                errors::XqdbError::AuthErr() => XqdbAuthError::new_err(e.to_string()),
                _ => XqdbError::new_err(e.to_string()),
            },
            PolarsErr(error) => PyValueError::new_err(error.to_string()),
            PythonErr(err) => err,
        }
    }
}

create_exception!(xqdb.exceptions, XqdbError, PyException);
create_exception!(xqdb.exceptions, XqdbIOError, PyException);
create_exception!(xqdb.exceptions, XqdbAuthError, PyException);

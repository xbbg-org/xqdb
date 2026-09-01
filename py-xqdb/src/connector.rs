use crate::arrow::{import_value, ArrowSeries, ArrowTable};
use crate::error::PyXqdbError;
use chrono::{Datelike, Timelike};
use indexmap::IndexMap;
use polars::prelude::{DataFrame, Series};
use pyo3::exceptions::{PyOverflowError, PyTypeError, PyValueError};
use pyo3::types::{
    PyBool, PyBytes, PyDate, PyDateTime, PyDelta, PyDict, PyFloat, PyInt, PyList, PyString, PyTime,
    PyTuple, PyTzInfo, PyTzInfoAccess,
};
use pyo3::{prelude::*, IntoPyObjectExt};
use std::collections::HashSet;
use xqdb::connector::Connector;
use xqdb::types::{MsgType, QLambda, QOperator, SymbolEncoding, K, MIN_Q_TIMESTAMP_UNIX_NANOS};

pub(crate) enum ArrowValue {
    DataFrame(DataFrame),
    Series(Series),
}

#[pyclass(frozen, eq, module = "xqdb", skip_from_py_object)]
#[derive(Clone, Eq, PartialEq)]
pub struct XqdbQOperator {
    operator: QOperator,
}

#[pymethods]
impl XqdbQOperator {
    #[new]
    fn new(name: &str) -> Result<Self, PyXqdbError> {
        Ok(Self {
            operator: QOperator::new(name)?,
        })
    }

    #[classattr]
    #[pyo3(name = "PLUS")]
    fn plus() -> Self {
        Self {
            operator: QOperator::PLUS,
        }
    }

    #[getter]
    fn name(&self) -> &str {
        self.operator.name()
    }

    fn __repr__(&self) -> String {
        format!("XqdbQOperator({:?})", self.operator.name())
    }
}

#[pyclass(frozen, eq, module = "xqdb", skip_from_py_object)]
#[derive(Clone, Eq, PartialEq)]
pub struct XqdbQLambda {
    lambda: QLambda,
}

#[pymethods]
impl XqdbQLambda {
    #[new]
    #[pyo3(signature = (source, context = ""))]
    fn new(source: &str, context: &str) -> Result<Self, PyXqdbError> {
        Ok(Self {
            lambda: QLambda::with_context(source, context)?,
        })
    }

    #[getter]
    fn source(&self) -> &str {
        self.lambda.source()
    }

    #[getter]
    fn context(&self) -> &str {
        self.lambda.context()
    }

    fn __repr__(&self) -> String {
        if self.lambda.context().is_empty() {
            format!("XqdbQLambda({:?})", self.lambda.source())
        } else {
            format!(
                "XqdbQLambda({:?}, {:?})",
                self.lambda.source(),
                self.lambda.context()
            )
        }
    }
}

#[pyclass]
pub struct XqdbConnector {
    q: Connector,
}

fn parse_symbol_encoding(value: &str) -> PyResult<SymbolEncoding> {
    SymbolEncoding::from_name(value).ok_or_else(|| {
        PyValueError::new_err(format!(
            "symbol_encoding must be 'strict' or 'lossy', got {value:?}"
        ))
    })
}

const MAX_CONVERSION_DEPTH: usize = 64;
const MAX_CALL_ARGUMENTS: usize = 8;
const MICROSECONDS_PER_SECOND: i64 = 1_000_000;
const MICROSECONDS_PER_DAY: i64 = 86_400 * MICROSECONDS_PER_SECOND;

impl XqdbConnector {
    fn execute(&mut self, py: Python, expr: &str, args: Bound<PyTuple>) -> PyResult<Py<PyAny>> {
        let args = cast_to_k_vec(args)?;
        let k = py
            .detach(move || self.q.execute(expr, &args))
            .map_err(PyXqdbError::from)?;
        cast_k_to_py(py, k)
    }

    fn execute_async(
        &mut self,
        py: Python,
        expr: &str,
        args: Bound<PyTuple>,
    ) -> Result<(), PyXqdbError> {
        let args = cast_to_k_vec(args)?;
        py.detach(move || self.q.execute_async(expr, &args))
            .map_err(PyXqdbError::from)
    }
}

fn python_date_parts(value: &impl Datelike) -> PyResult<(i32, u8, u8)> {
    let year = value.year();
    if !(1..=9999).contains(&year) {
        return Err(PyOverflowError::new_err(format!(
            "year {year} is outside Python's supported range"
        )));
    }
    Ok((year, value.month() as u8, value.day() as u8))
}

fn python_microseconds(nanoseconds: u32, type_name: &str) -> PyResult<u32> {
    if !nanoseconds.is_multiple_of(1_000) {
        return Err(PyValueError::new_err(format!(
            "{type_name} has sub-microsecond precision that Python cannot represent"
        )));
    }
    Ok(nanoseconds / 1_000)
}

fn cast_k_to_py(py: Python, k: K) -> PyResult<Py<PyAny>> {
    cast_k_to_py_inner(py, k, 0)
}

fn cast_k_to_py_inner(py: Python, k: K, depth: usize) -> PyResult<Py<PyAny>> {
    if depth > MAX_CONVERSION_DEPTH {
        return Err(PyValueError::new_err(format!(
            "q value nesting exceeds {MAX_CONVERSION_DEPTH} levels"
        )));
    }

    match k {
        K::Boolean(k) => k.into_py_any(py),
        K::Guid(k) => k.to_string().into_py_any(py),
        K::U8(k) => k.into_py_any(py),
        K::I16(k) => k.into_py_any(py),
        K::I32(k) => k.into_py_any(py),
        K::I64(k) => k.into_py_any(py),
        K::F32(k) => k.into_py_any(py),
        K::F64(k) => k.into_py_any(py),
        K::Char(k) => (k as char).into_py_any(py),
        K::CharVector(k) => match std::str::from_utf8(&k) {
            Ok(text) => text.into_py_any(py),
            Err(_) => PyBytes::new(py, &k).into_py_any(py),
        },
        K::Symbol(k) => k.into_py_any(py),
        K::String(k) => k.into_py_any(py),
        K::DateTime(k) => {
            let (year, month, day) = python_date_parts(&k)?;
            let microsecond = python_microseconds(k.nanosecond(), "q timestamp")?;
            // A q timestamp carries no timezone, so it maps to a naive datetime. This
            // matches the naive `timestamp[ns]` Arrow columns and avoids asserting UTC
            // over q processes that store local wall-clock times.
            PyDateTime::new(
                py,
                year,
                month,
                day,
                k.hour() as u8,
                k.minute() as u8,
                k.second() as u8,
                microsecond,
                None,
            )?
            .into_py_any(py)
        }
        K::Date(k) => {
            let (year, month, day) = python_date_parts(&k)?;
            PyDate::new(py, year, month, day)?.into_py_any(py)
        }
        K::Time(k) => {
            let microsecond = python_microseconds(k.nanosecond(), "q time")?;
            PyTime::new(
                py,
                k.hour() as u8,
                k.minute() as u8,
                k.second() as u8,
                microsecond,
                None,
            )?
            .into_py_any(py)
        }
        K::Duration(k) => {
            let nanoseconds = k.num_nanoseconds().ok_or_else(|| {
                PyOverflowError::new_err("q timespan is outside Python's supported range")
            })?;
            if nanoseconds % 1_000 != 0 {
                return Err(PyValueError::new_err(
                    "q timespan has sub-microsecond precision that Python cannot represent",
                ));
            }
            let microseconds = nanoseconds / 1_000;
            let days = microseconds.div_euclid(MICROSECONDS_PER_DAY);
            let day_microseconds = microseconds.rem_euclid(MICROSECONDS_PER_DAY);
            let seconds = day_microseconds / MICROSECONDS_PER_SECOND;
            let remaining_microseconds = day_microseconds % MICROSECONDS_PER_SECOND;
            let days = i32::try_from(days).map_err(|_| {
                PyOverflowError::new_err("q timespan is outside Python's supported range")
            })?;
            PyDelta::new(
                py,
                days,
                seconds as i32,
                remaining_microseconds as i32,
                false,
            )?
            .into_py_any(py)
        }
        K::MixedList(values) => {
            let py_objects = values
                .into_iter()
                .map(|value| cast_k_to_py_inner(py, value, depth + 1))
                .collect::<PyResult<Vec<_>>>()?;
            PyTuple::new(py, py_objects)?.into_py_any(py)
        }
        K::Series(k) => Ok(Py::new(py, ArrowSeries::new(k))?.into_any()),
        K::DataFrame(k) => Ok(Py::new(py, ArrowTable::new(k))?.into_any()),
        K::Operator(operator) => Ok(Py::new(py, XqdbQOperator { operator })?.into_any()),
        K::Lambda(lambda) => Ok(Py::new(py, XqdbQLambda { lambda })?.into_any()),
        K::Null => ().into_py_any(py),
        K::Dict(dict) => {
            let py_dict = PyDict::new(py);
            for (key, value) in dict {
                py_dict.set_item(key, cast_k_to_py_inner(py, value, depth + 1)?)?;
            }
            Ok(py_dict.into())
        }
    }
}

#[pymethods]
impl XqdbConnector {
    #[new]
    pub fn __init__(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        enable_tls: bool,
        timeout: u64,
        version: u8,
    ) -> PyResult<Self> {
        Ok(Self {
            q: Connector::new(host, port, user, password, enable_tls, timeout, version),
        })
    }

    #[getter]
    fn symbol_encoding(&self) -> &'static str {
        self.q.symbol_encoding.name()
    }

    #[setter]
    fn set_symbol_encoding(&mut self, value: &str) -> PyResult<()> {
        self.q.symbol_encoding = parse_symbol_encoding(value)?;
        Ok(())
    }

    pub fn connect(&mut self, py: Python) -> Result<(), PyXqdbError> {
        py.detach(|| self.q.connect().map_err(PyXqdbError::from))
    }

    pub fn shutdown(&mut self, py: Python) -> Result<(), PyXqdbError> {
        py.detach(|| self.q.shutdown().map_err(PyXqdbError::from))
    }

    #[pyo3(signature = (expr, *args))]
    pub fn sync(&mut self, py: Python, expr: &str, args: Bound<PyTuple>) -> PyResult<Py<PyAny>> {
        self.execute(py, expr, args)
    }

    #[pyo3(signature = (expr, *args))]
    pub fn asyn(
        &mut self,
        py: Python,
        expr: &str,
        args: Bound<PyTuple>,
    ) -> Result<(), PyXqdbError> {
        self.execute_async(py, expr, args)
    }

    pub fn receive(&mut self, py: Python) -> PyResult<Py<PyAny>> {
        let k = py.detach(move || self.q.receive().map_err(PyXqdbError::from))?;
        cast_k_to_py(py, k)
    }
}

fn cast_to_k_vec(tuple: Bound<PyTuple>) -> Result<Vec<K>, PyXqdbError> {
    if tuple.len() > MAX_CALL_ARGUMENTS {
        return Err(PyTypeError::new_err(format!(
            "q functions accept at most {MAX_CALL_ARGUMENTS} arguments"
        ))
        .into());
    }

    let mut active_containers = HashSet::new();
    tuple
        .into_iter()
        .map(|value| cast_to_k_inner(value, 0, &mut active_containers))
        .collect::<PyResult<Vec<_>>>()
        .map_err(PyXqdbError::from)
}

fn cast_to_k(any: Bound<PyAny>) -> PyResult<K> {
    cast_to_k_inner(any, 0, &mut HashSet::new())
}

fn cast_to_k_inner(
    any: Bound<PyAny>,
    depth: usize,
    active_containers: &mut HashSet<usize>,
) -> PyResult<K> {
    if depth > MAX_CONVERSION_DEPTH {
        return Err(PyValueError::new_err(format!(
            "Python value nesting exceeds {MAX_CONVERSION_DEPTH} levels"
        )));
    }

    if any.is_instance_of::<XqdbQOperator>() {
        let value = any.extract::<PyRef<XqdbQOperator>>()?;
        Ok(K::Operator(value.operator))
    } else if any.is_instance_of::<XqdbQLambda>() {
        let value = any.extract::<PyRef<XqdbQLambda>>()?;
        Ok(K::Lambda(value.lambda.clone()))
    } else if any.is_instance_of::<PyBool>() {
        Ok(K::Boolean(any.extract()?))
    } else if any.is_instance_of::<PyInt>() {
        Ok(K::I64(any.extract()?))
    } else if any.is_instance_of::<PyFloat>() {
        Ok(K::F64(any.extract()?))
    } else if any.is_instance_of::<PyString>() {
        Ok(K::Symbol(any.extract::<&str>()?.to_owned()))
    } else if any.is_instance_of::<PyBytes>() {
        let value = any.cast::<PyBytes>()?;
        Ok(K::CharVector(value.as_bytes().to_vec()))
    } else if any.hasattr("__arrow_c_stream__")? {
        match import_value(&any)? {
            ArrowValue::Series(series) => Ok(K::Series(series)),
            ArrowValue::DataFrame(frame) => Ok(K::DataFrame(frame)),
        }
    } else if any.is_none() {
        Ok(K::Null)
    } else if any.is_instance_of::<PyDateTime>() {
        let datetime = any.cast::<PyDateTime>()?;
        let py = any.py();
        // A q timestamp carries no timezone, so a naive datetime is taken as the q wall
        // clock as-is and values read out of naive `timestamp[ns]` columns and atoms
        // round-trip unchanged. Awareness uses Python's documented test,
        // `d.tzinfo is not None and d.tzinfo.utcoffset(d) is not None`, asking the tzinfo
        // directly rather than the datetime's own overrideable `utcoffset()`. Misjudging
        // this would hand the value to `astimezone()`, which presumes the host's local
        // zone for a naive datetime and would silently shift the wall clock.
        let value: chrono::DateTime<chrono::Utc> = match datetime.get_tzinfo() {
            None => datetime.extract::<chrono::NaiveDateTime>()?.and_utc(),
            Some(tz) => {
                let offset = tz.call_method1(pyo3::intern!(py, "utcoffset"), (&datetime,))?;
                if offset.is_none() {
                    // Python considers this naive. Drop the offsetless tzinfo without
                    // touching the wall-clock fields.
                    let kwargs = PyDict::new(py);
                    kwargs.set_item(pyo3::intern!(py, "tzinfo"), py.None())?;
                    datetime
                        .call_method(pyo3::intern!(py, "replace"), (), Some(&kwargs))?
                        .extract::<chrono::NaiveDateTime>()?
                        .and_utc()
                } else {
                    // Let Python resolve the instant so fixed offsets, `zoneinfo` zones,
                    // and any other tzinfo normalize identically, including across DST.
                    let utc = PyTzInfo::utc(py)?;
                    datetime
                        .call_method1(pyo3::intern!(py, "astimezone"), (utc,))?
                        .extract::<chrono::DateTime<chrono::Utc>>()?
                }
            }
        };
        // `pandas.Timestamp` subclasses `datetime` and keeps its sub-microsecond digits in
        // `.nanosecond`, which the datetime accessors never expose. PyArrow hands back that
        // type for `timestamp[ns]` scalars, so fold the remainder in to keep a nanosecond
        // value read out of an Arrow column exact. Python offsets have at most microsecond
        // resolution, so the remainder is invariant under the conversion above and the
        // original object is the right source.
        let value = match datetime.getattr_opt(pyo3::intern!(py, "nanosecond"))? {
            Some(attr) => {
                let remainder = attr.extract::<i64>()?;
                if !(0..1_000).contains(&remainder) {
                    return Err(PyValueError::new_err(
                        "datetime.nanosecond must be between 0 and 999",
                    ));
                }
                value
                    .checked_add_signed(chrono::TimeDelta::nanoseconds(remainder))
                    .ok_or_else(|| {
                        PyOverflowError::new_err(
                            "datetime is outside q's representable timestamp range",
                        )
                    })?
            }
            None => value,
        };
        let nanoseconds = value.timestamp_nanos_opt().ok_or_else(|| {
            PyOverflowError::new_err("datetime is outside q's representable timestamp range")
        })?;
        if nanoseconds < MIN_Q_TIMESTAMP_UNIX_NANOS {
            return Err(PyOverflowError::new_err(
                "datetime is outside q's representable timestamp range",
            ));
        }
        Ok(K::DateTime(value))
    } else if any.is_instance_of::<PyDate>() {
        let value: chrono::NaiveDate = any.cast::<PyDate>()?.extract()?;
        Ok(K::Date(value))
    } else if any.is_instance_of::<PyTime>() {
        let value: chrono::NaiveTime = any.cast::<PyTime>()?.extract()?;
        if !value.nanosecond().is_multiple_of(1_000_000) {
            return Err(PyValueError::new_err(
                "q time only supports millisecond precision",
            ));
        }
        Ok(K::Time(value))
    } else if any.is_instance_of::<PyDelta>() {
        let value: chrono::Duration = any.cast::<PyDelta>()?.extract()?;
        if value.num_nanoseconds().is_none() {
            return Err(PyOverflowError::new_err(
                "timedelta is outside q's representable timespan range",
            ));
        }
        Ok(K::Duration(value))
    } else if any.is_instance_of::<PyDict>() {
        let identity = any.as_ptr() as usize;
        if !active_containers.insert(identity) {
            return Err(PyValueError::new_err(
                "cyclic Python containers cannot be converted to q",
            ));
        }
        let result = (|| {
            let py_dict = any.cast::<PyDict>()?;
            let mut dict = IndexMap::with_capacity(py_dict.len());
            for (key, value) in py_dict {
                let key = key.extract::<&str>()?.to_owned();
                dict.insert(key, cast_to_k_inner(value, depth + 1, active_containers)?);
            }
            Ok(K::Dict(dict))
        })();
        active_containers.remove(&identity);
        result
    } else if any.is_instance_of::<PyList>() {
        let identity = any.as_ptr() as usize;
        if !active_containers.insert(identity) {
            return Err(PyValueError::new_err(
                "cyclic Python containers cannot be converted to q",
            ));
        }
        let result = (|| {
            let py_list = any.cast::<PyList>()?;
            let mut values = Vec::with_capacity(py_list.len());
            for value in py_list {
                values.push(cast_to_k_inner(value, depth + 1, active_containers)?);
            }
            Ok(K::MixedList(values))
        })();
        active_containers.remove(&identity);
        result
    } else if any.is_instance_of::<PyTuple>() {
        let identity = any.as_ptr() as usize;
        if !active_containers.insert(identity) {
            return Err(PyValueError::new_err(
                "cyclic Python containers cannot be converted to q",
            ));
        }
        let result = (|| {
            let py_tuple = any.cast::<PyTuple>()?;
            let mut values = Vec::with_capacity(py_tuple.len());
            for value in py_tuple {
                values.push(cast_to_k_inner(value, depth + 1, active_containers)?);
            }
            Ok(K::MixedList(values))
        })();
        active_containers.remove(&identity);
        result
    } else {
        Err(PyTypeError::new_err(format!(
            "unsupported Python type {:?}",
            any.get_type()
        )))
    }
}

#[pyfunction]
#[pyo3(signature = (filepath, symbol_encoding = "strict"))]
pub fn read_j6_binary_table(
    py: Python,
    filepath: &str,
    symbol_encoding: &str,
) -> PyResult<Py<ArrowTable>> {
    let encoding = parse_symbol_encoding(symbol_encoding)?;
    let filepath = filepath.to_owned();
    let frame = py
        .detach(move || xqdb::io::read_j6_binary_table(&filepath, encoding))
        .map_err(PyXqdbError::from)?;
    Py::new(py, ArrowTable::new(frame))
}

#[pyfunction]
pub fn generate_j6_ipc_msg<'a>(
    py: Python<'a>,
    msg_type: u8,
    enable_compression: bool,
    any: Bound<PyAny>,
) -> PyResult<Bound<'a, PyBytes>> {
    let msg_type = match msg_type {
        0 => MsgType::Async,
        1 => MsgType::Sync,
        2 => MsgType::Response,
        value => {
            return Err(PyValueError::new_err(format!(
                "msg_type must be 0, 1, or 2; got {value}"
            )))
        }
    };
    let value = cast_to_k(any)?;
    let bytes = py
        .detach(move || xqdb::io::generate_j6_ipc_msg(msg_type, enable_compression, value))
        .map_err(PyXqdbError::from)?;
    Ok(PyBytes::new(py, &bytes))
}

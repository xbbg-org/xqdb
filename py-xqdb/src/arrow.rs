use crate::error::PyXqdbError;
use polars::error::PolarsResult;
use polars::prelude::{CompatLevel, DataFrame, Series};
use polars_arrow::array::{Array, StructArray};
use polars_arrow::datatypes::{ArrowDataType, Field};
use polars_arrow::ffi::{export_iterator, ArrowArrayStream, ArrowArrayStreamReader};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyCapsule, PyCapsuleMethods};
use std::ffi::{c_void, CStr};
use std::mem::ManuallyDrop;

const ARROW_ARRAY_STREAM: &CStr = c"arrow_array_stream";
const SERIES_MARKER: &str = "__xqdb_series__";

unsafe extern "C" fn drop_stream_capsule(capsule: *mut ffi::PyObject) {
    // SAFETY: CPython invokes this destructor only for the capsule created in
    // `stream_capsule`; `capsule` is therefore a live PyCapsule.
    let name = unsafe { ffi::PyCapsule_GetName(capsule) };
    if name.is_null() {
        return;
    }
    // SAFETY: `name` came from this live capsule and remains valid for the
    // duration of the call.
    let pointer = unsafe { ffi::PyCapsule_GetPointer(capsule, name) };
    if pointer.is_null() {
        return;
    }

    // SAFETY: this destructor is installed only by `stream_capsule`, whose
    // pointer comes from `Box<ManuallyDrop<ArrowArrayStream>>`. CPython calls
    // the destructor once, so this uniquely recovers and frees that allocation.
    let mut stream = unsafe { Box::from_raw(pointer.cast::<ManuallyDrop<ArrowArrayStream>>()) };
    // SAFETY: a non-null capsule name is a NUL-terminated string owned by
    // the live capsule.
    if unsafe { CStr::from_ptr(name) } == ARROW_ARRAY_STREAM {
        // SAFETY: the unchanged Arrow capsule name means the stream was not
        // consumed. This is the only owner, so its release callback runs once.
        unsafe { ManuallyDrop::drop(&mut stream) };
    }
}

fn stream_capsule(py: Python<'_>, stream: ArrowArrayStream) -> PyResult<Py<PyAny>> {
    let owner = Box::new(ManuallyDrop::new(stream));
    let pointer = Box::into_raw(owner).cast::<c_void>();
    // SAFETY: `ManuallyDrop<T>` has the same layout as `T`, so `pointer`
    // exposes the Arrow C Stream layout; the pointer is non-null, the name is
    // static, and the destructor uniquely owns the allocation after successful
    // capsule creation.
    let capsule = unsafe {
        ffi::PyCapsule_New(
            pointer,
            ARROW_ARRAY_STREAM.as_ptr(),
            Some(drop_stream_capsule),
        )
    };
    if capsule.is_null() {
        // SAFETY: capsule creation failed, so ownership never left this
        // function and the allocation can be uniquely recovered and dropped.
        let mut owner = unsafe { Box::from_raw(pointer.cast::<ManuallyDrop<ArrowArrayStream>>()) };
        // SAFETY: `owner` was uniquely recovered above and its stream was
        // never transferred, so the release callback must run exactly once.
        unsafe { ManuallyDrop::drop(&mut owner) };
        return Err(PyErr::fetch(py));
    }
    // SAFETY: `PyCapsule_New` returned a new owned reference, transferred to
    // this `Bound` exactly once.
    Ok(unsafe { Bound::<PyAny>::from_owned_ptr(py, capsule) }.unbind())
}

fn take_stream(capsule: &Bound<'_, PyCapsule>) -> PyResult<ArrowArrayStream> {
    if !capsule.is_valid_checked(Some(ARROW_ARRAY_STREAM)) {
        let name = capsule
            .name()?
            .map(|name| {
                // SAFETY: PyO3 obtained this name from the live capsule, whose
                // storage remains valid while the GIL-bound capsule is borrowed.
                unsafe { name.as_cstr() }.to_string_lossy().into_owned()
            })
            .unwrap_or_else(|| "<unnamed>".to_owned());
        return Err(PyTypeError::new_err(format!(
            "expected an unused 'arrow_array_stream' capsule, got {name:?}"
        )));
    }

    let pointer = capsule
        .pointer_checked(Some(ARROW_ARRAY_STREAM))?
        .cast::<ArrowArrayStream>();
    // SAFETY: the validated, unused Arrow capsule name establishes the Arrow C
    // Stream pointer type. Holding the GIL gives unique consumption; replacing
    // the value with an empty stream transfers its callbacks to the reader and
    // prevents the capsule producer from releasing them a second time.
    let stream = unsafe { std::ptr::replace(pointer.as_ptr(), ArrowArrayStream::empty()) };
    Ok(stream)
}

fn frame_stream(mut frame: DataFrame) -> ArrowArrayStream {
    if frame.should_rechunk() {
        frame.rechunk_mut();
    }

    let compat_level = CompatLevel::newest();
    let fields = frame
        .columns()
        .iter()
        .map(|column| column.field().to_arrow(compat_level))
        .collect::<Vec<_>>();
    let struct_dtype = ArrowDataType::Struct(fields);
    let batches = frame
        .iter_chunks(compat_level, false)
        .map(|batch| {
            let length = batch.len();
            let values = batch
                .into_arrays()
                .into_iter()
                .map(|array| array.as_ref().to_boxed())
                .collect();
            Ok(
                Box::new(StructArray::new(struct_dtype.clone(), length, values, None))
                    as Box<dyn Array>,
            )
        })
        .collect::<Vec<PolarsResult<Box<dyn Array>>>>();
    let field = Field::new("".into(), struct_dtype, false);
    export_iterator(Box::new(batches.into_iter()), field)
}

fn validate_import_dtype(dtype: &ArrowDataType) -> PyResult<()> {
    match dtype {
        ArrowDataType::Null
        | ArrowDataType::Boolean
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64
        | ArrowDataType::UInt8
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64
        | ArrowDataType::Float32
        | ArrowDataType::Float64
        | ArrowDataType::Timestamp(_, _)
        | ArrowDataType::Date32
        | ArrowDataType::Date64
        | ArrowDataType::Time32(_)
        | ArrowDataType::Time64(_)
        | ArrowDataType::Duration(_)
        | ArrowDataType::Binary
        | ArrowDataType::FixedSizeBinary(_)
        | ArrowDataType::LargeBinary
        | ArrowDataType::Utf8
        | ArrowDataType::LargeUtf8
        | ArrowDataType::BinaryView
        | ArrowDataType::Utf8View => Ok(()),
        ArrowDataType::List(field)
        | ArrowDataType::FixedSizeList(field, _)
        | ArrowDataType::LargeList(field) => validate_nested_import_field(field),
        ArrowDataType::Dictionary(_, value, _) => validate_import_dtype(value),
        ArrowDataType::Int8
        | ArrowDataType::Int128
        | ArrowDataType::UInt16
        | ArrowDataType::UInt128
        | ArrowDataType::Float16
        | ArrowDataType::Interval(_)
        | ArrowDataType::Struct(_)
        | ArrowDataType::Map(_, _)
        | ArrowDataType::Decimal(_, _)
        | ArrowDataType::Decimal32(_, _)
        | ArrowDataType::Decimal64(_, _)
        | ArrowDataType::Decimal256(_, _)
        | ArrowDataType::Extension(_)
        | ArrowDataType::Unknown
        | ArrowDataType::Union(_) => Err(PyValueError::new_err(format!(
            "unsupported Arrow datatype: {dtype:?}"
        ))),
    }
}

fn validate_nested_import_field(field: &Field) -> PyResult<()> {
    const RESERVED_POLARS_METADATA: [&str; 4] = [
        "_PL_ENUM_VALUES",
        "_PL_ENUM_VALUES2",
        "_PL_CATEGORICAL",
        "_PL_CATEGORICAL2",
    ];
    if let Some(key) = field.metadata.as_ref().and_then(|metadata| {
        metadata
            .keys()
            .find(|key| RESERVED_POLARS_METADATA.contains(&key.as_str()))
    }) {
        return Err(PyValueError::new_err(format!(
            "nested Arrow field contains unsupported reserved Polars metadata: {key}"
        )));
    }
    validate_import_dtype(field.dtype())
}

fn sanitize_top_level_import_field(field: &Field) -> PyResult<Field> {
    validate_import_dtype(field.dtype())?;
    Ok(Field::new(
        field.name.clone(),
        field.dtype.clone(),
        field.is_nullable,
    ))
}

fn frame_from_struct(array: &StructArray) -> PyResult<DataFrame> {
    if array
        .validity()
        .is_some_and(|validity| validity.unset_bits() != 0)
    {
        return Err(PyValueError::new_err(
            "Arrow table batches cannot contain null struct rows",
        ));
    }

    let (fields, height, values, _) = array.clone().into_data();
    let fields = fields
        .iter()
        .map(sanitize_top_level_import_field)
        .collect::<PyResult<Vec<_>>>()?;
    let array = StructArray::new(ArrowDataType::Struct(fields), height, values, None);
    DataFrame::try_from(array)
        .map_err(PyXqdbError::from)
        .map_err(PyErr::from)
}

fn empty_frame(fields: &[Field]) -> PyResult<DataFrame> {
    let dtype = ArrowDataType::Struct(fields.to_vec());
    let array = StructArray::new_empty(dtype);
    frame_from_struct(&array)
}

pub fn import_frame(value: &Bound<'_, PyAny>) -> PyResult<DataFrame> {
    let capsule = value.call_method1("__arrow_c_stream__", (value.py().None(),))?;
    let capsule = capsule
        .cast_into::<PyCapsule>()
        .map_err(|_| PyTypeError::new_err("__arrow_c_stream__ must return a PyCapsule"))?;
    let stream = take_stream(&capsule)?;
    // SAFETY: `take_stream` consumed a conforming, unused Arrow C Stream
    // capsule and transferred exclusive ownership of its callback table here.
    let mut reader =
        unsafe { ArrowArrayStreamReader::try_new(Box::new(stream)) }.map_err(PyXqdbError::from)?;
    let fields = match reader.field().dtype() {
        ArrowDataType::Struct(fields) => fields.clone(),
        dtype => {
            return Err(PyTypeError::new_err(format!(
                "Arrow stream schema must be a struct, got {dtype:?}"
            )))
        }
    };

    let mut frame: Option<DataFrame> = None;
    // SAFETY: `try_new` validated the stream schema and initialized the reader;
    // advancing it follows the Arrow C Stream callback contract while the
    // reader retains exclusive ownership.
    while let Some(array) = unsafe { reader.next() } {
        let array = array.map_err(PyXqdbError::from)?;
        let array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| PyTypeError::new_err("Arrow stream batch must be a struct array"))?;
        let batch = frame_from_struct(array)?;
        match &mut frame {
            Some(frame) => {
                frame.vstack_mut(&batch).map_err(PyXqdbError::from)?;
            }
            None => frame = Some(batch),
        }
    }

    match frame {
        Some(frame) => Ok(frame),
        None => empty_frame(&fields),
    }
}

pub fn import_value(value: &Bound<'_, PyAny>) -> PyResult<crate::connector::ArrowValue> {
    let frame = import_frame(value)?;
    let is_series = if value.hasattr(SERIES_MARKER)? {
        value.getattr(SERIES_MARKER)?.is_truthy()?
    } else {
        false
    };
    if !is_series {
        return Ok(crate::connector::ArrowValue::DataFrame(frame));
    }
    if frame.width() != 1 {
        return Err(PyTypeError::new_err(format!(
            "series Arrow stream must contain exactly one column, got {}",
            frame.width()
        )));
    }
    let series = frame
        .into_columns()
        .into_iter()
        .next()
        .expect("one-column frame validated")
        .take_materialized_series();
    Ok(crate::connector::ArrowValue::Series(series))
}

#[pyclass(frozen, module = "xqdb.xqdb", skip_from_py_object)]
pub struct ArrowTable {
    frame: DataFrame,
}

impl ArrowTable {
    pub fn new(frame: DataFrame) -> Self {
        Self { frame }
    }
}

#[pymethods]
impl ArrowTable {
    #[getter]
    fn shape(&self) -> (usize, usize) {
        self.frame.shape()
    }

    #[getter]
    fn columns(&self) -> Vec<String> {
        self.frame
            .get_column_names()
            .into_iter()
            .map(ToString::to_string)
            .collect()
    }

    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_stream__(
        &self,
        py: Python<'_>,
        requested_schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        if requested_schema.is_some_and(|schema| !schema.is_none()) {
            return Err(PyValueError::new_err(
                "requested Arrow schema negotiation is not supported",
            ));
        }
        stream_capsule(py, frame_stream(self.frame.clone()))
    }

    fn __repr__(&self) -> String {
        format!(
            "ArrowTable(shape={:?}, columns={:?})",
            self.frame.shape(),
            self.frame.get_column_names()
        )
    }
}

#[pyclass(frozen, module = "xqdb.xqdb", skip_from_py_object)]
pub struct ArrowSeries {
    series: Series,
}

impl ArrowSeries {
    pub fn new(series: Series) -> Self {
        Self { series }
    }
}

#[pymethods]
impl ArrowSeries {
    #[getter]
    fn shape(&self) -> (usize,) {
        (self.series.len(),)
    }

    #[getter]
    fn name(&self) -> String {
        self.series.name().to_string()
    }

    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_stream__(
        &self,
        py: Python<'_>,
        requested_schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        if requested_schema.is_some_and(|schema| !schema.is_none()) {
            return Err(PyValueError::new_err(
                "requested Arrow schema negotiation is not supported",
            ));
        }
        stream_capsule(py, frame_stream(self.series.clone().into_frame()))
    }

    fn __repr__(&self) -> String {
        format!(
            "ArrowSeries(name={:?}, shape=({},))",
            self.series.name(),
            self.series.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars_arrow::datatypes::{IntegerType, Metadata};

    fn field_with_metadata(dtype: ArrowDataType, key: &str) -> Field {
        let mut metadata = Metadata::new();
        metadata.insert(key.into(), "malformed".into());
        Field::new("item".into(), dtype, true).with_metadata(metadata)
    }

    #[test]
    fn rejects_reserved_metadata_through_dictionary_and_list_nesting() {
        let child = field_with_metadata(
            ArrowDataType::Dictionary(IntegerType::Int32, Box::new(ArrowDataType::Utf8), false),
            "_PL_ENUM_VALUES2",
        );
        let field = Field::new(
            "outer".into(),
            ArrowDataType::Dictionary(
                IntegerType::Int32,
                Box::new(ArrowDataType::List(Box::new(child))),
                false,
            ),
            true,
        );

        assert!(validate_nested_import_field(&field).is_err());
    }

    #[test]
    fn accepts_benign_metadata_through_nested_lists() {
        let child = field_with_metadata(ArrowDataType::Int64, "semantic");
        let field = Field::new("outer".into(), ArrowDataType::List(Box::new(child)), true);

        assert!(validate_nested_import_field(&field).is_ok());
    }
}

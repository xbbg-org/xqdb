use xqdb::errors::XqdbError;

pub(crate) const CODE_IO: &str = "XQDB_IO";
pub(crate) const CODE_AUTH: &str = "XQDB_AUTH";
pub(crate) const CODE_SERVER: &str = "XQDB_SERVER";
pub(crate) const CODE_CONVERSION: &str = "XQDB_CONVERSION";
pub(crate) const CODE_UNSUPPORTED: &str = "XQDB_UNSUPPORTED";
pub(crate) const CODE_ERROR: &str = "XQDB_ERROR";
pub(crate) const CODE_INTERNAL: &str = "XQDB_INTERNAL";
pub(crate) const CODE_BACKPRESSURE: &str = "XQDB_BACKPRESSURE";

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BindingError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl BindingError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub(crate) fn conversion(message: impl Into<String>) -> Self {
        Self::new(CODE_CONVERSION, message)
    }

    pub(crate) fn backpressure(message: impl Into<String>) -> Self {
        Self::new(CODE_BACKPRESSURE, message)
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(CODE_INTERNAL, message)
    }
}

impl From<XqdbError> for BindingError {
    fn from(error: XqdbError) -> Self {
        let code = match &error {
            XqdbError::IOError(_)
            | XqdbError::FailedToConnectErr(_)
            | XqdbError::NotConnectedErr() => CODE_IO,
            XqdbError::AuthErr() => CODE_AUTH,
            XqdbError::ServerErr(_) => CODE_SERVER,
            XqdbError::DeserializationErr(_)
            | XqdbError::NotAbleToSerializeErr(_)
            | XqdbError::OverLengthErr()
            | XqdbError::TooManyArgumentErr() => CODE_CONVERSION,
            XqdbError::VersionErr()
            | XqdbError::NotSupportedKTypeErr(_)
            | XqdbError::NotSupportedMinusTimeErr(_)
            | XqdbError::NotSupportedKOperatorErr(_)
            | XqdbError::NotSupportedKNestedListErr(_)
            | XqdbError::NotSupportedKListErr(_)
            | XqdbError::NotSupportedKMixedListErr(_, _)
            | XqdbError::NotSupportedArrowTypeErr(_)
            | XqdbError::NotSupportedSeriesTypeErr(_)
            | XqdbError::NotSupportedArrowNestedListTypeErr(_)
            | XqdbError::NotSupportedPolarsNestedListTypeErr(_)
            | XqdbError::NotSupportedBigEndianErr() => CODE_UNSUPPORTED,
            XqdbError::Err(_) => CODE_ERROR,
        };
        Self::new(code, error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use xqdb::errors::XqdbError;

    use super::{
        BindingError, CODE_AUTH, CODE_BACKPRESSURE, CODE_CONVERSION, CODE_IO, CODE_SERVER,
        CODE_UNSUPPORTED,
    };

    #[test]
    fn categorizes_core_errors_stably() {
        let cases = [
            (
                BindingError::from(XqdbError::IOError(io::Error::other("io"))).code,
                CODE_IO,
            ),
            (BindingError::from(XqdbError::AuthErr()).code, CODE_AUTH),
            (
                BindingError::from(XqdbError::ServerErr("server".into())).code,
                CODE_SERVER,
            ),
            (
                BindingError::from(XqdbError::DeserializationErr("bad value".into())).code,
                CODE_CONVERSION,
            ),
            (
                BindingError::from(XqdbError::NotSupportedKTypeErr(42)).code,
                CODE_UNSUPPORTED,
            ),
        ];

        for (actual, expected) in cases {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn backpressure_has_a_stable_code() {
        assert_eq!(
            BindingError::backpressure("queue full").code,
            CODE_BACKPRESSURE
        );
    }
}

use std::fmt;

#[derive(Debug)]
pub enum CloudError {
    Reqwest(reqwest::Error),
    Xml(quick_xml::DeError),
    UintN(uintn::Error),
    NoFilesFound,
    Io(std::io::Error),
    InvalidUrl(url::ParseError),
    InvalidStatusCode(u16),
    TruncatedListingWithoutToken,
}

impl fmt::Display for CloudError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CloudError::Reqwest(e) => write!(f, "HTTP request error: {}", e),
            CloudError::Xml(e) => write!(f, "XML parsing error: {}", e),
            CloudError::UintN(e) => write!(f, "UintN error: {}", e),
            CloudError::NoFilesFound => write!(f, "No files found"),
            CloudError::Io(e) => write!(f, "IO error: {}", e),
            CloudError::InvalidUrl(e) => write!(f, "Invalid URL: {}", e),
            CloudError::InvalidStatusCode(code) => write!(f, "Invalid status code: {}", code),
            CloudError::TruncatedListingWithoutToken => {
                write!(f, "listing truncated without a continuation token")
            }
        }
    }
}

impl std::error::Error for CloudError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CloudError::Reqwest(e) => Some(e),
            CloudError::Xml(e) => Some(e),
            CloudError::UintN(e) => Some(e),
            CloudError::NoFilesFound => None,
            CloudError::Io(e) => Some(e),
            CloudError::InvalidUrl(e) => Some(e),
            CloudError::InvalidStatusCode(_) | CloudError::TruncatedListingWithoutToken => None,
        }
    }
}

impl From<reqwest::Error> for CloudError {
    fn from(err: reqwest::Error) -> Self {
        CloudError::Reqwest(err)
    }
}

impl From<quick_xml::DeError> for CloudError {
    fn from(err: quick_xml::DeError) -> Self {
        CloudError::Xml(err)
    }
}

impl From<url::ParseError> for CloudError {
    fn from(err: url::ParseError) -> Self {
        CloudError::InvalidUrl(err)
    }
}

impl From<uintn::Error> for CloudError {
    fn from(err: uintn::Error) -> Self {
        CloudError::UintN(err)
    }
}

impl From<std::io::Error> for CloudError {
    fn from(err: std::io::Error) -> Self {
        CloudError::Io(err)
    }
}

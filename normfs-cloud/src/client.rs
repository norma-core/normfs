use bytes::Bytes;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use std::time::Duration;

const PRESIGNED_URL_DURATION: Duration = Duration::from_secs(3600); // 1 hour

/// Read size for a streamed upload body.
const STREAM_CHUNK: usize = 256 * 1024;

#[derive(Clone)]
pub struct S3Client {
    bucket: Bucket,
    credentials: Credentials,
    http_client: reqwest::Client,
    list_page_size: Option<usize>,
}

impl S3Client {
    pub fn new(
        endpoint: url::Url,
        bucket_name: String,
        region: String,
        access_key: String,
        secret_key: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let bucket = Bucket::new(endpoint, UrlStyle::Path, bucket_name, region)?;

        let credentials = Credentials::new(access_key, secret_key);

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()?;

        Ok(Self {
            bucket,
            credentials,
            http_client,
            list_page_size: None,
        })
    }

    /// Caps each listing page below the endpoint's own limit, so a test can
    /// reach the second page without a thousand objects.
    pub fn with_list_page_size(mut self, keys: usize) -> Self {
        self.list_page_size = Some(keys);
        self
    }

    /// Creates the bucket; an existing one is not an error. For tests and
    /// first-run tooling, not the write path.
    pub async fn create_bucket(&self) -> Result<(), crate::errors::CloudError> {
        let action = self.bucket.create_bucket(&self.credentials);
        let url = action.sign(PRESIGNED_URL_DURATION);
        let response = self.http_client.put(url.as_str()).send().await?;
        match response.status().as_u16() {
            200 | 409 => Ok(()),
            code => Err(crate::errors::CloudError::InvalidStatusCode(code)),
        }
    }

    pub async fn put_object(
        &self,
        key: &str,
        data: Bytes,
    ) -> Result<u16, crate::errors::CloudError> {
        let action = self.bucket.put_object(Some(&self.credentials), key);
        let url = action.sign(PRESIGNED_URL_DURATION);

        let response = self.http_client.put(url.as_str()).body(data).send().await?;

        Ok(response.status().as_u16())
    }

    /// [`S3Client::put_object`] with the body read from `body` as it is sent,
    /// so a file of any size takes a chunk of memory rather than its size.
    pub async fn put_object_stream<R>(
        &self,
        key: &str,
        body: R,
        len: u64,
    ) -> Result<u16, crate::errors::CloudError>
    where
        R: tokio::io::AsyncRead + Send + 'static,
    {
        let action = self.bucket.put_object(Some(&self.credentials), key);
        let url = action.sign(PRESIGNED_URL_DURATION);
        let stream = tokio_util::io::ReaderStream::with_capacity(body, STREAM_CHUNK);

        let response = self
            .http_client
            .put(url.as_str())
            .header(reqwest::header::CONTENT_LENGTH, len)
            .body(reqwest::Body::wrap_stream(stream))
            .send()
            .await?;

        Ok(response.status().as_u16())
    }

    pub async fn head_object(&self, key: &str) -> Result<Option<u64>, crate::errors::CloudError> {
        let action = self.bucket.head_object(Some(&self.credentials), key);
        let url = action.sign(PRESIGNED_URL_DURATION);

        let response = self.http_client.head(url.as_str()).send().await?;

        let status = response.status().as_u16();

        if status == 404 || status == 403 {
            return Ok(None);
        }

        if status != 200 {
            return Err(crate::errors::CloudError::InvalidStatusCode(status));
        }

        let content_length = response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());

        Ok(content_length)
    }

    pub async fn get_object(
        &self,
        key: &str,
    ) -> Result<Option<bytes::Bytes>, crate::errors::CloudError> {
        let action = self.bucket.get_object(Some(&self.credentials), key);
        let url = action.sign(PRESIGNED_URL_DURATION);

        let response = self.http_client.get(url.as_str()).send().await?;

        let status = response.status().as_u16();

        if status == 404 {
            return Ok(None);
        }

        if status != 200 && status != 206 {
            return Err(crate::errors::CloudError::InvalidStatusCode(status));
        }

        let bytes = response.bytes().await?;
        Ok(Some(bytes))
    }

    pub async fn get_object_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
    ) -> Result<Option<bytes::Bytes>, crate::errors::CloudError> {
        let action = self.bucket.get_object(Some(&self.credentials), key);
        let url = action.sign(PRESIGNED_URL_DURATION);

        let range_header = match end {
            Some(e) => format!("bytes={}-{}", start, e),
            None => format!("bytes={}-", start),
        };

        let response = self
            .http_client
            .get(url.as_str())
            .header("Range", range_header)
            .send()
            .await?;

        let status = response.status().as_u16();

        if status == 404 {
            return Ok(None);
        }

        if status != 200 && status != 206 {
            return Err(crate::errors::CloudError::InvalidStatusCode(status));
        }

        let bytes = response.bytes().await?;
        Ok(Some(bytes))
    }

    /// Every page of the listing: a caller looking for the largest key cannot
    /// stop at the first thousand.
    pub async fn list_objects(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
    ) -> Result<ListObjectsResult, crate::errors::CloudError> {
        let mut out = ListObjectsResult {
            contents: Vec::new(),
            common_prefixes: Vec::new(),
        };
        let mut token: Option<String> = None;
        loop {
            let mut query =
                rusty_s3::actions::ListObjectsV2::new(&self.bucket, Some(&self.credentials));
            query.with_prefix(prefix);
            // Into the action before signing: a query parameter appended to the
            // signed URL is outside the signature, and a strict endpoint answers
            // 403 to every listing.
            if let Some(delim) = delimiter {
                query.query_mut().insert("delimiter", delim);
            }
            // rusty-s3 asks for URL-encoded keys, and callers match keys
            // against the prefix they passed: an encoded key matches nothing.
            query.query_mut().remove("encoding-type");
            if let Some(max_keys) = self.list_page_size {
                query.with_max_keys(max_keys);
            }
            if let Some(token) = &token {
                query.with_continuation_token(token.as_str());
            }
            let url = query.sign(PRESIGNED_URL_DURATION);

            let response = self.http_client.get(url.as_str()).send().await?;

            let status = response.status().as_u16();

            if status != 200 {
                return Err(crate::errors::CloudError::InvalidStatusCode(status));
            }

            let xml = response.text().await?;
            let page: ListBucketResult = quick_xml::de::from_str(&xml)?;
            out.contents.extend(page.contents);
            out.common_prefixes.extend(page.common_prefixes);

            if !page.is_truncated {
                return Ok(out);
            }
            // A truncated page without a token would repeat the first page
            // forever.
            token = Some(
                page.next_continuation_token
                    .ok_or(crate::errors::CloudError::TruncatedListingWithoutToken)?,
            );
        }
    }

    pub fn bucket(&self) -> &Bucket {
        &self.bucket
    }

    pub fn credentials(&self) -> &Credentials {
        &self.credentials
    }
}

#[derive(Debug)]
pub struct ListObjectsResult {
    pub contents: Vec<S3Object>,
    pub common_prefixes: Vec<CommonPrefix>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    #[serde(rename = "Contents", default)]
    contents: Vec<S3Object>,
    #[serde(rename = "CommonPrefixes", default)]
    common_prefixes: Vec<CommonPrefix>,
    #[serde(rename = "IsTruncated", default)]
    is_truncated: bool,
    #[serde(rename = "NextContinuationToken", default)]
    next_continuation_token: Option<String>,
}

#[derive(Debug, serde::Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct S3Object {
    pub key: String,
    pub size: u64,
}

#[derive(Debug, serde::Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct CommonPrefix {
    pub prefix: String,
}

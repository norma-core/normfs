use bytes::Bytes;
use futures_core::Stream;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

const PRESIGNED_URL_DURATION: Duration = Duration::from_secs(3600); // 1 hour

/// Size of an upload body's chunks, and so of the steps its progress is
/// seen in: four seconds each at 32 kbit/s.
const UPLOAD_CHUNK: usize = 16 * 1024;

/// How long a request may go without moving before it fails and is retried;
/// a lossy link otherwise holds it for the whole timeout.
const STALL: Duration = Duration::from_secs(30);

/// How long a PUT waits for its answer once the whole body is handed to the
/// socket. What the socket still holds drains at the link's pace, which the
/// client cannot see. On Linux it is also the `TCP_USER_TIMEOUT` of uploads:
/// at `STALL` the kernel aborted uploads on a slow link that was still
/// acking, while a body that stops moving is already caught by `STALL`.
const ANSWER_WAIT: Duration = Duration::from_secs(300);

/// When an upload body last moved, and how much of it has.
struct Progress {
    at: Instant,
    sent: u64,
}

/// An upload body that notes each chunk the HTTP client takes, which it
/// does only once the previous one is on its way.
struct Watched<S> {
    inner: S,
    progress: Arc<Mutex<Progress>>,
}

impl<S> Stream for Watched<S>
where
    S: Stream<Item = std::io::Result<Bytes>> + Unpin,
{
    type Item = std::io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let polled = Pin::new(&mut self.inner).poll_next(cx);
        if let Poll::Ready(Some(Ok(chunk))) = &polled {
            let mut progress = self.progress.lock().unwrap();
            progress.at = Instant::now();
            progress.sent += chunk.len() as u64;
        }
        polled
    }
}

/// A body already in memory, handed out in slices of itself.
struct Slices {
    data: Bytes,
}

impl Stream for Slices {
    type Item = std::io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.data.is_empty() {
            return Poll::Ready(None);
        }
        let n = self.data.len().min(UPLOAD_CHUNK);
        Poll::Ready(Some(Ok(self.data.split_to(n))))
    }
}

#[derive(Clone)]
pub struct S3Client {
    bucket: Bucket,
    credentials: Credentials,
    http_client: reqwest::Client,
    /// Without the read timeout, which runs from the send to the answer and
    /// would cut off a slow upload that is still moving; see `put_body`.
    upload_client: reqwest::Client,
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

        // A blackholed endpoint otherwise holds a request for the OS's own
        // connect timeout, over a minute.
        let connect = Duration::from_secs(5);
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .read_timeout(STALL)
            .connect_timeout(connect)
            .build()?;
        let upload_client = reqwest::Client::builder().connect_timeout(connect);
        #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
        let upload_client = upload_client.tcp_user_timeout(ANSWER_WAIT);
        let upload_client = upload_client.build()?;

        Ok(Self {
            bucket,
            credentials,
            http_client,
            upload_client,
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
        let len = data.len() as u64;
        self.put_body(key, Slices { data }, len).await
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
        let body = tokio_util::io::ReaderStream::with_capacity(Box::pin(body), UPLOAD_CHUNK);
        self.put_body(key, body, len).await
    }

    /// A PUT that fails once its body has not moved for `STALL`, or its
    /// answer has not come `ANSWER_WAIT` after the body went out. There is no
    /// cap on the whole, but what the socket buffer took has to leave within
    /// `ANSWER_WAIT`, so the floor is about that buffer per 300 s.
    async fn put_body<S>(
        &self,
        key: &str,
        body: S,
        len: u64,
    ) -> Result<u16, crate::errors::CloudError>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Unpin + Send + 'static,
    {
        let action = self.bucket.put_object(Some(&self.credentials), key);
        let url = action.sign(PRESIGNED_URL_DURATION);
        let progress = Arc::new(Mutex::new(Progress {
            at: Instant::now(),
            sent: 0,
        }));
        let body = Watched {
            inner: body,
            progress: progress.clone(),
        };
        let send = self
            .upload_client
            .put(url.as_str())
            .header(reqwest::header::CONTENT_LENGTH, len)
            .body(reqwest::Body::wrap_stream(body))
            .send();
        tokio::pin!(send);
        loop {
            tokio::select! {
                response = &mut send => return Ok(response?.status().as_u16()),
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    let progress = progress.lock().unwrap();
                    let limit = if progress.sent < len { STALL } else { ANSWER_WAIT };
                    if progress.at.elapsed() > limit {
                        return Err(crate::errors::CloudError::Stalled {
                            sent: progress.sent,
                            len,
                            waited: progress.at.elapsed(),
                        });
                    }
                }
            }
        }
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
        // The object is there but ends before `start`.
        if status == 416 {
            return Ok(Some(bytes::Bytes::new()));
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

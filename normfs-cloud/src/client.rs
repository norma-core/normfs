use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use std::time::Duration;

const PRESIGNED_URL_DURATION: Duration = Duration::from_secs(3600); // 1 hour

#[derive(Clone)]
pub struct S3Client {
    bucket: Bucket,
    credentials: Credentials,
    http_client: reqwest::Client,
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
        })
    }

    pub async fn put_object(
        &self,
        key: &str,
        data: &[u8],
    ) -> Result<u16, crate::errors::CloudError> {
        let action = self.bucket.put_object(Some(&self.credentials), key);
        let url = action.sign(PRESIGNED_URL_DURATION);

        let response = self
            .http_client
            .put(url.as_str())
            .body(data.to_vec())
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

    pub async fn list_objects(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
    ) -> Result<ListObjectsResult, crate::errors::CloudError> {
        let mut query =
            rusty_s3::actions::ListObjectsV2::new(&self.bucket, Some(&self.credentials));
        query.with_prefix(prefix);

        // Build URL with delimiter as a query parameter if provided
        let mut url = query.sign(PRESIGNED_URL_DURATION);
        if let Some(delim) = delimiter {
            // Manually add delimiter to query string
            let url_str = if url.query().is_some() {
                format!("{}&delimiter={}", url.as_str(), urlencoding::encode(delim))
            } else {
                format!("{}?delimiter={}", url.as_str(), urlencoding::encode(delim))
            };
            url = url::Url::parse(&url_str)?;
        }

        let response = self.http_client.get(url.as_str()).send().await?;

        let status = response.status().as_u16();

        if status != 200 {
            return Err(crate::errors::CloudError::InvalidStatusCode(status));
        }

        let xml = response.text().await?;
        let result: ListBucketResult = quick_xml::de::from_str(&xml)?;

        Ok(ListObjectsResult {
            contents: result.contents,
            common_prefixes: result.common_prefixes,
        })
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

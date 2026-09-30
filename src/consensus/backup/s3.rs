//! The S3 calls backups make, signed with Signature Version 4: put, get,
//! list (ListObjectsV2), delete and multipart uploads. It speaks to AWS S3 and
//! to S3-compatible stores (Garage, MinIO, Ceph, R2) by path-style or
//! virtual-hosted addressing. Every request signs its payload's SHA-256, and
//! transient failures (transport errors, 5xx, 429, S3's RequestTimeout) are
//! retried with backoff.
use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, bail};
use aws_lc_rs::{digest, hmac};
use axum::body::Bytes;
use reqwest::{Method, StatusCode};

use super::hex;

#[derive(Clone)]
pub(super) struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key_id", &self.access_key_id)
            .finish_non_exhaustive()
    }
}

/// Where a bucket is and how to address it.
#[derive(Clone, Debug)]
pub(super) struct Location {
    pub scheme: String,
    /// The endpoint's host and port.
    pub authority: String,
    /// The endpoint's path, empty or starting with `/`, without a trailing `/`.
    pub base_path: String,
    pub bucket: String,
    pub virtual_host: bool,
    pub region: String,
}

pub(super) struct S3 {
    client: reqwest::Client,
    location: Location,
    credentials: Credentials,
}

/// One page of a listing: keys with their sizes, and with a delimiter the
/// common prefixes.
#[derive(Debug, Default)]
pub(super) struct Page {
    pub keys: Vec<(String, u64)>,
    pub prefixes: Vec<String>,
    pub next: Option<String>,
}

/// An S3 error response.
#[derive(Debug)]
pub(super) struct S3Error {
    pub status: StatusCode,
    pub code: String,
    pub message: String,
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "S3 {} {}", self.status.as_u16(), self.code)?;
        if !self.message.is_empty() {
            write!(f, ": {}", self.message)?;
        }
        Ok(())
    }
}

impl std::error::Error for S3Error {}

const ATTEMPTS: u32 = 6;
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

impl S3 {
    pub fn new(location: Location, credentials: Credentials) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(60))
            .build()
            .context("build the S3 client")?;
        Ok(Self {
            client,
            location,
            credentials,
        })
    }

    pub fn location(&self) -> &Location {
        &self.location
    }

    /// The host and canonical (encoded) path of an object, or of the bucket.
    fn address(&self, key: Option<&str>) -> (String, String) {
        let location = &self.location;
        let key = key.map(|key| encode(key, false)).unwrap_or_default();
        if location.virtual_host {
            (
                format!("{}.{}", location.bucket, location.authority),
                format!("{}/{key}", location.base_path),
            )
        } else {
            let bucket = encode(&location.bucket, true);
            let path = if key.is_empty() {
                format!("{}/{bucket}", location.base_path)
            } else {
                format!("{}/{bucket}/{key}", location.base_path)
            };
            (location.authority.clone(), path)
        }
    }

    /// Send a signed request, retrying transient failures, and return its
    /// successful response or the S3 error.
    async fn send(
        &self,
        method: Method,
        key: Option<&str>,
        query: &[(&str, &str)],
        body: Bytes,
        timeout: Option<Duration>,
    ) -> anyhow::Result<reqwest::Response> {
        let (host, path) = self.address(key);
        let query: Vec<(String, String)> = query
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        let payload = if body.is_empty() {
            EMPTY_SHA256.to_owned()
        } else {
            hex(digest::digest(&digest::SHA256, &body).as_ref())
        };
        let mut url = format!("{}://{host}{path}", self.location.scheme);
        let canonical = canonical_query(&query);
        if !canonical.is_empty() {
            url.push('?');
            url.push_str(&canonical);
        }
        let mut attempt = 0;
        loop {
            attempt += 1;
            let amz_date = amz_date(super::now_ms());
            let mut headers = BTreeMap::new();
            headers.insert("host".to_owned(), host.clone());
            headers.insert("x-amz-content-sha256".to_owned(), payload.clone());
            headers.insert("x-amz-date".to_owned(), amz_date.clone());
            if let Some(token) = &self.credentials.session_token {
                headers.insert("x-amz-security-token".to_owned(), token.clone());
            }
            let authorization = authorization(
                &self.credentials,
                &self.location.region,
                method.as_str(),
                &path,
                &query,
                &headers,
                &payload,
                &amz_date,
            );
            let mut request = self
                .client
                .request(method.clone(), &url)
                .header("authorization", authorization)
                .body(body.clone());
            for (name, value) in &headers {
                if name != "host" {
                    request = request.header(name.as_str(), value.as_str());
                }
            }
            if let Some(timeout) = timeout {
                request = request.timeout(timeout);
            }
            let failure = match request.send().await {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) => {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    let error = S3Error {
                        status,
                        code: tag(&text, "Code")
                            .map(unescape)
                            .unwrap_or_else(|| status.canonical_reason().unwrap_or("").to_owned()),
                        message: tag(&text, "Message").map(unescape).unwrap_or_default(),
                    };
                    let transient = status.is_server_error()
                        || status == StatusCode::TOO_MANY_REQUESTS
                        || error.code == "RequestTimeout"
                        || error.code == "SlowDown";
                    if !transient || attempt >= ATTEMPTS {
                        return Err(anyhow::Error::new(error)
                            .context(format!("{method} {}", key.unwrap_or("(bucket)"))));
                    }
                    anyhow::Error::new(error)
                }
                Err(error) => {
                    if attempt >= ATTEMPTS {
                        return Err(anyhow::Error::new(error)
                            .context(format!("{method} {}", key.unwrap_or("(bucket)"))));
                    }
                    anyhow::Error::new(error)
                }
            };
            let delay = backoff(attempt);
            tracing::debug!(target: "flower::backup", attempt, error = %format!("{failure:#}"),
                delay_ms = delay.as_millis() as u64, "S3 request failed; retrying");
            tokio::time::sleep(delay).await;
        }
    }

    pub async fn put(&self, key: &str, body: Bytes) -> anyhow::Result<()> {
        let timeout = transfer_timeout(body.len());
        self.send(Method::PUT, Some(key), &[], body, Some(timeout))
            .await?;
        Ok(())
    }

    /// An object's bytes, or `None` if there is no such object.
    pub async fn get(&self, key: &str) -> anyhow::Result<Option<Bytes>> {
        let Some(response) = self.open(key).await? else {
            return Ok(None);
        };
        Ok(Some(
            response
                .bytes()
                .await
                .with_context(|| format!("read {key}"))?,
        ))
    }

    /// An object's response, to read as it arrives, or `None` if there is no
    /// such object.
    pub async fn open(&self, key: &str) -> anyhow::Result<Option<reqwest::Response>> {
        match self
            .send(Method::GET, Some(key), &[], Bytes::new(), None)
            .await
        {
            Ok(response) => Ok(Some(response)),
            Err(error) if missing(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        match self
            .send(
                Method::DELETE,
                Some(key),
                &[],
                Bytes::new(),
                Some(Duration::from_secs(60)),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if missing(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// One page of keys under `prefix`, after `start_after`, in key order.
    pub async fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        delimiter: Option<&str>,
        continuation: Option<&str>,
    ) -> anyhow::Result<Page> {
        let mut query = vec![("list-type", "2"), ("prefix", prefix), ("max-keys", "1000")];
        if let Some(start_after) = start_after {
            query.push(("start-after", start_after));
        }
        if let Some(delimiter) = delimiter {
            query.push(("delimiter", delimiter));
        }
        if let Some(continuation) = continuation {
            query.push(("continuation-token", continuation));
        }
        let response = self
            .send(
                Method::GET,
                None,
                &query,
                Bytes::new(),
                Some(Duration::from_secs(60)),
            )
            .await?;
        let text = response.text().await.context("read an S3 listing")?;
        parse_listing(&text)
    }

    pub async fn create_upload(&self, key: &str) -> anyhow::Result<String> {
        let response = self
            .send(
                Method::POST,
                Some(key),
                &[("uploads", "")],
                Bytes::new(),
                Some(Duration::from_secs(60)),
            )
            .await?;
        let text = response.text().await?;
        tag(&text, "UploadId")
            .map(unescape)
            .with_context(|| format!("no UploadId in S3's answer to creating an upload of {key}"))
    }

    /// Upload one part (numbered from 1), returning its ETag.
    pub async fn upload_part(
        &self,
        key: &str,
        upload: &str,
        number: u32,
        body: Bytes,
    ) -> anyhow::Result<String> {
        let number = number.to_string();
        let timeout = transfer_timeout(body.len());
        let response = self
            .send(
                Method::PUT,
                Some(key),
                &[("partNumber", &number), ("uploadId", upload)],
                body,
                Some(timeout),
            )
            .await?;
        response
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .with_context(|| format!("no ETag for part {number} of {key}"))
    }

    pub async fn complete_upload(
        &self,
        key: &str,
        upload: &str,
        parts: &[(u32, String)],
    ) -> anyhow::Result<()> {
        let mut body = String::from("<CompleteMultipartUpload>");
        for (number, etag) in parts {
            body.push_str(&format!(
                "<Part><PartNumber>{number}</PartNumber><ETag>{}</ETag></Part>",
                escape(etag)
            ));
        }
        body.push_str("</CompleteMultipartUpload>");
        let response = self
            .send(
                Method::POST,
                Some(key),
                &[("uploadId", upload)],
                Bytes::from(body),
                Some(Duration::from_secs(300)),
            )
            .await?;
        // S3 can answer 200 and still fail, in the body.
        let text = response.text().await?;
        if text.contains("<Error>") {
            bail!(
                "completing the upload of {key} failed: {} {}",
                tag(&text, "Code").map(unescape).unwrap_or_default(),
                tag(&text, "Message").map(unescape).unwrap_or_default()
            );
        }
        Ok(())
    }

    pub async fn abort_upload(&self, key: &str, upload: &str) -> anyhow::Result<()> {
        match self
            .send(
                Method::DELETE,
                Some(key),
                &[("uploadId", upload)],
                Bytes::new(),
                Some(Duration::from_secs(60)),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if missing(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// A transfer of `bytes` gets at least a minute, and more at 1 MiB/s.
fn transfer_timeout(bytes: usize) -> Duration {
    Duration::from_secs(60 + (bytes as u64 >> 20))
}

fn missing(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<S3Error>()
        .is_some_and(|error| error.status == StatusCode::NOT_FOUND)
}

fn backoff(attempt: u32) -> Duration {
    let base = 250u64 << (attempt - 1).min(5);
    let mut jitter = [0u8; 2];
    let _ = getrandom::fill(&mut jitter);
    Duration::from_millis(base + u64::from(u16::from_le_bytes(jitter)) % base)
}

/// S3's URI encoding: everything but unreserved characters, and `/` in paths.
fn encode(text: &str, slash: bool) -> String {
    let mut encoded = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            b'/' if !slash => encoded.push('/'),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(name, value)| (encode(name, true), encode(value, true)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// The `Authorization` header of a request: `path` is its canonical (encoded)
/// path, `query` its parameters as they are, `headers` every signed header by
/// its lowercase name, `host` included.
#[allow(clippy::too_many_arguments)]
fn authorization(
    credentials: &Credentials,
    region: &str,
    method: &str,
    path: &str,
    query: &[(String, String)],
    headers: &BTreeMap<String, String>,
    payload: &str,
    amz_date: &str,
) -> String {
    let signed = headers.keys().cloned().collect::<Vec<_>>().join(";");
    let mut canonical = format!("{method}\n{path}\n{}\n", canonical_query(query));
    for (name, value) in headers {
        canonical.push_str(&format!("{name}:{}\n", value.trim()));
    }
    canonical.push_str(&format!("\n{signed}\n{payload}"));
    let date = &amz_date[..8];
    let scope = format!("{date}/{region}/s3/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex(digest::digest(&digest::SHA256, canonical.as_bytes()).as_ref())
    );
    let sign = |key: &[u8], data: &str| -> Vec<u8> {
        hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data.as_bytes())
            .as_ref()
            .to_vec()
    };
    let key = sign(
        format!("AWS4{}", credentials.secret_access_key).as_bytes(),
        date,
    );
    let key = sign(&key, region);
    let key = sign(&key, "s3");
    let key = sign(&key, "aws4_request");
    let signature = hex(&sign(&key, &to_sign));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}",
        credentials.access_key_id
    )
}

/// `YYYYMMDDTHHMMSSZ` for a Unix time in milliseconds.
fn amz_date(ms: u64) -> String {
    let (year, month, day, hour, minute, second) = super::civil_time(ms);
    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

fn parse_listing(text: &str) -> anyhow::Result<Page> {
    anyhow::ensure!(
        text.contains("<ListBucketResult"),
        "S3 listing is not a ListBucketResult"
    );
    let mut page = Page::default();
    for contents in tags(text, "Contents") {
        let key = tag(contents, "Key").context("S3 listing entry without a key")?;
        let size = tag(contents, "Size")
            .and_then(|size| size.trim().parse().ok())
            .unwrap_or(0);
        page.keys.push((unescape(key), size));
    }
    for prefixes in tags(text, "CommonPrefixes") {
        if let Some(prefix) = tag(prefixes, "Prefix") {
            page.prefixes.push(unescape(prefix));
        }
    }
    if tag(text, "IsTruncated").is_some_and(|truncated| truncated.trim() == "true") {
        page.next = Some(
            tag(text, "NextContinuationToken")
                .map(unescape)
                .context("truncated S3 listing without a continuation token")?,
        );
    }
    Ok(page)
}

/// The text of every `<name>…</name>` element, in order.
fn tags<'a>(xml: &'a str, name: &str) -> Vec<&'a str> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let mut found = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else { break };
        found.push(&after[..end]);
        rest = &after[end + close.len()..];
    }
    found
}

fn tag<'a>(xml: &'a str, name: &str) -> Option<&'a str> {
    tags(xml, name).into_iter().next()
}

fn unescape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find('&') {
        out.push_str(&rest[..start]);
        let after = &rest[start..];
        let Some(end) = after.find(';') else {
            out.push_str(after);
            return out;
        };
        let entity = &after[1..end];
        let decoded = match entity {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ => entity
                .strip_prefix("#x")
                .and_then(|hex| u32::from_str_radix(hex, 16).ok())
                .or_else(|| entity.strip_prefix('#').and_then(|n| n.parse().ok()))
                .and_then(char::from_u32),
        };
        match decoded {
            Some(character) => out.push(character),
            None => out.push_str(&after[..=end]),
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    out
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example() -> Credentials {
        Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
        }
    }

    fn headers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    /// The examples of AWS's Signature Version 4 documentation for S3
    /// ("Authenticating Requests: Using the Authorization Header").
    #[test]
    fn requests_sign_as_the_aws_examples_do() {
        let get = authorization(
            &example(),
            "us-east-1",
            "GET",
            "/test.txt",
            &[],
            &headers(&[
                ("host", "examplebucket.s3.amazonaws.com"),
                ("range", "bytes=0-9"),
                ("x-amz-content-sha256", EMPTY_SHA256),
                ("x-amz-date", "20130524T000000Z"),
            ]),
            EMPTY_SHA256,
            "20130524T000000Z",
        );
        assert_eq!(
            get,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
        let body = "Welcome to Amazon S3.";
        let payload = hex(digest::digest(&digest::SHA256, body.as_bytes()).as_ref());
        assert_eq!(
            payload,
            "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"
        );
        let put = authorization(
            &example(),
            "us-east-1",
            "PUT",
            &format!("/{}", encode("test$file.text", false)),
            &[],
            &headers(&[
                ("date", "Fri, 24 May 2013 00:00:00 GMT"),
                ("host", "examplebucket.s3.amazonaws.com"),
                ("x-amz-content-sha256", &payload),
                ("x-amz-date", "20130524T000000Z"),
                ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
            ]),
            &payload,
            "20130524T000000Z",
        );
        assert!(
            put.ends_with(
                "Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
            ),
            "{put}"
        );
        let list = authorization(
            &example(),
            "us-east-1",
            "GET",
            "/",
            &[
                ("max-keys".into(), "2".into()),
                ("prefix".into(), "J".into()),
            ],
            &headers(&[
                ("host", "examplebucket.s3.amazonaws.com"),
                ("x-amz-content-sha256", EMPTY_SHA256),
                ("x-amz-date", "20130524T000000Z"),
            ]),
            EMPTY_SHA256,
            "20130524T000000Z",
        );
        assert!(
            list.ends_with(
                "Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
            ),
            "{list}"
        );
        assert_eq!(amz_date(1_369_353_600_000), "20130524T000000Z");
    }

    #[test]
    fn paths_and_queries_encode_as_s3_expects() {
        assert_eq!(encode("a b/c+d~e", false), "a%20b/c%2Bd~e");
        assert_eq!(encode("a/b", true), "a%2Fb");
        assert_eq!(
            canonical_query(&[
                ("prefix".into(), "log/".into()),
                ("list-type".into(), "2".into()),
                ("uploads".into(), String::new()),
            ]),
            "list-type=2&prefix=log%2F&uploads="
        );
        let s3 = |virtual_host| {
            S3::new(
                Location {
                    scheme: "http".into(),
                    authority: "127.0.0.1:3900".into(),
                    base_path: String::new(),
                    bucket: "backups".into(),
                    virtual_host,
                    region: "garage".into(),
                },
                example(),
            )
            .unwrap()
        };
        assert_eq!(
            s3(false).address(Some("root/log/1.seg")),
            ("127.0.0.1:3900".into(), "/backups/root/log/1.seg".into())
        );
        assert_eq!(
            s3(false).address(None),
            ("127.0.0.1:3900".into(), "/backups".into())
        );
        assert_eq!(
            s3(true).address(Some("a b")),
            ("backups.127.0.0.1:3900".into(), "/a%20b".into())
        );
    }

    #[test]
    fn listings_and_errors_parse() {
        let page = parse_listing(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name><Prefix>root/</Prefix>
<KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys><Delimiter>/</Delimiter><IsTruncated>true</IsTruncated>
<Contents><Key>root/a&amp;b.seg</Key><LastModified>2026-09-30T00:00:00.000Z</LastModified><Size>12</Size></Contents>
<Contents><Key>root/c.seg</Key><Size>3</Size></Contents>
<CommonPrefixes><Prefix>root/generations/</Prefix></CommonPrefixes>
<NextContinuationToken>token&lt;1&gt;</NextContinuationToken></ListBucketResult>"#,
        )
        .unwrap();
        assert_eq!(
            page.keys,
            vec![("root/a&b.seg".into(), 12), ("root/c.seg".into(), 3)]
        );
        assert_eq!(page.prefixes, vec!["root/generations/".to_owned()]);
        assert_eq!(page.next.as_deref(), Some("token<1>"));
        let last =
            parse_listing("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>")
                .unwrap();
        assert!(last.keys.is_empty() && last.next.is_none());
        assert!(parse_listing("<Error><Code>AccessDenied</Code></Error>").is_err());
        assert_eq!(unescape("&#65;&#x42;&unknown;&"), "AB&unknown;&");
    }
}

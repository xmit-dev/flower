//! Where backups go: an S3 bucket under a prefix, or a directory
//! (`file://`), which tests and mounted volumes use. Keys are relative to
//! the backup's root and use `/` in both.
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, ensure};
use axum::body::Bytes;

use super::s3::{Credentials, Location, S3};

/// S3 needs every part of a multipart upload but the last to hold at least
/// 5 MiB.
pub(super) const MIN_PART_BYTES: usize = 5 << 20;

pub(super) enum Target {
    S3 { s3: Arc<S3>, root: String },
    Directory(PathBuf),
}

/// An object being read as it arrives.
pub(super) enum Download {
    S3(reqwest::Response),
    File(tokio::fs::File),
}

impl Download {
    pub async fn chunk(&mut self) -> anyhow::Result<Option<Bytes>> {
        match self {
            Download::S3(response) => Ok(response.chunk().await?),
            Download::File(file) => {
                use tokio::io::AsyncReadExt;
                let mut buffer = vec![0; 1 << 20];
                let read = file.read(&mut buffer).await?;
                if read == 0 {
                    return Ok(None);
                }
                buffer.truncate(read);
                Ok(Some(Bytes::from(buffer)))
            }
        }
    }
}

/// An object written in parts, which appears only once finished.
pub(super) enum Upload {
    S3 {
        key: String,
        id: String,
        parts: Vec<(u32, String)>,
    },
    File {
        file: Arc<std::fs::File>,
        temporary: PathBuf,
        path: PathBuf,
    },
}

impl Target {
    pub fn s3(location: Location, credentials: Credentials, root: String) -> anyhow::Result<Self> {
        Ok(Target::S3 {
            s3: Arc::new(S3::new(location, credentials)?),
            root,
        })
    }

    /// The target, credentials left out.
    pub fn describe(&self) -> String {
        match self {
            Target::S3 { s3, root } => {
                let location = s3.location();
                format!(
                    "s3://{}/{root} at {}://{}{} ({}, {})",
                    location.bucket,
                    location.scheme,
                    location.authority,
                    location.base_path,
                    location.region,
                    if location.virtual_host {
                        "virtual-hosted"
                    } else {
                        "path-style"
                    }
                )
            }
            Target::Directory(path) => format!("file://{}", path.display()),
        }
    }

    fn path(root: &Path, key: &str) -> anyhow::Result<PathBuf> {
        ensure!(
            !key.split('/')
                .any(|part| part.is_empty() || part == "." || part == ".."),
            "invalid backup key {key}"
        );
        Ok(root.join(key))
    }

    pub async fn put(&self, key: &str, body: Bytes) -> anyhow::Result<()> {
        match self {
            Target::S3 { s3, root } => s3.put(&format!("{root}{key}"), body).await,
            Target::Directory(directory) => {
                let path = Self::path(directory, key)?;
                tokio::task::spawn_blocking(move || write_atomically(&path, &body))
                    .await
                    .context("backup file writer")?
            }
        }
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<Option<Bytes>> {
        match self {
            Target::S3 { s3, root } => s3.get(&format!("{root}{key}")).await,
            Target::Directory(directory) => {
                match tokio::fs::read(Self::path(directory, key)?).await {
                    Ok(bytes) => Ok(Some(Bytes::from(bytes))),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(error) => Err(error).with_context(|| format!("read backup file {key}")),
                }
            }
        }
    }

    pub async fn open(&self, key: &str) -> anyhow::Result<Option<Download>> {
        match self {
            Target::S3 { s3, root } => {
                Ok(s3.open(&format!("{root}{key}")).await?.map(Download::S3))
            }
            Target::Directory(directory) => {
                match tokio::fs::File::open(Self::path(directory, key)?).await {
                    Ok(file) => Ok(Some(Download::File(file))),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(error) => Err(error).with_context(|| format!("open backup file {key}")),
                }
            }
        }
    }

    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        match self {
            Target::S3 { s3, root } => s3.delete(&format!("{root}{key}")).await,
            Target::Directory(directory) => {
                let path = Self::path(directory, key)?;
                match tokio::fs::remove_file(&path).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                    Err(error) => {
                        return Err(error).with_context(|| format!("delete backup file {key}"));
                    }
                }
                // Leave no empty directory behind, as a bucket keeps none.
                let mut parent = path.parent();
                while let Some(current) = parent
                    && current != directory.as_path()
                    && current.starts_with(directory)
                    && tokio::fs::remove_dir(current).await.is_ok()
                {
                    parent = current.parent();
                }
                Ok(())
            }
        }
    }

    /// Visit the keys under `prefix` after `start_after`, in key order, with
    /// their sizes, until `visit` returns false.
    pub async fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        mut visit: impl FnMut(&str, u64) -> bool,
    ) -> anyhow::Result<()> {
        match self {
            Target::S3 { s3, root } => {
                let full_prefix = format!("{root}{prefix}");
                let start_after = start_after.map(|key| format!("{root}{key}"));
                let mut continuation = None;
                loop {
                    let page = s3
                        .list(
                            &full_prefix,
                            start_after.as_deref(),
                            None,
                            continuation.as_deref(),
                        )
                        .await?;
                    for (key, size) in &page.keys {
                        let Some(relative) = key.strip_prefix(root.as_str()) else {
                            continue;
                        };
                        if !visit(relative, *size) {
                            return Ok(());
                        }
                    }
                    match page.next {
                        Some(next) => continuation = Some(next),
                        None => return Ok(()),
                    }
                }
            }
            Target::Directory(directory) => {
                let directory = directory.clone();
                let prefix = prefix.to_owned();
                let keys = tokio::task::spawn_blocking(move || file_keys(&directory, &prefix))
                    .await
                    .context("backup file lister")??;
                for (key, size) in keys {
                    if start_after.is_some_and(|after| key.as_str() <= after) {
                        continue;
                    }
                    if !visit(&key, size) {
                        break;
                    }
                }
                Ok(())
            }
        }
    }

    /// The names directly under `prefix` (which ends with `/`) that have
    /// keys beneath them, each ending with `/`, in order.
    pub async fn children(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        match self {
            Target::S3 { s3, root } => {
                let full_prefix = format!("{root}{prefix}");
                let mut children = Vec::new();
                let mut continuation = None;
                loop {
                    let page = s3
                        .list(&full_prefix, None, Some("/"), continuation.as_deref())
                        .await?;
                    for child in page.prefixes {
                        if let Some(relative) = child.strip_prefix(root.as_str()) {
                            children.push(relative.to_owned());
                        }
                    }
                    match page.next {
                        Some(next) => continuation = Some(next),
                        None => break,
                    }
                }
                children.sort();
                Ok(children)
            }
            Target::Directory(directory) => {
                let path = directory.join(prefix.trim_end_matches('/'));
                let prefix = prefix.to_owned();
                tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<String>> {
                    let mut children = Vec::new();
                    let entries = match std::fs::read_dir(&path) {
                        Ok(entries) => entries,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            return Ok(children);
                        }
                        Err(error) => return Err(error.into()),
                    };
                    for entry in entries {
                        let entry = entry?;
                        if entry.file_type()?.is_dir()
                            && let Some(name) = entry.file_name().to_str()
                        {
                            children.push(format!("{prefix}{name}/"));
                        }
                    }
                    children.sort();
                    Ok(children)
                })
                .await
                .context("backup file lister")?
            }
        }
    }

    pub async fn begin_upload(&self, key: &str) -> anyhow::Result<Upload> {
        match self {
            Target::S3 { s3, root } => {
                let key = format!("{root}{key}");
                let id = s3.create_upload(&key).await?;
                Ok(Upload::S3 {
                    key,
                    id,
                    parts: Vec::new(),
                })
            }
            Target::Directory(directory) => {
                let path = Self::path(directory, key)?;
                tokio::task::spawn_blocking(move || -> anyhow::Result<Upload> {
                    let parent = path.parent().context("backup file without a directory")?;
                    std::fs::create_dir_all(parent)?;
                    let temporary = temporary_path(&path);
                    let file = Arc::new(std::fs::File::create(&temporary)?);
                    Ok(Upload::File {
                        file,
                        temporary,
                        path,
                    })
                })
                .await
                .context("backup file writer")?
            }
        }
    }

    pub async fn upload_part(&self, upload: &mut Upload, body: Bytes) -> anyhow::Result<()> {
        match (self, upload) {
            (Target::S3 { s3, .. }, Upload::S3 { key, id, parts }) => {
                let number = parts.len() as u32 + 1;
                let etag = s3.upload_part(key, id, number, body).await?;
                parts.push((number, etag));
                Ok(())
            }
            (Target::Directory(_), Upload::File { file, .. }) => {
                use std::io::Write;
                let file = file.clone();
                tokio::task::spawn_blocking(move || (&*file).write_all(&body))
                    .await
                    .context("backup file writer")??;
                Ok(())
            }
            _ => anyhow::bail!("upload of another kind of target"),
        }
    }

    pub async fn finish_upload(&self, upload: Upload) -> anyhow::Result<()> {
        match (self, upload) {
            (Target::S3 { s3, .. }, Upload::S3 { key, id, parts }) => {
                s3.complete_upload(&key, &id, &parts).await
            }
            (
                Target::Directory(_),
                Upload::File {
                    file,
                    temporary,
                    path,
                },
            ) => tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                file.sync_all()?;
                drop(file);
                std::fs::rename(&temporary, &path)?;
                sync_parent(&path)
            })
            .await
            .context("backup file writer")?,
            _ => anyhow::bail!("upload of another kind of target"),
        }
    }

    pub async fn abort_upload(&self, upload: Upload) -> anyhow::Result<()> {
        match (self, upload) {
            (Target::S3 { s3, .. }, Upload::S3 { key, id, .. }) => s3.abort_upload(&key, &id).await,
            (Target::Directory(_), Upload::File { temporary, .. }) => {
                let _ = tokio::fs::remove_file(temporary).await;
                Ok(())
            }
            _ => anyhow::bail!("upload of another kind of target"),
        }
    }
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut random = [0u8; 8];
    let _ = getrandom::fill(&mut random);
    let mut name = path.file_name().unwrap_or_default().to_owned();
    name.push(format!(".partial-{}", super::hex(&random)));
    path.with_file_name(name)
}

fn write_atomically(path: &Path, body: &[u8]) -> anyhow::Result<()> {
    use std::io::Write;
    let parent = path.parent().context("backup file without a directory")?;
    std::fs::create_dir_all(parent)?;
    let temporary = temporary_path(path);
    let mut file = match std::fs::File::create(&temporary) {
        // A delete may just have removed the emptied directory.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(parent)?;
            std::fs::File::create(&temporary)?
        }
        created => created?,
    };
    file.write_all(body)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temporary, path)?;
    sync_parent(path)
}

fn sync_parent(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// Every file key under `prefix` in `root`, sorted, partial uploads left out.
fn file_keys(root: &Path, prefix: &str) -> anyhow::Result<Vec<(String, u64)>> {
    // Walk from the deepest directory the prefix names.
    let start = match prefix.rfind('/') {
        Some(end) => &prefix[..end],
        None => "",
    };
    let mut keys = Vec::new();
    let mut pending = vec![(root.join(start), start.to_owned())];
    while let Some((directory, relative)) = pending.pop() {
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let key = if relative.is_empty() {
                name.clone()
            } else {
                format!("{relative}/{name}")
            };
            let kind = entry.file_type()?;
            if kind.is_dir() {
                if key.starts_with(prefix) || prefix.starts_with(&format!("{key}/")) {
                    pending.push((entry.path(), key));
                }
            } else if kind.is_file() && key.starts_with(prefix) && !name.contains(".partial-") {
                keys.push((key, entry.metadata()?.len()));
            }
        }
    }
    keys.sort();
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn directory_targets_list_in_key_order_and_write_whole_objects() {
        let directory = tempfile::tempdir().unwrap();
        let target = Target::Directory(directory.path().to_owned());
        exercise(&target, 3).await;
        assert_eq!(
            std::fs::read_dir(directory.path().join("g/c"))
                .unwrap()
                .count(),
            1
        );
        // Deleting an object leaves no empty directory behind.
        target.delete("g/b/2").await.unwrap();
        assert!(!directory.path().join("g/b").exists());
    }

    /// Against a real store: FLOWER_BACKUP_TEST_URL=s3://BUCKET/PREFIX, with
    /// FLOWER_BACKUP_S3_* for its endpoint and credentials. Skipped without.
    #[tokio::test]
    async fn s3_targets_behave_as_directory_targets_do() {
        let Some(url) = std::env::var("FLOWER_BACKUP_TEST_URL")
            .ok()
            .filter(|url| !url.is_empty())
        else {
            eprintln!("FLOWER_BACKUP_TEST_URL is unset: skipped");
            return;
        };
        let mut random = [0u8; 8];
        getrandom::fill(&mut random).unwrap();
        let url = format!(
            "{}/target-test-{}",
            url.trim_end_matches('/'),
            super::super::hex(&random)
        );
        let target = super::super::Config::from_env_with_url(Some(&url))
            .unwrap()
            .unwrap()
            .target()
            .unwrap();
        // Multipart parts but the last need 5 MiB.
        exercise(&target, MIN_PART_BYTES).await;
        let mut keys = Vec::new();
        target
            .list("", None, |key, _| {
                keys.push(key.to_owned());
                true
            })
            .await
            .unwrap();
        for key in keys {
            target.delete(&key).await.unwrap();
        }
        let mut left = 0;
        target
            .list("", None, |_, _| {
                left += 1;
                true
            })
            .await
            .unwrap();
        assert_eq!(left, 0);
    }

    async fn exercise(target: &Target, part_bytes: usize) {
        for key in ["g/b/2", "g/a/1", "g/a/10", "h", "g/a/3"] {
            target
                .put(key, Bytes::from(key.as_bytes().to_vec()))
                .await
                .unwrap();
        }
        let mut listed = Vec::new();
        target
            .list("g/a/", Some("g/a/1"), |key, size| {
                listed.push((key.to_owned(), size));
                true
            })
            .await
            .unwrap();
        assert_eq!(listed, vec![("g/a/10".into(), 6), ("g/a/3".into(), 5)]);
        let mut first = Vec::new();
        target
            .list("g/", None, |key, _| {
                first.push(key.to_owned());
                first.len() < 2
            })
            .await
            .unwrap();
        assert_eq!(first, vec!["g/a/1", "g/a/10"]);
        assert_eq!(target.children("g/").await.unwrap(), vec!["g/a/", "g/b/"]);
        assert_eq!(
            target.get("h").await.unwrap().unwrap(),
            Bytes::from_static(b"h")
        );
        assert!(target.get("missing").await.unwrap().is_none());
        target.delete("h").await.unwrap();
        target.delete("h").await.unwrap();
        assert!(target.get("h").await.unwrap().is_none());
        assert!(target.put("../escape", Bytes::new()).await.is_err());

        let first: Vec<u8> = (0..part_bytes).map(|n| (n % 251) as u8).collect();
        let mut upload = target.begin_upload("g/c/big").await.unwrap();
        target
            .upload_part(&mut upload, Bytes::from(first.clone()))
            .await
            .unwrap();
        target
            .upload_part(&mut upload, Bytes::from_static(b"two"))
            .await
            .unwrap();
        // Nothing is listed until the upload is finished.
        let mut during = Vec::new();
        target
            .list("g/c/", None, |key, _| {
                during.push(key.to_owned());
                true
            })
            .await
            .unwrap();
        assert!(during.is_empty());
        target.finish_upload(upload).await.unwrap();
        let mut download = target.open("g/c/big").await.unwrap().unwrap();
        let mut read = Vec::new();
        while let Some(chunk) = download.chunk().await.unwrap() {
            read.extend_from_slice(&chunk);
        }
        assert!(read == [first.as_slice(), b"two"].concat());
        let upload = target.begin_upload("g/c/dropped").await.unwrap();
        target.abort_upload(upload).await.unwrap();
        let mut finished = Vec::new();
        target
            .list("g/c/", None, |key, size| {
                finished.push((key.to_owned(), size));
                true
            })
            .await
            .unwrap();
        assert_eq!(
            finished,
            vec![("g/c/big".to_owned(), part_bytes as u64 + 3)]
        );
    }
}

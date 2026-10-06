use std::fs::{File, Metadata};
use std::future::Future;
use std::io::{self, SeekFrom};
use std::os::unix::fs::FileExt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};

use crate::{Fs, FsError};

type ReadJob = Pin<Box<dyn Future<Output = Result<(Vec<u8>, io::Result<usize>), FsError>> + Send>>;

// Start small for header probes; grow sequential reads to amortize pool handoffs.
const AHEAD_MIN: usize = 64 * 1024;
const AHEAD_MAX: usize = 1024 * 1024;
// Two 2 MiB fills outperformed one 4 MiB fill with eight cached readers on the Mac.
const FILL_MAX: usize = 2 * 1024 * 1024;

pub struct ReadFile {
    fs: Fs,
    file: Arc<File>,
    /// File position of the next byte the caller gets.
    offset: u64,
    pub(crate) buffer: Vec<u8>,
    start: usize,
    end: usize,
    ahead: usize,
    pending: Option<ReadJob>,
}

impl ReadFile {
    pub(crate) fn new(fs: Fs, file: File) -> Self {
        Self {
            fs,
            file: Arc::new(file),
            offset: 0,
            buffer: Vec::new(),
            start: 0,
            end: 0,
            ahead: AHEAD_MIN,
            pending: None,
        }
    }

    pub fn metadata(&self) -> impl Future<Output = io::Result<Metadata>> + Send + 'static {
        let (fs, file) = (self.fs.clone(), self.file.clone());
        async move { fs.metadata(file).await.map_err(Into::into) }
    }

    /// File position of the first byte in the window.
    fn window_base(&self) -> u64 {
        self.offset - self.start as u64
    }

    pub async fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let offset = match from {
            SeekFrom::Start(n) => Some(n),
            SeekFrom::Current(n) => self.offset.checked_add_signed(n),
            SeekFrom::End(n) => self.metadata().await?.len().checked_add_signed(n),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid seek offset"))?;
        // Reclaim the allocation even when the read's consumer was cancelled.
        if let Some(job) = self.pending.as_mut() {
            let result = job.await;
            self.pending = None;
            if let Ok((buffer, _)) = result {
                self.buffer = buffer;
            }
            self.start = 0;
            self.end = 0;
        }
        let base = self.window_base();
        if offset >= base && offset <= base + self.end as u64 {
            self.start = (offset - base) as usize;
        } else {
            self.start = 0;
            self.end = 0;
        }
        self.offset = offset;
        Ok(offset)
    }
}

impl AsyncRead for ReadFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.start == self.end {
            if self.pending.is_none() {
                let fs = self.fs.clone();
                let file = self.file.clone();
                let offset = self.offset;
                // A seek empties the window without demonstrating sequential demand.
                if self.end > 0 {
                    self.ahead = (self.ahead * 2).min(AHEAD_MAX);
                }
                let len = buf.remaining().clamp(self.ahead, FILL_MAX);
                let mut buffer = std::mem::take(&mut self.buffer);
                self.pending = Some(Box::pin(async move {
                    fs.run_blocking(move || {
                        if buffer.len() < len {
                            buffer.resize(len, 0);
                        }
                        let n = loop {
                            match file.read_at(&mut buffer[..len], offset) {
                                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                                result => break result,
                            }
                        };
                        Ok((buffer, n))
                    })
                    .await
                }));
            }
            match self.pending.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    self.pending = None;
                    let (buffer, n) = match result {
                        Ok(result) => result,
                        Err(e) => return Poll::Ready(Err(e.into())),
                    };
                    self.buffer = buffer;
                    self.start = 0;
                    self.end = 0;
                    match n {
                        Ok(n) => self.end = n,
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }
            }
        }
        let n = buf.remaining().min(self.end - self.start);
        buf.put_slice(&self.buffer[self.start..self.start + n]);
        self.start += n;
        self.offset += n as u64;
        Poll::Ready(Ok(()))
    }
}

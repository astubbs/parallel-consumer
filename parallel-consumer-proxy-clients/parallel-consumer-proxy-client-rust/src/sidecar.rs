// Copyright (C) 2026 Antony Stubbs and contributors

//! The sidecar child process and the lifecycle pipe that keeps it alive.

use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::error::ClientError;
use crate::options::{ClientOptions, SidecarStderr};

/// The lifecycle channel's whole vocabulary: the proxy prints `port: <n>` and connects nothing
/// else to it.
const PORT_LINE_PREFIX: &str = "port: ";

/// The proxy child process.
///
/// **The stdin pipe is the parent-death signal**: this process holds the write end and never
/// writes to it, so EOF on the child's stdin is proof the parent is gone. That is why the binary is
/// launched DIRECTLY and never through a shell - a wrapper process would hold the write end open
/// and leak a JVM that still holds group membership.
pub(crate) struct Sidecar {
    child: Child,
    stdin: Option<ChildStdin>,
    pub(crate) port: u16,
}

impl Sidecar {
    /// Spawns the sidecar and waits for its port line.
    pub(crate) async fn spawn(options: &ClientOptions) -> Result<Self, ClientError> {
        // Command, never a shell: see the type comment.
        let mut command = Command::new(&options.sidecar_path);
        command
            .args(&options.sidecar_args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(match options.sidecar_stderr {
                SidecarStderr::Inherit => Stdio::inherit(),
                SidecarStderr::Null => Stdio::null(),
            });

        let mut child = command.spawn().map_err(|e| {
            ClientError::Sidecar(format!("{} could not be started: {e}", options.sidecar_path.display()))
        })?;

        let stdin = child.stdin.take();
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ClientError::Sidecar("the sidecar was spawned without a stdout pipe".to_owned()))?;

        let (port_tx, port_rx) = oneshot::channel();
        // The drain runs for the child's whole life, so a sidecar that keeps logging after the port
        // line never blocks on a full pipe buffer. It ends when the child's stdout closes.
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            let mut port_tx = Some(port_tx);
            while let Ok(Some(line)) = lines.next_line().await {
                let Some(sender) = port_tx.take() else {
                    continue; // keep draining
                };
                match parse_port_line(&line) {
                    Some(port) => {
                        let _ = sender.send(Ok(port));
                    }
                    None => port_tx = Some(sender),
                }
            }
            if let Some(sender) = port_tx.take() {
                let _ = sender.send(Err(ClientError::Sidecar(format!(
                    "the sidecar's stdout ended before a {PORT_LINE_PREFIX:?} line"
                ))));
            }
        });

        let mut sidecar = Self { child, stdin, port: 0 };

        match timeout(options.connect_timeout, port_rx).await {
            Ok(Ok(Ok(port))) => {
                sidecar.port = port;
                Ok(sidecar)
            }
            Ok(Ok(Err(e))) => {
                sidecar.stop(options.shutdown_grace).await.ok();
                Err(e)
            }
            Ok(Err(_recv)) => {
                sidecar.stop(options.shutdown_grace).await.ok();
                Err(ClientError::Sidecar(
                    "the sidecar's lifecycle channel closed without reporting a port".to_owned(),
                ))
            }
            Err(_elapsed) => {
                sidecar.stop(options.shutdown_grace).await.ok();
                Err(ClientError::Timeout(format!(
                    "waiting {:?} for the sidecar's port line",
                    options.connect_timeout
                )))
            }
        }
    }

    /// Closes the lifecycle pipe and reaps the child.
    ///
    /// **Closing stdin is the reap**: it is the parent-death signal the proxy watches, and it is
    /// also the only thing that ends the conformance harness, which serves until stdin EOF and
    /// does not exit after a clean drain. Killing is the backstop for a child that honours
    /// neither.
    pub(crate) async fn stop(mut self, grace: Duration) -> Result<(), ClientError> {
        drop(self.stdin.take());

        match timeout(grace, self.child.wait()).await {
            Ok(Ok(_status)) => Ok(()),
            Ok(Err(e)) => Err(ClientError::Sidecar(format!("could not be reaped: {e}"))),
            Err(_elapsed) => {
                self.child.start_kill().ok();
                self.child.wait().await.ok();
                Err(ClientError::Sidecar(format!(
                    "did not exit within {grace:?} of its lifecycle pipe closing, so it was killed"
                )))
            }
        }
    }
}

/// The port from a lifecycle line, if this line is one.
///
/// The specification's contract is that the port is stdout's FIRST line. The conformance harness
/// diverges - it logs before it - and the guide says a test absorbs that rather than asserting the
/// position, so the caller scans for the line instead of reading exactly one. Scanning satisfies
/// both.
fn parse_port_line(line: &str) -> Option<u16> {
    line.strip_prefix(PORT_LINE_PREFIX)?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_port_line_is_recognised_among_log_lines() {
        assert_eq!(parse_port_line("port: 43117"), Some(43117));
        assert_eq!(parse_port_line("port: 43117 "), Some(43117));
        assert_eq!(parse_port_line("12:01:02 INFO  starting up"), None);
        assert_eq!(parse_port_line("port: not-a-number"), None);
        assert_eq!(parse_port_line("the port: 43117"), None);
    }
}

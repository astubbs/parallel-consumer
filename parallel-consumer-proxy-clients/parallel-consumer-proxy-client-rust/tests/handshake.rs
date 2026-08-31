// Copyright (C) 2026 Antony Stubbs and contributors

//! The handshake, against a real sidecar process, over the real wire.
//!
//! This module's one against-a-real-process test, and the only claim it can honestly make on this
//! stack. The sidecar spawned is `parallel-consumer-proxy`'s production entry point - a real bind,
//! the real authority allowlist, the real single-connection guard, and the real session service.
//! That service hosts no engine and refuses every session, so there is no dispatch to observe here
//! and none is invented.
//!
//! What **is** observed is everything this library does before an engine would matter: launch the
//! child directly, read `port:` off its stdout, hold its stdin as the parent-death lifeline, open
//! the channel, put `Configure` on the wire, and turn what came back into a [`ClientError`]. The
//! dispatch scenarios - one record end to end, the in-flight ceiling, the redelivery history -
//! belong to the shared conformance suite and are deferred until an engine exists.
//!
//! **The status code is the assertion, not merely "it failed".** A refusal from the authority
//! allowlist is `PermissionDenied` and one from the admission slot is `ResourceExhausted`, both
//! raised by interceptors *before* the service method runs. Only `Unimplemented` can have come from
//! the service itself, so the code is what separates "the connection was turned away" from "the
//! handshake was delivered and answered".

mod harness;

use std::collections::HashMap;
use std::io::Write;

use parallel_consumer_proxy_client::{ClientError, ClientOptions, ParallelConsumerClient};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_handshake_reaches_the_session_service_and_its_refusal_reaches_the_caller() {
    let sidecar = harness::engine_less_sidecar().expect("locating the sidecar");

    let refused = failure_of(ParallelConsumerClient::connect(ClientOptions {
        sidecar_path: sidecar.path,
        sidecar_args: sidecar.args,
        topics: vec!["handshake-topic".to_owned()],
        // The sidecar reads no properties at all on this build. Real credentials never belong in a
        // test, and there is nothing here to give them to.
        kafka_properties: HashMap::new(),
        instance_tag: Some("rust-handshake".to_owned()),
        ..Default::default()
    })
    .await, "the sidecar hosts no engine, so connect must fail rather than report a session");

    let rendered = refused.to_string();
    // tonic renders a Status by its code's DESCRIPTION rather than by the variant name, and this
    // client's ClientError::Transport carries the rendering rather than the Status - so the code is
    // asserted through tonic's own constant instead of a hand-copied string. That the code cannot be
    // matched on programmatically is a real gap in this surface, recorded rather than papered over.
    assert!(
        rendered.contains(tonic::Code::Unimplemented.description()),
        "handshake failed with {rendered:?} - Unimplemented is the only code the session SERVICE \
         raises, so it is what proves the Configure was delivered rather than turned away by an \
         interceptor"
    );
    assert!(
        rendered.contains(harness::NO_ENGINE_DESCRIPTION),
        "the refusal must name what is missing, or a client author debugs their own code: \
         {rendered:?}"
    );
}

/// The control arm, permanent rather than a one-off demonstration: pointed at a port nothing is
/// listening on, the same client fails in a way that is not the refusal above. Without it, the test
/// that matters could be passing on any failure at all - which is the shape of an assertion that
/// cannot fail for the reason it names.
///
/// The stand-in announces a port and then holds its stdin, which is the spawning contract's whole
/// client-visible surface, so the library takes its **real** connect path at a dead port rather
/// than the different path a child that printed nothing would take.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sidecar_that_is_not_listening_fails_differently_from_one_that_refuses() {
    let dead_port = reserve_then_release_a_port();
    let announcer = write_announcer(dead_port);

    let failed = failure_of(ParallelConsumerClient::connect(ClientOptions {
        sidecar_path: announcer,
        topics: vec!["handshake-topic".to_owned()],
        kafka_properties: HashMap::new(),
        instance_tag: Some("rust-handshake-control".to_owned()),
        ..Default::default()
    })
    .await, "nothing is listening on that port, so connect cannot have succeeded");

    let rendered = failed.to_string();
    assert!(
        !rendered.contains(tonic::Code::Unimplemented.description()),
        "nothing answered, so nothing can have refused: {rendered:?}"
    );
}

/// The failure a connect attempt produced, or a panic naming what a success would have meant.
///
/// `Result::expect_err` cannot be used: it needs the OK type to be `Debug`, and a live client is a
/// process, a channel and a queue - deriving `Debug` on it to satisfy a test would put the session's
/// internals in anything that formatted one.
fn failure_of(attempt: Result<ParallelConsumerClient, ClientError>, why: &str) -> ClientError {
    match attempt {
        Ok(_) => panic!("{why}"),
        Err(failure) => failure,
    }
}

/// A loopback port the OS has just handed out and nothing is listening on.
fn reserve_then_release_a_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserving a port");
    listener.local_addr().expect("reading the reserved port").port()
}

/// A sidecar that announces a port and then holds its stdin. `printf` and `read` are shell
/// builtins, so it is one process holding its own lifeline and no grandchild survives the reap.
fn write_announcer(port: u16) -> std::path::PathBuf {
    let directory = std::env::temp_dir().join(format!("pc-rust-announcer-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("creating the announcer directory");
    let script = directory.join("announcer.sh");
    let mut file = std::fs::File::create(&script).expect("creating the announcer");
    write!(
        file,
        "#!/bin/sh\nprintf 'port: {port}\\n'\nwhile read -r _ignored; do :; done\nexit 0\n"
    )
    .expect("writing the announcer");
    drop(file);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700))
            .expect("making the announcer executable");
    }
    script
}

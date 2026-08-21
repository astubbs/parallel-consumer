// Copyright (C) 2026 Antony Stubbs and contributors

//! **The Rust demo.** The same records through Rust's own Kafka client and through Rust over the
//! Parallel Consumer sidecar, with throughput out.
//!
//! The contract this keeps - the flags, the environment variables, the defaults, the two replays,
//! the two tables, the fingerprint, and the rule against reporting latency - is
//! `parallel-consumer-proxy/demo/README.md`, and it binds all eleven languages. Read that first;
//! `demo/README.md` beside this file records only what is specific to Rust.
//!
//! ```text
//! parallel-consumer-proxy-clients/parallel-consumer-proxy-client-rust/demo/run.sh
//! ```

mod arms;
mod broker;
mod options;
mod sidecar;

use std::collections::HashMap;
use std::process::ExitCode;

use arms::{ArmResult, AK_CORE};
use broker::DemoBroker;
use options::{DemoOptions, USAGE};

/// A misspelled flag exits 2 rather than running with settings nobody asked for; a failed run
/// exits 1. Both are what a scripted caller reads, so they are named rather than spelled inline.
const EXIT_USAGE: u8 = 2;
const EXIT_FAILED: u8 = 1;

/// **The first thing the demo prints, and it names the product.** Not the module, not an arm, not
/// a configuration line: a reader who runs this and is met with
/// `rust-grpc: the proxy granted 100 executor threads` has been told nothing about what they are
/// looking at. Every language prints this same banner, differing only in its own name - contract,
/// in `parallel-consumer-proxy/demo/README.md`.
const BANNER: &str = "\
================================================================
  PARALLEL CONSUMER  -  Rust demo
  The same records, twice: one at a time, then all at once.
================================================================";

#[tokio::main]
async fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if DemoOptions::is_help_requested(&argv) {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }

    let environment: HashMap<String, String> = std::env::vars().collect();
    let options = match DemoOptions::parse(&argv, &environment) {
        Ok(options) => options,
        Err(problem) => {
            eprintln!("{problem}\n\n{USAGE}");
            return ExitCode::from(EXIT_USAGE);
        }
    };

    match run(options).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(problem) => {
            eprintln!("\nThe demo failed: {problem}");
            ExitCode::from(EXIT_FAILED)
        }
    }
}

async fn run(options: DemoOptions) -> Result<(), String> {
    let topic = options
        .topic
        .clone()
        .unwrap_or_else(|| format!("pc-demo-{}", arms::unique()));

    // The banner, then the fingerprint, then the run - in that order, because a reader needs to
    // know what they are watching before they can care how it was configured.
    println!("\n{BANNER}");

    // The bootstrap address is not in the fingerprint: own-cluster mode puts a user's real broker
    // there, and a number without its settings is not reproducible anyway.
    println!("\nEffective configuration:\n  {options}\n  topic = {topic}");

    let sidecar = sidecar::resolve()?;
    let broker = DemoBroker::resolve(options.bootstrap.as_deref())?;
    broker.ensure_topic(&topic, options.partitions).await?;
    broker.seed(&topic, 0, options.records)?;

    let ak = ak_core_arm(&broker, &options, &topic, options.records).await?;
    let over_the_sidecar =
        arms::rust_grpc(&broker, &options, &sidecar, &topic, options.records).await?;
    let small = vec![ak, over_the_sidecar];
    report(
        &format!(
            "Small replay - every arm over the same {} records (the comparison)",
            options.records
        ),
        &small,
        baseline_of(&small),
        false,
    );

    if !options.big_replay_wanted() {
        println!("\nBig replay skipped (--replay-factor {}).", options.replay_factor);
        return finish(&broker);
    }

    let total = options.big_replay_records();
    broker.seed(&topic, options.records, total)?;

    // AK core is excluded here because it does not go parallel: it would need total * delayMs
    // milliseconds to finish a backlog the sidecar arm clears in seconds, and a demo that makes a
    // reader wait that long to learn nothing new is not worth the wall clock.
    let big = vec![arms::rust_grpc(&broker, &options, &sidecar, &topic, total).await?];
    report(
        &format!(
            "Big replay - {total} records, parallel arms only (AK core is serial and would take {}s+)",
            total as u64 * options.delay_ms / 1000
        ),
        &big,
        baseline_of(&small),
        true,
    );
    finish(&broker)
}

/// The serial arm blocks a whole thread for its entire run, so it goes on the blocking pool rather
/// than occupying an async worker for minutes. Nothing else is running while it does.
async fn ak_core_arm(
    broker: &DemoBroker,
    options: &DemoOptions,
    topic: &str,
    target: usize,
) -> Result<ArmResult, String> {
    tokio::task::block_in_place(|| arms::ak_core(broker, options, topic, target))
}

fn finish(broker: &DemoBroker) -> Result<(), String> {
    if let Some(teardown) = broker.teardown_hint() {
        println!("\nThe broker this run started is still up. To stop it:\n  {teardown}");
    }
    Ok(())
}

fn baseline_of(results: &[ArmResult]) -> Option<&ArmResult> {
    results.iter().find(|result| result.arm == AK_CORE)
}

/// One table, in the reference's columns and the reference's order.
///
/// **`records` and `keys` are what turn the table from an assertion into a demonstration.**
/// Throughput alone cannot show the work happened - a short arm would look like a fast one - and
/// the distinct keys show the backlog was really spread rather than being one key replayed. They
/// are also the only two figures here that are deterministic, so they are the ones that can be
/// compared between languages at all.
///
/// The arm column carries the client that produced the row, because "AK core" is a category rather
/// than a client and the answer differs in every language.
///
/// `across_replays` marks the big replay's ratio column, whose denominator is the *small* replay's
/// AK core arm - the only baseline there is, because the serial arm never runs the big one. It is
/// not like-for-like, and the footnote says so rather than letting the column imply otherwise.
fn report(title: &str, results: &[ArmResult], baseline: Option<&ArmResult>, across_replays: bool) {
    println!("{}", render(title, results, baseline, across_replays));
}

/// The table as text. Split from [`report`] only so a test can read the columns a reader sees -
/// column identity and order are contract, and nothing else in this demo can assert them.
fn render(title: &str, results: &[ArmResult], baseline: Option<&ArmResult>, across_replays: bool) -> String {
    let mut table = format!("\n\n{title}\n");
    table.push_str(&format!(
        "  {:<24} {:>10} {:>14} {:>14} {:>10} {:>8}\n",
        "arm",
        "elapsed",
        "msg/s",
        if across_replays { "vs AK core*" } else { "vs AK core" },
        "records",
        "keys"
    ));
    for result in results {
        let ratio = match baseline {
            Some(base) if base.rate_per_second() != 0.0 => {
                format!("{:.1}x", result.rate_per_second() / base.rate_per_second())
            }
            _ => "-".to_owned(),
        };
        table.push_str(&format!(
            "  {:<24} {:>9.1}s {:>14} {:>14} {:>10} {:>8}\n",
            result.label(),
            result.elapsed.as_millis() as f64 / 1000.0,
            thousands(result.rate_per_second() as u64),
            ratio,
            thousands(result.processed as u64),
            thousands(result.unique_keys as u64)
        ));
    }
    if across_replays {
        table.push_str(
            "\n  * against the SMALL replay's AK core arm. Across replays, so not like-for-like.\n",
        );
    }
    table
}

/// Grouped with commas, the way the reference's `%,d` renders in the root locale - so two
/// languages' tables can be read side by side without one of them looking like a different number.
fn thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (position, digit) in digits.chars().enumerate() {
        if position > 0 && (digits.len() - position) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn result(arm: &str, millis: u64, processed: usize) -> ArmResult {
        ArmResult {
            arm: arm.to_owned(),
            client: "a client".to_owned(),
            elapsed: Duration::from_millis(millis),
            processed,
            unique_keys: processed,
        }
    }

    #[test]
    fn thousands_groups_the_way_the_reference_does() {
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(999), "999");
        assert_eq!(thousands(1_000), "1,000");
        assert_eq!(thousands(1_234_567), "1,234,567");
    }

    #[test]
    fn throughput_is_records_over_wall_clock() {
        assert_eq!(result("x", 2_000, 1_000).rate_per_second(), 500.0);
        assert_eq!(result("x", 0, 1_000).rate_per_second(), 0.0, "no division by zero");
    }

    #[test]
    fn the_banner_names_the_product_and_the_language() {
        // The contract's shape, not this demo's taste: the first thing printed says what the
        // reader is looking at. A banner that named the module or the arm would be the defect it
        // was written to remove.
        let lines: Vec<&str> = BANNER.lines().collect();

        assert_eq!(lines.len(), 4, "rule, two lines, rule");
        assert!(lines[0].starts_with("========"), "opens with a rule");
        assert_eq!(lines[1], "  PARALLEL CONSUMER  -  Rust demo");
        assert_eq!(lines[2], "  The same records, twice: one at a time, then all at once.");
        assert_eq!(lines[3], lines[0], "closes with the same rule");
    }

    #[test]
    fn every_arm_names_the_client_that_produced_its_row() {
        // "AK core" is a category rather than a client, so the row carries both.
        let ak = ArmResult {
            arm: AK_CORE.to_owned(),
            client: arms::AK_CORE_CLIENT.to_owned(),
            elapsed: Duration::from_millis(1),
            processed: 1,
            unique_keys: 1,
        };

        assert_eq!(ak.label(), "AK core (rdkafka)");
        assert_eq!(
            ArmResult {
                arm: arms::RUST_GRPC.to_owned(),
                client: arms::RUST_GRPC_CLIENT.to_owned(),
                ..ak
            }
            .label(),
            "rust-grpc (this client)"
        );
    }

    #[test]
    fn the_table_reports_records_and_keys_beside_the_rate() {
        let mut ak = result(AK_CORE, 1_000, 2_000);
        ak.client = arms::AK_CORE_CLIENT.to_owned();
        ak.unique_keys = 1_000;
        let results = vec![ak];

        let table = render("Small replay", &results, baseline_of(&results), false);
        let header = table.lines().find(|line| line.contains("msg/s")).expect("a header row");
        let row = table.lines().find(|line| line.contains("rdkafka")).expect("an arm row");

        // Column IDENTITY and ORDER are the contract; the padding is not.
        let columns: Vec<&str> = header.split_whitespace().collect();
        let expected = vec!["arm", "elapsed", "msg/s", "vs", "AK", "core", "records", "keys"];
        assert_eq!(columns, expected);
        assert!(row.contains("AK core (rdkafka)"), "the row names its client: {row}");
        // Deterministic, unlike elapsed and msg/s, which is what makes them comparable across
        // languages - and grouped the same way the rate is, so a table reads as one table.
        assert!(row.contains(" 2,000 "), "records processed: {row}");
        assert!(row.trim_end().ends_with(" 1,000"), "unique keys last: {row}");
    }

    #[test]
    fn the_baseline_is_the_ak_core_arm_and_nothing_else() {
        let results = vec![result("rust-grpc", 100, 10), result(AK_CORE, 1_000, 10)];

        assert_eq!(baseline_of(&results).unwrap().arm, AK_CORE);
        assert!(baseline_of(&results[..1]).is_none(), "no AK core arm, no baseline");
    }
}

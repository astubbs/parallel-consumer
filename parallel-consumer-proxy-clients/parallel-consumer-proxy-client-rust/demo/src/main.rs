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

    // THE FINGERPRINT COMES FIRST, and the bootstrap address is not in it: own-cluster mode puts a
    // user's real broker there, and a number without its settings is not reproducible anyway.
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
/// `across_replays` marks the big replay's ratio column, whose denominator is the *small* replay's
/// AK core arm - the only baseline there is, because the serial arm never runs the big one. It is
/// not like-for-like, and the footnote says so rather than letting the column imply otherwise.
fn report(title: &str, results: &[ArmResult], baseline: Option<&ArmResult>, across_replays: bool) {
    let mut table = format!("\n\n{title}\n");
    table.push_str(&format!(
        "  {:<14} {:>10} {:>14} {:>14}\n",
        "arm",
        "elapsed",
        "msg/s",
        if across_replays { "vs AK core*" } else { "vs AK core" }
    ));
    for result in results {
        let ratio = match baseline {
            Some(base) if base.rate_per_second() != 0.0 => {
                format!("{:.1}x", result.rate_per_second() / base.rate_per_second())
            }
            _ => "-".to_owned(),
        };
        table.push_str(&format!(
            "  {:<14} {:>9.1}s {:>14} {:>14}\n",
            result.arm,
            result.elapsed.as_millis() as f64 / 1000.0,
            thousands(result.rate_per_second() as u64),
            ratio
        ));
    }
    if across_replays {
        table.push_str(
            "\n  * against the SMALL replay's AK core arm. Across replays, so not like-for-like.\n",
        );
    }
    println!("{table}");
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
            elapsed: Duration::from_millis(millis),
            processed,
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
    fn the_baseline_is_the_ak_core_arm_and_nothing_else() {
        let results = vec![result("rust-grpc", 100, 10), result(AK_CORE, 1_000, 10)];

        assert_eq!(baseline_of(&results).unwrap().arm, AK_CORE);
        assert!(baseline_of(&results[..1]).is_none(), "no AK core arm, no baseline");
    }
}

// Copyright (C) 2026 Antony Stubbs and contributors

//! The demo's dials. **This surface is the contract**, not this demo's own idea: the same seven
//! flags, the same seven `PC_DEMO_*` environment variables, the same defaults and the same
//! precedence as every other language's copy - see `parallel-consumer-proxy/demo/README.md`.
//!
//! Flags beat the environment beats the defaults, the ordinary convention, chosen because a
//! container passes configuration by environment while a person at a terminal passes flags, and
//! both must be able to override the other's layer.
//!
//! **R39 does not govern a demo.** R39 constrains how configuration reaches the *proxy*; a demo is
//! an application, so `--records` is not a violation of it. The Java reference says the same thing
//! in the same place, because without it someone reads these flags as breaking the plan's own rule
//! and deletes them.

use std::collections::HashMap;
use std::fmt;

/// The prefix on every environment variable this demo reads, so a reader can grep one string.
pub const ENV_PREFIX: &str = "PC_DEMO_";

/// The effective configuration of one run.
#[derive(Debug, Clone)]
pub struct DemoOptions {
    /// Records in the comparison replay.
    pub records: usize,
    /// Simulated work per record, in milliseconds.
    pub delay_ms: u64,
    /// The in-flight ceiling asked of the engine.
    pub max_concurrency: usize,
    /// Partitions on the demo topic.
    pub partitions: i32,
    /// The big replay is `records * replay_factor`; 1 or less skips it.
    pub replay_factor: usize,
    /// An existing broker, or `None` to start one.
    pub bootstrap: Option<String>,
    /// An existing topic, or `None` to name a fresh one.
    pub topic: Option<String>,
}

impl Default for DemoOptions {
    fn default() -> Self {
        Self {
            records: 2_000,
            delay_ms: 2,
            max_concurrency: 100,
            partitions: 10,
            replay_factor: 20,
            bootstrap: None,
            topic: None,
        }
    }
}

impl DemoOptions {
    /// Whether the caller asked for the usage text rather than a run.
    ///
    /// Answered here rather than only in `run.sh`, because the script is not the only way in:
    /// `docker compose run demo --help` reaches this binary directly, and answering that with
    /// "unknown option: --help" would be a poor first impression of a demo ten languages copy.
    pub fn is_help_requested(argv: &[String]) -> bool {
        argv.iter().any(|argument| argument == "-h" || argument == "--help")
    }

    /// Parses the command line over the environment over the defaults.
    ///
    /// # Errors
    ///
    /// On an unknown flag, a missing value, or a value that is not a number in range. A demo that
    /// silently ignored a misspelled flag would report numbers for settings nobody asked for.
    pub fn parse(argv: &[String], env: &HashMap<String, String>) -> Result<Self, String> {
        let mut options = Self::default();
        options.apply_environment(env)?;

        let mut index = 0;
        while index < argv.len() {
            let flag = argv[index].as_str();
            match flag {
                "--records" => options.records = positive(flag, value(argv, &mut index, flag)?)?,
                "--delay-ms" => options.delay_ms = non_negative(flag, value(argv, &mut index, flag)?)?,
                "--concurrency" => {
                    options.max_concurrency = positive(flag, value(argv, &mut index, flag)?)?;
                }
                "--partitions" => {
                    options.partitions = positive(flag, value(argv, &mut index, flag)?)? as i32;
                }
                // 1 or less skips the big replay, so this one is allowed to be zero
                "--replay-factor" => {
                    options.replay_factor = non_negative(flag, value(argv, &mut index, flag)?)? as usize;
                }
                "--bootstrap" => options.bootstrap = Some(value(argv, &mut index, flag)?.to_owned()),
                "--topic" => options.topic = Some(value(argv, &mut index, flag)?.to_owned()),
                other => return Err(format!("unknown option: {other}")),
            }
            index += 1;
        }
        options.validate()?;
        Ok(options)
    }

    fn apply_environment(&mut self, env: &HashMap<String, String>) -> Result<(), String> {
        if let Some(raw) = env_value(env, "RECORDS") {
            self.records = positive("PC_DEMO_RECORDS", &raw)?;
        }
        if let Some(raw) = env_value(env, "DELAY_MS") {
            self.delay_ms = non_negative("PC_DEMO_DELAY_MS", &raw)?;
        }
        if let Some(raw) = env_value(env, "CONCURRENCY") {
            self.max_concurrency = positive("PC_DEMO_CONCURRENCY", &raw)?;
        }
        if let Some(raw) = env_value(env, "PARTITIONS") {
            self.partitions = positive("PC_DEMO_PARTITIONS", &raw)? as i32;
        }
        if let Some(raw) = env_value(env, "REPLAY_FACTOR") {
            self.replay_factor = non_negative("PC_DEMO_REPLAY_FACTOR", &raw)? as usize;
        }
        if let Some(raw) = env_value(env, "BOOTSTRAP") {
            self.bootstrap = Some(raw);
        }
        if let Some(raw) = env_value(env, "TOPIC") {
            self.topic = Some(raw);
        }
        Ok(())
    }

    /// Checked as a `u64` rather than trusted later: the Java reference found that
    /// `records * replayFactor` overflows silently, and a wrapped value turns the big replay into
    /// a tiny one that still prints a confident throughput figure. `usize` is 64-bit here, so the
    /// ceiling is the reference's own `i32` limit rather than this language's - deliberately, so
    /// the same command is refused in both.
    fn validate(&self) -> Result<(), String> {
        let big = self.records as u64 * self.replay_factor.max(1) as u64;
        if big > i32::MAX as u64 {
            return Err(format!(
                "--records times --replay-factor is {big}, which is more records than the demo can \
                 count; lower one of them"
            ));
        }
        Ok(())
    }

    /// The records the big replay consumes in total, including the small replay's own.
    pub fn big_replay_records(&self) -> usize {
        self.records * self.replay_factor.max(1)
    }

    /// Whether the big replay is worth running; a factor of 1 or less skips it.
    pub fn big_replay_wanted(&self) -> bool {
        self.replay_factor > 1
    }
}

/// The effective configuration, for printing before the run.
///
/// A number without its settings is not reproducible, so this is part of the contract every
/// language's demo keeps rather than a debugging aid. **The bootstrap address is deliberately
/// absent**: own-cluster mode puts a user's real broker address here, and the credential-hygiene
/// rule that binds the proxy binds a demo too - nothing logged, nothing echoed. The field names
/// are the reference's camel case rather than Rust's snake case, so that two languages' output can
/// be diffed line for line.
impl fmt::Display for DemoOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "records = {}\n  delayMs = {}\n  maxConcurrency = {}\n  partitions = {}\n  replayFactor = {}",
            self.records, self.delay_ms, self.max_concurrency, self.partitions, self.replay_factor
        )
    }
}

/// The value belonging to the flag at `index`, advancing past it.
fn value<'a>(argv: &'a [String], index: &mut usize, flag: &str) -> Result<&'a str, String> {
    *index += 1;
    argv.get(*index)
        .map(String::as_str)
        .ok_or_else(|| format!("{flag} needs a value"))
}

fn env_value(env: &HashMap<String, String>, suffix: &str) -> Option<String> {
    let raw = env.get(&format!("{ENV_PREFIX}{suffix}"))?.trim();
    (!raw.is_empty()).then(|| raw.to_owned())
}

fn positive(flag: &str, raw: &str) -> Result<usize, String> {
    let parsed = number(flag, raw)?;
    if parsed < 1 {
        return Err(format!("{flag} must be at least 1, got {parsed}"));
    }
    Ok(parsed as usize)
}

fn non_negative(flag: &str, raw: &str) -> Result<u64, String> {
    let parsed = number(flag, raw)?;
    if parsed < 0 {
        return Err(format!("{flag} must not be negative, got {parsed}"));
    }
    Ok(parsed as u64)
}

/// Parsed as a signed number on purpose, so `--records -1` is reported as out of range rather than
/// as "not a whole number" - the message a reader can act on.
fn number(flag: &str, raw: &str) -> Result<i64, String> {
    raw.trim()
        .parse::<i64>()
        .map_err(|_| format!("{flag} needs a whole number, got '{raw}'"))
}

/// The usage text, identical in wording to `run.sh`'s and to the reference demo's.
pub const USAGE: &str = "\
usage: demo/run.sh [options]

  --records N        records in the comparison replay   (default 2000)
  --delay-ms N       simulated work per record, ms      (default 2)
  --concurrency N    max in-flight records              (default 100)
  --partitions N     partitions on the demo topic       (default 10)
  --replay-factor N  big replay = records x N; 1 skips  (default 20)
  --bootstrap ADDR   an existing broker; omit to start one
  --topic NAME       an existing topic; omit to create one

Every flag has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.
Flags beat the environment beats the defaults.";

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn flags(argv: &[&str]) -> Vec<String> {
        argv.iter().map(|a| (*a).to_owned()).collect()
    }

    #[test]
    fn no_arguments_at_all_is_the_documented_defaults() {
        let options = DemoOptions::parse(&[], &HashMap::new()).unwrap();

        assert_eq!(options.records, 2_000);
        assert_eq!(options.delay_ms, 2);
        assert_eq!(options.max_concurrency, 100);
        assert_eq!(options.partitions, 10);
        assert_eq!(options.replay_factor, 20);
        assert_eq!(options.bootstrap, None);
        assert_eq!(options.topic, None);
    }

    #[test]
    fn a_flag_beats_the_environment_beats_the_default() {
        let environment = env(&[("PC_DEMO_RECORDS", "50"), ("PC_DEMO_DELAY_MS", "7")]);

        let options = DemoOptions::parse(&flags(&["--records", "9"]), &environment).unwrap();

        assert_eq!(options.records, 9, "the flag wins");
        assert_eq!(options.delay_ms, 7, "the environment wins over the default");
        assert_eq!(options.max_concurrency, 100, "the default survives");
    }

    #[test]
    fn every_flag_has_an_environment_variable() {
        let environment = env(&[
            ("PC_DEMO_RECORDS", "1"),
            ("PC_DEMO_DELAY_MS", "0"),
            ("PC_DEMO_CONCURRENCY", "2"),
            ("PC_DEMO_PARTITIONS", "3"),
            ("PC_DEMO_REPLAY_FACTOR", "4"),
            ("PC_DEMO_BOOTSTRAP", "somewhere:9092"),
            ("PC_DEMO_TOPIC", "given"),
        ]);

        let options = DemoOptions::parse(&[], &environment).unwrap();

        assert_eq!(options.records, 1);
        assert_eq!(options.delay_ms, 0);
        assert_eq!(options.max_concurrency, 2);
        assert_eq!(options.partitions, 3);
        assert_eq!(options.replay_factor, 4);
        assert_eq!(options.bootstrap.as_deref(), Some("somewhere:9092"));
        assert_eq!(options.topic.as_deref(), Some("given"));
    }

    #[test]
    fn the_fingerprint_never_prints_the_bootstrap_address() {
        let options = DemoOptions {
            bootstrap: Some("broker.internal.example:9092".to_owned()),
            ..Default::default()
        };

        let printed = options.to_string();

        assert!(!printed.contains("broker.internal.example"), "{printed}");
        assert!(!printed.contains("9092"), "{printed}");
        assert!(printed.starts_with("records = 2000"), "{printed}");
    }

    #[test]
    fn a_misspelled_flag_is_refused_rather_than_ignored() {
        let problem = DemoOptions::parse(&flags(&["--record", "5"]), &HashMap::new()).unwrap_err();

        assert_eq!(problem, "unknown option: --record");
    }

    #[test]
    fn a_flag_without_a_value_is_refused() {
        let problem = DemoOptions::parse(&flags(&["--records"]), &HashMap::new()).unwrap_err();

        assert_eq!(problem, "--records needs a value");
    }

    #[test]
    fn a_replay_factor_of_one_skips_the_big_replay() {
        let options = DemoOptions::parse(&flags(&["--replay-factor", "1"]), &HashMap::new()).unwrap();

        assert!(!options.big_replay_wanted());
        assert_eq!(options.big_replay_records(), options.records);
    }

    #[test]
    fn a_big_replay_larger_than_the_reference_can_count_is_refused() {
        let problem =
            DemoOptions::parse(&flags(&["--records", "2000000", "--replay-factor", "2000"]), &HashMap::new())
                .unwrap_err();

        assert!(problem.contains("more records than the demo can count"), "{problem}");
    }

    #[test]
    fn help_is_recognised_in_either_spelling() {
        assert!(DemoOptions::is_help_requested(&flags(&["--records", "5", "-h"])));
        assert!(DemoOptions::is_help_requested(&flags(&["--help"])));
        assert!(!DemoOptions::is_help_requested(&flags(&["--records", "5"])));
    }
}

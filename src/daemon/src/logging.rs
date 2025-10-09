use chrono::Timelike;
use std::path::PathBuf;
use std::str::FromStr;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::Directive;
use tracing_subscriber::fmt::{format::Writer, time::FormatTime};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub struct SystemTime;

impl FormatTime for SystemTime {
    fn format_time(&self, w: &mut Writer<'_>) -> core::fmt::Result {
        let time = chrono::prelude::Local::now();
        write!(
            w,
            "{:02}:{:02}:{:02}.{:03}",
            time.hour() % 24,
            time.minute(),
            time.second(),
            time.timestamp_subsec_millis()
        )
    }
}

#[derive(Debug, Clone)]
pub struct ExactLevelFilter {
    level: Vec<tracing::Level>,
}

impl ExactLevelFilter {
    pub fn new(level: Vec<tracing::Level>) -> Self {
        Self { level }
    }
}

// This is the core logic. We implement the `Filter` trait.
impl<S> tracing_subscriber::layer::Filter<S> for ExactLevelFilter {
    fn enabled(
        &self,
        meta: &tracing::Metadata<'_>,
        _cx: &tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        // The filter's logic: return true only if the metadata's level
        // is an exact match with the one we've configured.
        self.level.contains(meta.level())
    }
}

pub fn init_tracing() {
    let stdout_layer = fmt::layer()
        .compact()
        .with_writer(std::io::stdout)
        .with_timer(SystemTime)
        .with_filter(LevelFilter::DEBUG);
    let temp_file_path = PathBuf::from_str(&format!(
        "/tmp/tmp-nihilphase-daemon-{}.log",
        chrono::prelude::Local::now().format("%Y%m%d-%H%M%S")
    ))
    .unwrap();
    let temp_file_handle =
        std::fs::File::create(&temp_file_path).expect("Failed to create temp log file");
    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_writer(temp_file_handle)
        .with_filter(ExactLevelFilter::new(vec![
            tracing::Level::TRACE,
            tracing::Level::DEBUG,
        ]));
    // .with_filter(LevelFilter::TRACE);
    tracing_subscriber::registry()
        .with(file_layer)
        .with(stdout_layer)
        .with(
            EnvFilter::builder()
                .with_default_directive(Directive::from_str("nihilphase=trace").unwrap())
                .from_env_lossy(),
        )
        .init();
    tracing::info!(
        "Logging initialized. Level=TRACE will be saved in {:?}",
        temp_file_path
    );
}

use chrono::Timelike;
use std::str::FromStr;
use tracing_subscriber::filter::Directive;
use tracing_subscriber::fmt::{format::Writer, time::FormatTime};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter};

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

pub fn init_tracing() {
    let stdout_layer = fmt::layer()
        .compact()
        .with_writer(std::io::stdout)
        .with_timer(SystemTime);
    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(
            EnvFilter::builder()
                .with_default_directive(Directive::from_str("nihilphase=trace").unwrap())
                .from_env_lossy(),
        )
        .init();
}

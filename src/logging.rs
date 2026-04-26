use tracing_subscriber::{prelude::*, EnvFilter, Layer};

pub fn init() {
    let env_filter = EnvFilter::from_default_env().add_directive("info".parse().unwrap());

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .with_filter(env_filter);

    let registry = tracing_subscriber::registry().with(fmt_layer);

    #[cfg(feature = "console")]
    let registry = registry.with(
        console_subscriber::ConsoleLayer::builder()
            .with_default_env()
            .spawn(),
    );

    let _ = registry.try_init();
}

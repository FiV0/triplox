use tracing_subscriber::EnvFilter;

pub fn init() {
    // Initialize tracing subscriber only once
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("debug,triplox=trace"))
        .with_thread_ids(true)      // Show thread IDs
        .with_thread_names(true)    // Show thread names
        .with_file(true)           // Show file names
        .with_line_number(true)     // Show line numbers
        .with_target(false)         // Hide target
        .try_init();
}


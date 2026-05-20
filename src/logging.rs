use tracing_subscriber::EnvFilter;

pub fn init() {
    init_with_default_directive(None);
}

pub fn init_with_default_info() {
    init_with_default_directive(Some("info"));
}

fn init_with_default_directive(default_directive: Option<&str>) {
    // Initialize tracing subscriber only once
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter(default_directive))
        .with_thread_ids(true) // Show thread IDs
        .with_thread_names(true) // Show thread names
        .with_file(true) // Show file names
        .with_line_number(true) // Show line numbers
        .with_target(false) // Hide target
        .try_init();
}

fn env_filter(default_directive: Option<&str>) -> EnvFilter {
    env_filter_from_directives(
        &std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or_default(),
        default_directive,
    )
}

fn env_filter_from_directives(directives: &str, default_directive: Option<&str>) -> EnvFilter {
    let mut builder = EnvFilter::builder();
    if let Some(default_directive) = default_directive {
        builder = builder.with_default_directive(
            default_directive
                .parse()
                .expect("default logging directive should be valid"),
        );
    }
    builder.parse_lossy(directives)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_filter_is_quiet_by_default() {
        assert_eq!(format!("{}", env_filter_from_directives("", None)), "");
    }

    #[test]
    fn env_filter_can_use_info_default() {
        assert_eq!(
            format!("{}", env_filter_from_directives("", Some("info"))),
            "info"
        );
    }

    #[test]
    fn explicit_filter_overrides_default() {
        assert_eq!(
            format!(
                "{}",
                env_filter_from_directives("triplox=debug", Some("info"))
            ),
            "triplox=debug"
        );
    }
}

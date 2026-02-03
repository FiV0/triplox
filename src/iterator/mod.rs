pub(crate) mod generic_prefix_extender;
mod prefix_extender;
pub(crate) mod slate_iterator;

pub use generic_prefix_extender::GenericPrefixExtender;
pub use prefix_extender::{PatternPrefixExtender, PatternPrefixExtenderIterator};

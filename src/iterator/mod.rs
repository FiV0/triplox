pub(crate) mod generic_and_prefix_extender;
pub(crate) mod generic_not_prefix_extender;
pub(crate) mod generic_or_prefix_extender;
pub(crate) mod generic_prefix_extender;
mod prefix_extender;
pub(crate) mod slate_iterator;

pub use generic_and_prefix_extender::GenericAndPrefixExtender;
pub use generic_not_prefix_extender::GenericNotPrefixExtender;
pub use generic_or_prefix_extender::GenericOrPrefixExtender;
pub use generic_prefix_extender::GenericPrefixExtender;
pub use prefix_extender::{PatternPrefixExtender, PatternPrefixExtenderIterator};

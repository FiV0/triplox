pub(crate) mod generic_and_prefix_extender;
pub(crate) mod generic_fn_prefix_extender;
pub(crate) mod generic_not_prefix_extender;
pub(crate) mod generic_or_prefix_extender;
pub(crate) mod generic_predicate_prefix_extender;
pub(crate) mod generic_prefix_extender;
mod prefix_extender;
pub(crate) mod slate_iterator;
pub(crate) mod temporal_filter_iterator;

pub use generic_and_prefix_extender::GenericAndPrefixExtender;
pub use generic_fn_prefix_extender::GenericFnPrefixExtender;
pub use generic_not_prefix_extender::GenericNotPrefixExtender;
pub use generic_or_prefix_extender::GenericOrPrefixExtender;
pub use generic_predicate_prefix_extender::GenericPredicatePrefixExtender;
pub use generic_prefix_extender::GenericPrefixExtender;
pub use prefix_extender::{PatternPrefixExtender, PatternPrefixExtenderIterator};

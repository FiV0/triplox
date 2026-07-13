// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

pub mod entities;
pub mod intern_set;
pub use intern_set::InternSet;
// Intentionally not pub.
pub mod matcher;
mod namespaceable_name;
pub mod pretty_print;
pub mod query;
pub mod symbols;
pub mod types;
pub mod utils;
pub mod value_rc;
pub use value_rc::{Cloned, FromRc, ValueRc};

pub mod parse;

// Re-export the types we use.
pub use chrono::{DateTime, Utc};
pub use num::BigInt;
pub use ordered_float::OrderedFloat;
pub use uuid::Uuid;

// Export from our modules.
pub use parse::ParseError;
pub use types::{Span, SpannedValue, ToMicros, ToMillis, Value, ValueAndSpan};
pub use uuid::Error as UuidParseError;

pub use symbols::{Keyword, NamespacedSymbol, PlainSymbol};

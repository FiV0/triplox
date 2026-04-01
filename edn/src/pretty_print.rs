// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use chrono::{
    SecondsFormat,
};

use itertools::Itertools;
use pretty;

use std::io;
use std::borrow::Cow;

use types::Value;

impl Value {
    /// Return a pretty string representation of this `Value`.
    pub fn to_pretty(&self, width: usize) -> Result<String, io::Error> {
        let mut out = Vec::new();
        self.write_pretty(width, &mut out)?;
        Ok(String::from_utf8_lossy(&out).into_owned())
    }

    /// Write a pretty representation of this `Value` to the given writer.
    fn write_pretty<W>(&self, width: usize, out: &mut W) -> Result<(), io::Error> where W: io::Write {
        self.as_doc(&pretty::BoxAllocator).1.render(width, out)
    }

    /// Bracket a collection of values.
    ///
    /// We aim for
    /// [1 2 3]
    /// and fall back if necessary to
    /// [1,
    ///  2,
    ///  3].
    fn bracket<'a, A, T, I>(&'a self, allocator: &'a A, open: T, vs: I, close: T) -> pretty::DocBuilder<'a, A>
    where A: pretty::DocAllocator<'a>, T: Into<Cow<'a, str>>, I: IntoIterator<Item=&'a Value> {
        let open = open.into();
        let n = open.len();
        let i = vs.into_iter().map(|v| v.as_doc(allocator)).intersperse_with(|| allocator.space());
        allocator.text(open)
            .append(allocator.concat(i).nest(n as isize))
            .append(allocator.text(close))
            .group()
    }

    /// Recursively traverses this value and creates a pretty.rs document.
    /// This pretty printing implementation is optimized for edn queries
    /// readability and limited whitespace expansion.
    fn as_doc<'a, A>(&'a self, pp: &'a A) -> pretty::DocBuilder<'a, A>
        where A: pretty::DocAllocator<'a> {
        match *self {
            Value::Vector(ref vs) => self.bracket(pp, "[", vs, "]"),
            Value::List(ref vs) => self.bracket(pp, "(", vs, ")"),
            Value::Set(ref vs) => self.bracket(pp, "#{", vs, "}"),
            Value::Map(ref vs) => {
                let xs = vs.iter().rev().map(|(k, v)| k.as_doc(pp).append(pp.space()).append(v.as_doc(pp)).group()).intersperse_with(|| pp.space());
                pp.text("{")
                    .append(pp.concat(xs).nest(1))
                    .append(pp.text("}"))
                    .group()
            }
            Value::NamespacedSymbol(ref v) => pp.text(v.namespace()).append("/").append(v.name()),
            Value::PlainSymbol(ref v) => pp.text(v.to_string()),
            Value::Keyword(ref v) => pp.text(v.to_string()),
            Value::Text(ref v) => pp.text("\"").append(v.as_str()).append("\""),
            Value::Uuid(ref u) => pp.text("#uuid \"").append(u.hyphenated().to_string()).append("\""),
            Value::Instant(ref v) => pp.text("#inst \"").append(v.to_rfc3339_opts(SecondsFormat::AutoSi, true)).append("\""),
            _ => pp.text(self.to_string())
        }
    }
}

#[cfg(test)]
mod test {
    use parse;

    /// Assert that pretty-printing produces valid EDN that parses back
    /// to the same value, regardless of exact whitespace layout.
    fn assert_pretty_round_trips(input: &str, width: usize) {
        let data = parse::value(input).unwrap().without_spans();
        let pretty = data.to_pretty(width).unwrap();
        let reparsed = parse::value(&pretty).unwrap().without_spans();
        assert_eq!(data, reparsed, "pretty output did not round-trip\n  input:  {}\n  pretty: {}", input, pretty);
    }

    #[test]
    fn test_pp_io() {
        let string = "$";
        let data = parse::value(string).unwrap().without_spans();

        assert_eq!(data.write_pretty(40, &mut Vec::new()).is_ok(), true);
    }

    #[test]
    fn test_pp_types_empty() {
        let string = "[ [ ] ( ) #{ } { }, \"\" ]";
        let data = parse::value(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(40).unwrap(), "[[] () #{} {} \"\"]");
    }

    #[test]
    fn test_vector() {
        let string = "[1 2 3 4 5 6]";
        assert_pretty_round_trips(string, 20);
        assert_pretty_round_trips(string, 10);
    }

    #[test]
    fn test_map() {
        let string = "{:a 1 :b 2 :c 3}";
        assert_pretty_round_trips(string, 20);
        assert_pretty_round_trips(string, 10);
    }

    #[test]
    fn test_pp_types() {
        let string = "[ 1 2 ( 3.14 ) #{ 4N } { foo/bar 42 :baz/boz 43 } [ ] :five :six/seven eight nine/ten true false nil #f NaN #f -Infinity #f +Infinity ]";
        assert_pretty_round_trips(string, 40);
    }

    #[test]
    fn test_pp_query1() {
        let string = "[:find ?id ?bar ?baz :in $ :where [?id :session/keyword-foo ?symbol1 ?symbol2 \"some string\"] [?tx :db/tx ?ts]]";
        assert_pretty_round_trips(string, 40);
    }

    #[test]
    fn test_pp_query2() {
        let string = "[:find [?id ?bar ?baz] :in [$] :where [?id :session/keyword-foo ?symbol1 ?symbol2 \"some string\"] [?tx :db/tx ?ts] (not-join [?id] [?id :session/keyword-bar _])]";
        assert_pretty_round_trips(string, 40);
    }
}

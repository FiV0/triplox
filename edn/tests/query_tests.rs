// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use edn::{Keyword, PlainSymbol};

use edn::query::{
    Binding, Direction, Element, FindSpec, FnArg, Limit, NonIntegerConstant, OrJoin, OrWhereClause,
    Order, Pattern, PatternNonValuePlace, PatternValuePlace, Predicate, ToVariable, UnifyVars,
    WhereClause,
};

use edn::parse::parse_query;

// N.B., parsing a query can be done without reference to a DB.
// Processing the parsed query into something we can work with
// for planning involves interrogating the schema and idents in
// the store.
// See <https://github.com/mozilla/mentat/wiki/Querying> for more.
#[test]
fn can_parse_predicates() {
    let s = "[:find [?x ...] :where [?x _ ?y] [(< ?y 10)]]";
    let p = parse_query(s).unwrap();

    assert_eq!(
        p.find_spec,
        FindSpec::FindColl(Element::Variable("?x".to_var()))
    );
    assert_eq!(
        p.where_clauses,
        vec![
            WhereClause::Pattern(Pattern {
                source: None,
                entity: PatternNonValuePlace::Variable("?x".to_var()),
                attribute: PatternNonValuePlace::Placeholder,
                value: PatternValuePlace::Variable("?y".to_var()),
                tx: PatternNonValuePlace::Placeholder,
            }),
            WhereClause::Pred(Predicate {
                expr: FnArg::SExpr(
                    PlainSymbol::plain("<"),
                    vec![FnArg::Variable("?y".to_var()), FnArg::EntidOrInteger(10)],
                ),
            }),
        ]
    );
}

#[test]
fn can_parse_simple_or() {
    let s = "[:find ?x . :where (or [?x _ 10] [?x _ 15])]";
    let p = parse_query(s).unwrap();

    assert_eq!(
        p.find_spec,
        FindSpec::FindScalar(Element::Variable("?x".to_var()))
    );
    assert_eq!(
        p.where_clauses,
        vec![WhereClause::OrJoin(OrJoin::new(
            UnifyVars::Implicit,
            vec![
                OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                    source: None,
                    entity: PatternNonValuePlace::Variable("?x".to_var()),
                    attribute: PatternNonValuePlace::Placeholder,
                    value: PatternValuePlace::EntidOrInteger(10),
                    tx: PatternNonValuePlace::Placeholder,
                })),
                OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                    source: None,
                    entity: PatternNonValuePlace::Variable("?x".to_var()),
                    attribute: PatternNonValuePlace::Placeholder,
                    value: PatternValuePlace::EntidOrInteger(15),
                    tx: PatternNonValuePlace::Placeholder,
                })),
            ],
        )),]
    );
}

#[test]
fn can_parse_unit_or_join() {
    let s = "[:find ?x . :where (or-join [?x] [?x _ 15])]";
    let p = parse_query(s).expect("to be able to parse find");

    assert_eq!(
        p.find_spec,
        FindSpec::FindScalar(Element::Variable("?x".to_var()))
    );
    assert_eq!(
        p.where_clauses,
        vec![WhereClause::OrJoin(OrJoin::new(
            UnifyVars::Explicit(std::iter::once("?x".to_var()).collect()),
            vec![OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                source: None,
                entity: PatternNonValuePlace::Variable("?x".to_var()),
                attribute: PatternNonValuePlace::Placeholder,
                value: PatternValuePlace::EntidOrInteger(15),
                tx: PatternNonValuePlace::Placeholder,
            })),],
        )),]
    );
}

#[test]
fn can_parse_simple_or_join() {
    let s = "[:find ?x . :where (or-join [?x] [?x _ 10] [?x _ -15])]";
    let p = parse_query(s).unwrap();

    assert_eq!(
        p.find_spec,
        FindSpec::FindScalar(Element::Variable("?x".to_var()))
    );
    assert_eq!(
        p.where_clauses,
        vec![WhereClause::OrJoin(OrJoin::new(
            UnifyVars::Explicit(std::iter::once("?x".to_var()).collect()),
            vec![
                OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                    source: None,
                    entity: PatternNonValuePlace::Variable("?x".to_var()),
                    attribute: PatternNonValuePlace::Placeholder,
                    value: PatternValuePlace::EntidOrInteger(10),
                    tx: PatternNonValuePlace::Placeholder,
                })),
                OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                    source: None,
                    entity: PatternNonValuePlace::Variable("?x".to_var()),
                    attribute: PatternNonValuePlace::Placeholder,
                    value: PatternValuePlace::EntidOrInteger(-15),
                    tx: PatternNonValuePlace::Placeholder,
                })),
            ],
        )),]
    );
}

#[cfg(test)]
fn ident(ns: &str, name: &str) -> PatternNonValuePlace {
    Keyword::namespaced(ns, name).into()
}

#[test]
fn can_parse_simple_or_and_join() {
    let s = "[:find ?x . :where (or [?x _ 10] (and (or [?x :foo/bar ?y] [?x :foo/baz ?y]) [(< ?y 1)]))]";
    let p = parse_query(s).unwrap();

    assert_eq!(
        p.find_spec,
        FindSpec::FindScalar(Element::Variable("?x".to_var()))
    );
    assert_eq!(
        p.where_clauses,
        vec![WhereClause::OrJoin(OrJoin::new(
            UnifyVars::Implicit,
            vec![
                OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                    source: None,
                    entity: PatternNonValuePlace::Variable("?x".to_var()),
                    attribute: PatternNonValuePlace::Placeholder,
                    value: PatternValuePlace::EntidOrInteger(10),
                    tx: PatternNonValuePlace::Placeholder,
                })),
                OrWhereClause::And(vec![
                    WhereClause::OrJoin(OrJoin::new(
                        UnifyVars::Implicit,
                        vec![
                            OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                                source: None,
                                entity: PatternNonValuePlace::Variable("?x".to_var()),
                                attribute: ident("foo", "bar"),
                                value: PatternValuePlace::Variable("?y".to_var()),
                                tx: PatternNonValuePlace::Placeholder,
                            })),
                            OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                                source: None,
                                entity: PatternNonValuePlace::Variable("?x".to_var()),
                                attribute: ident("foo", "baz"),
                                value: PatternValuePlace::Variable("?y".to_var()),
                                tx: PatternNonValuePlace::Placeholder,
                            })),
                        ],
                    )),
                    WhereClause::Pred(Predicate {
                        expr: FnArg::SExpr(
                            PlainSymbol::plain("<"),
                            vec![FnArg::Variable("?y".to_var()), FnArg::EntidOrInteger(1)],
                        ),
                    }),
                ],)
            ],
        )),]
    );
}

#[test]
fn can_parse_order_by() {
    let invalid = "[:find ?x :where [?x :foo/baz ?y] :order]";
    assert!(parse_query(invalid).is_err());

    // Defaults to ascending.
    let default = "[:find ?x :where [?x :foo/baz ?y] :order [?y]]";
    assert_eq!(
        parse_query(default).unwrap().order,
        Some(vec![Order(Direction::Ascending, "?y".to_var())])
    );

    let ascending = "[:find ?x :where [?x :foo/baz ?y] :order [?y :asc]]";
    assert_eq!(
        parse_query(ascending).unwrap().order,
        Some(vec![Order(Direction::Ascending, "?y".to_var())])
    );

    let descending = "[:find ?x :where [?x :foo/baz ?y] :order [?y :desc]]";
    assert_eq!(
        parse_query(descending).unwrap().order,
        Some(vec![Order(Direction::Descending, "?y".to_var())])
    );

    let mixed = "[:find ?x :where [?x :foo/baz ?y] :order [?y :desc] [?x :asc]]";
    assert_eq!(
        parse_query(mixed).unwrap().order,
        Some(vec![
            Order(Direction::Descending, "?y".to_var()),
            Order(Direction::Ascending, "?x".to_var())
        ])
    );
}

#[test]
fn can_parse_limit() {
    let invalid = "[:find ?x :where [?x :foo/baz ?y] :limit]";
    assert!(parse_query(invalid).is_err());

    let zero_invalid = "[:find ?x :where [?x :foo/baz ?y] :limit 00]";
    assert!(parse_query(zero_invalid).is_err());

    let none = "[:find ?x :where [?x :foo/baz ?y]]";
    assert_eq!(parse_query(none).unwrap().limit, Limit::None);

    let one = "[:find ?x :where [?x :foo/baz ?y] :limit 1]";
    assert_eq!(parse_query(one).unwrap().limit, Limit::Fixed(1));

    let onethousand = "[:find ?x :where [?x :foo/baz ?y] :limit 1000]";
    assert_eq!(parse_query(onethousand).unwrap().limit, Limit::Fixed(1000));

    let variable_with_in = "[:find ?x :in ?limit :where [?x :foo/baz ?y] :limit ?limit]";
    assert_eq!(
        parse_query(variable_with_in).unwrap().limit,
        Limit::Variable("?limit".to_var())
    );

    let variable_with_in_used = "[:find ?x :in ?limit :where [?x :foo/baz ?limit] :limit ?limit]";
    assert_eq!(
        parse_query(variable_with_in_used).unwrap().limit,
        Limit::Variable("?limit".to_var())
    );
}

#[test]
fn can_parse_uuid() {
    let expected =
        edn::Uuid::parse_str("4cb3f828-752d-497a-90c9-b1fd516d5644").expect("valid uuid");
    let s = "[:find ?x :where [?x :foo/baz #uuid \"4cb3f828-752d-497a-90c9-b1fd516d5644\"]]";
    assert_eq!(
        parse_query(s)
            .expect("parsed")
            .where_clauses
            .pop()
            .expect("a where clause"),
        WhereClause::Pattern(
            Pattern::new(
                None,
                PatternNonValuePlace::Variable("?x".to_var()),
                Keyword::namespaced("foo", "baz").into(),
                PatternValuePlace::Constant(NonIntegerConstant::Uuid(expected)),
                PatternNonValuePlace::Placeholder
            )
            .expect("valid pattern")
        )
    );
}

#[test]
fn can_parse_exotic_whitespace() {
    let expected =
        edn::Uuid::parse_str("4cb3f828-752d-497a-90c9-b1fd516d5644").expect("valid uuid");
    // The query string from `can_parse_uuid`, with newlines, commas, and line comments interspersed.
    let s = r#"[:find
?x ,, :where,   ;atest
[?x :foo/baz #uuid
   "4cb3f828-752d-497a-90c9-b1fd516d5644", ;testa
,],,  ,],;"#;
    assert_eq!(
        parse_query(s)
            .expect("parsed")
            .where_clauses
            .pop()
            .expect("a where clause"),
        WhereClause::Pattern(
            Pattern::new(
                None,
                PatternNonValuePlace::Variable("?x".to_var()),
                Keyword::namespaced("foo", "baz").into(),
                PatternValuePlace::Constant(NonIntegerConstant::Uuid(expected)),
                PatternNonValuePlace::Placeholder
            )
            .expect("valid pattern")
        )
    );
}

// ---------------------------------------------------------------------------
// Display round-trip tests
// ---------------------------------------------------------------------------

/// Parse a query, Display it, re-parse, and assert structural equality.
fn assert_round_trip(input: &str) {
    let parsed = parse_query(input).expect("initial parse failed");
    let displayed = parsed.to_string();
    let reparsed = parse_query(&displayed).unwrap_or_else(|e| {
        panic!(
            "re-parse of Display output failed: {}\nDisplayed: {}",
            e, displayed
        )
    });
    assert_eq!(
        parsed, reparsed,
        "round-trip mismatch\n  input:     {}\n  displayed: {}",
        input, displayed
    );
}

#[test]
fn round_trip_predicate() {
    assert_round_trip("[:find ?x :where [?x _ ?y] [(< ?y 10)]]");
}

#[test]
fn round_trip_where_fn() {
    assert_round_trip("[:find ?x ?result :where [?x _ ?y] [(+ ?y 2) ?result]]");
}

#[test]
fn rejects_literal_top_level_predicate() {
    assert!(parse_query(r#"[:find ?x :where [?x _ ?y] ["foo"]]"#).is_err());
}

#[test]
fn rejects_literal_top_level_where_fn() {
    assert!(parse_query(r#"[:find ?x ?out :where [?x _ ?y] ["foo" ?out]]"#).is_err());
}

#[test]
fn rejects_vector_fn_arg() {
    assert!(parse_query(r#"[:find ?x :where [?x _ ?y] [(contains? [?y] ?y)]]"#).is_err());
}

#[test]
fn round_trip_nested_expr_in_where_fn() {
    assert_round_trip("[:find ?x ?result :where [?x _ ?y] [(+ (* ?y 2) 1) ?result]]");
}

#[test]
fn round_trip_nested_expr_in_predicate() {
    assert_round_trip("[:find ?x :where [?x _ ?y] [(< (+ ?y 1) 10)]]");
}

#[test]
fn round_trip_if_in_where_fn() {
    assert_round_trip("[:find ?x ?result :where [?x _ ?y] [(if (= ?y 1) 10 0) ?result]]");
}

#[test]
fn round_trip_simple_pattern() {
    assert_round_trip("[:find ?x ?y :where [?x :foo/bar ?y]]");
}

#[test]
fn round_trip_or() {
    assert_round_trip("[:find ?x :where (or [?x _ 10] [?x _ 15])]");
}

#[test]
fn round_trip_or_join() {
    assert_round_trip("[:find ?x :where (or-join [?x] [?x _ 10] [?x _ 15])]");
}

#[test]
fn round_trip_not() {
    assert_round_trip("[:find ?x :where [?x :foo/bar _] (not [?x :foo/baz _])]");
}

#[test]
fn round_trip_string_with_special_chars() {
    assert_round_trip(r#"[:find ?x :where [?x :name "Alice \"Bob\" \\ \n"]]"#);
}

#[test]
fn round_trip_uuid() {
    assert_round_trip(r#"[:find ?x :where [?x :id #uuid "4cb3f828-752d-497a-90c9-b1fd516d5644"]]"#);
}

#[test]
fn round_trip_boolean() {
    assert_round_trip("[:find ?x :where [?x :active true]]");
}

#[test]
fn round_trip_float() {
    assert_round_trip("[:find ?x :where [?x :score 3.14]]");
}

#[test]
fn round_trip_integer_like_float() {
    // A float with zero fractional part must display as "1.0" not "1"
    assert_round_trip("[:find ?x :where [?x :score 1.0]]");
}

#[test]
fn round_trip_negative_integer() {
    assert_round_trip("[:find ?x :where [?x :foo/bar -5]]");
}

#[test]
fn round_trip_multiple_predicates() {
    assert_round_trip("[:find ?x :where [?x _ ?y] [(> ?y 0)] [(< ?y 100)]]");
}

#[test]
fn round_trip_or_and() {
    assert_round_trip("[:find ?x :where (or (and [?x :foo/bar _] [?x :foo/baz _]) [?x _ 10])]");
}

#[test]
fn can_parse_map_form_in_vars() {
    // Single :in variable in map form
    let single = "{:find [?e] :in [?name] :where [[?e :name ?name]]}";
    let parsed = parse_query(single).expect("map-form :in should parse");
    assert_eq!(
        parsed.in_bindings,
        vec![Binding::BindScalar("?name".to_var())]
    );

    // Multiple :in variables in map form
    let multi = "{:find [?e] :in [?name ?age] :where [[?e :name ?name] [?e :age ?age]]}";
    let parsed = parse_query(multi).expect("map-form multi :in should parse");
    assert_eq!(
        parsed.in_bindings,
        vec![
            Binding::BindScalar("?name".to_var()),
            Binding::BindScalar("?age".to_var()),
        ]
    );

    // Empty :in in map form
    let empty = "{:find [?e] :in [] :where [[?e :name ?name]]}";
    let parsed = parse_query(empty).expect("map-form empty :in should parse");
    assert!(parsed.in_bindings.is_empty());
}

#[test]
fn can_parse_list_form_in_vars() {
    // Single :in variable in list form
    let single = "[:find ?e :in ?name :where [?e :name ?name]]";
    let parsed = parse_query(single).expect("list-form :in should parse");
    assert_eq!(
        parsed.in_bindings,
        vec![Binding::BindScalar("?name".to_var())]
    );

    // Multiple :in variables in list form
    let multi = "[:find ?e :in ?name ?age :where [?e :name ?name] [?e :age ?age]]";
    let parsed = parse_query(multi).expect("list-form multi :in should parse");
    assert_eq!(
        parsed.in_bindings,
        vec![
            Binding::BindScalar("?name".to_var()),
            Binding::BindScalar("?age".to_var()),
        ]
    );

    // Empty :in in list form (no variables between :in and the next clause)
    let empty = "[:find ?e :in :where [?e :name ?name]]";
    let parsed = parse_query(empty).expect("list-form empty :in should parse");
    assert!(parsed.in_bindings.is_empty());
}

#[test]
fn can_parse_collection_binding_list_form() {
    let s = "[:find ?name :in [?name ...] :where [?e :name ?name]]";
    let parsed = parse_query(s).expect("collection binding should parse");
    assert_eq!(
        parsed.in_bindings,
        vec![Binding::BindColl("?name".to_var())]
    );
}

#[test]
fn can_parse_collection_binding_map_form() {
    let s = "{:find [?name] :in [[?name ...]] :where [[?e :name ?name]]}";
    let parsed = parse_query(s).expect("map-form collection binding should parse");
    assert_eq!(
        parsed.in_bindings,
        vec![Binding::BindColl("?name".to_var())]
    );
}

#[test]
fn can_parse_mixed_scalar_and_collection_bindings() {
    let s = "[:find ?name :in ?age [?name ...] :where [?e :name ?name] [?e :age ?age]]";
    let parsed = parse_query(s).expect("mixed bindings should parse");
    assert_eq!(
        parsed.in_bindings,
        vec![
            Binding::BindScalar("?age".to_var()),
            Binding::BindColl("?name".to_var()),
        ]
    );
}

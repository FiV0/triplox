use std::collections::HashSet;

use edn::query::{PatternNonValuePlace, PatternValuePlace, ToVariable};

use super::*;
use crate::query_validation::validate_query;

fn var(name: &str) -> Variable {
    name.to_var()
}

fn relation(id: PatternId, variables: &[&str]) -> Descriptor {
    Descriptor {
        id,
        variables: variables.iter().map(|name| var(name)).collect(),
        kind: DescriptorKind::Relation { rows: Vec::new() },
    }
}

fn predicate(id: PatternId, variables: &[&str]) -> Descriptor {
    Descriptor {
        id,
        variables: variables.iter().map(|name| var(name)).collect(),
        kind: DescriptorKind::Predicate {
            expression: Expr::Literal(DataType::Boolean(true)),
        },
    }
}

fn function(id: PatternId, input: &str, output: &str) -> Descriptor {
    let input = var(input);
    let output = var(output);
    Descriptor {
        id,
        variables: vec![input.clone(), output.clone()],
        kind: DescriptorKind::Function {
            expression: Expr::Variable(input.clone()),
            input_variables: vec![input],
            output,
        },
    }
}

fn or(id: PatternId, variables: &[&str], branches: Vec<Vec<Descriptor>>) -> Descriptor {
    Descriptor {
        id,
        variables: variables.iter().map(|name| var(name)).collect(),
        kind: DescriptorKind::Or { branches },
    }
}

fn refs(ids: &[PatternId]) -> Vec<ParticipantRef> {
    ids.iter().copied().map(ParticipantRef::Pattern).collect()
}

#[test]
fn lowers_query_data_to_one_recursive_descriptor_tree() {
    let query = edn::parse::parse_query(
        r#"[:find ?e ?next
            :in ?name [?age ...]
            :where
            [?e :person/name ?name]
            [(< ?age 30)]
            [(+ ?age 1) ?next]
            (not [?e :person/blocked true])]"#,
    )
    .unwrap();
    let arguments = vec![
        QueryArg::Scalar(DataType::String("Alice".into())),
        QueryArg::Collection(vec![DataType::Long(20), DataType::Long(40)]),
    ];
    validate_query(&query, &arguments).unwrap();

    let plan = build_logical_plan(&query, &arguments).unwrap();

    assert_eq!(
        plan.variable_order(),
        &[var("?name"), var("?age"), var("?e"), var("?next")]
    );
    assert_eq!(plan.descriptors.len(), 6);
    assert!(matches!(
        plan.descriptors[0].kind,
        DescriptorKind::Relation { .. }
    ));
    assert!(matches!(
        plan.descriptors[2].kind,
        DescriptorKind::Triple(_)
    ));
    assert!(matches!(
        plan.descriptors[3].kind,
        DescriptorKind::Predicate { .. }
    ));
    assert!(matches!(
        plan.descriptors[4].kind,
        DescriptorKind::Function { .. }
    ));
    let DescriptorKind::Not { children } = &plan.descriptors[5].kind else {
        panic!("expected NOT descriptor");
    };
    assert_eq!(children[0].id, 6);
    assert!(matches!(children[0].kind, DescriptorKind::Triple(_)));
}

#[test]
fn composite_descriptors_distinct_child_variables_in_occurrence_order() {
    let query = edn::parse::parse_query(
        r#"[:find ?e
            :where
            (or (and [?e :a ?x] [?x :b ?e])
                (and [?e :c ?x] [?x :d ?e]))
            (not [?e :blocked-by ?x] [?x :active ?e])]"#,
    )
    .unwrap();
    let mut builder = DescriptorBuilder::new();

    let descriptors = builder.where_clauses(&query.where_clauses).unwrap();

    assert_eq!(descriptors[0].variables, vec![var("?e"), var("?x")]);
    assert_eq!(descriptors[1].variables, vec![var("?e"), var("?x")]);
}

#[test]
fn descriptors_report_only_current_scope_groundability() {
    let relation_descriptor = relation(0, &["?x", "?y"]);
    let function_descriptor = function(1, "?x", "?y");
    let mut bound = HashSet::new();
    assert_eq!(relation_descriptor.groundable(&bound), vec![var("?x")]);
    assert!(function_descriptor.groundable(&bound).is_empty());

    bound.insert(var("?x"));
    assert_eq!(relation_descriptor.groundable(&bound), vec![var("?y")]);
    assert_eq!(function_descriptor.groundable(&bound), vec![var("?y")]);

    let grouped = or(
        2,
        &["?x", "?y"],
        vec![
            vec![relation(3, &["?x"]), function(4, "?x", "?y")],
            vec![relation(5, &["?x", "?y"])],
        ],
    );
    assert_eq!(
        grouped.groundable(&HashSet::new()),
        vec![var("?x"), var("?y")]
    );
}

#[test]
fn plans_multiple_proposers_and_ready_validators_together() {
    let descriptors = vec![
        relation(0, &["?x"]),
        relation(1, &["?x"]),
        predicate(2, &["?x"]),
    ];

    let stages = plan_scope(&descriptors, &[var("?x")], None).unwrap();

    assert_eq!(
        stages,
        vec![LogicalStage {
            added: vec![var("?x")],
            proposers: refs(&[0, 1]),
            participants: refs(&[0, 1, 2]),
            target_variables: vec![var("?x")],
        }]
    );
}

#[test]
fn grouped_or_proposes_all_missing_variables_then_constraints_validate() {
    let descriptors = vec![
        or(
            0,
            &["?x", "?y"],
            vec![
                vec![relation(1, &["?x", "?y"])],
                vec![relation(2, &["?x", "?y"])],
            ],
        ),
        predicate(3, &["?x", "?y"]),
    ];

    let stages = plan_scope(&descriptors, &[var("?x"), var("?y")], None).unwrap();

    assert_eq!(stages[0].added, vec![var("?x"), var("?y")]);
    assert_eq!(stages[0].proposers, refs(&[0]));
    assert_eq!(stages[0].participants, refs(&[0]));
    assert!(stages[1].added.is_empty());
    assert_eq!(stages[1].participants, refs(&[3]));
}

#[test]
fn first_variable_uses_grouped_or_before_a_later_variable_proposer() {
    let descriptors = vec![
        or(
            0,
            &["?x", "?y"],
            vec![
                vec![relation(1, &["?x", "?y"])],
                vec![relation(2, &["?x", "?y"])],
            ],
        ),
        relation(3, &["?y"]),
    ];

    let stages = plan_scope(&descriptors, &[var("?x"), var("?y")], None).unwrap();

    assert_eq!(stages[0].added, vec![var("?x"), var("?y")]);
    assert_eq!(stages[0].proposers, refs(&[0]));
}

#[test]
fn incoming_is_projected_but_empty_incoming_remains_explicit() {
    let descriptors = vec![relation(0, &["?x"])];
    let incoming = vec![var("?outer"), var("?x")];
    let stages = plan_scope(&descriptors, &[var("?x")], Some(&incoming)).unwrap();
    assert_eq!(
        stages[0].proposers,
        vec![ParticipantRef::Incoming, ParticipantRef::Pattern(0)]
    );

    let stages = plan_scope(&[], &[], Some(&[])).unwrap();
    assert_eq!(
        stages,
        vec![LogicalStage {
            added: Vec::new(),
            proposers: Vec::new(),
            participants: vec![ParticipantRef::Incoming],
            target_variables: Vec::new(),
        }]
    );
}

#[test]
fn target_layout_follows_scope_order() {
    let descriptors = vec![function(0, "?x", "?y"), relation(1, &["?x"])];

    let stages = plan_scope(&descriptors, &[var("?y"), var("?x")], None).unwrap();

    assert_eq!(stages[0].target_variables, vec![var("?x")]);
    assert_eq!(stages[1].target_variables, vec![var("?y"), var("?x")]);
}

#[test]
fn reports_insufficient_binding() {
    let descriptors = vec![predicate(0, &["?x"])];

    let error = plan_scope(&descriptors, &[var("?x")], None).unwrap_err();

    assert!(error.to_string().contains("Insufficient binding"));
}

#[test]
fn triple_descriptors_keep_runtime_independent_pattern_data() {
    let pattern = Pattern::simple(
        PatternNonValuePlace::Variable(var("?e")),
        PatternNonValuePlace::Entid(42),
        PatternValuePlace::Variable(var("?v")),
    )
    .unwrap();
    let mut builder = DescriptorBuilder::new();

    let descriptor = builder.triple(&pattern);

    assert_eq!(descriptor.variables, vec![var("?e"), var("?v")]);
    assert_eq!(descriptor.kind, DescriptorKind::Triple(pattern));
}

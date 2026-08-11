//! Incremental-query planning helpers.

use anyhow::{anyhow, bail, Result};
use edn::query::{
    Element, FindSpec, Limit, OrJoin, OrWhereClause, ParsedQuery, Pattern, PatternNonValuePlace,
    Variable, WhereClause,
};

use crate::query_validation::validate_query;
use crate::schema::Schema;

mod descriptor;
mod planner;

pub(crate) use descriptor::PatternSlot;
pub(crate) use planner::{PatternPlan, RelPlan, RelPlanKind};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IncrementalQueryPlan {
    pub find_vars: Vec<Variable>,
    pub where_plan: RelPlan,
}

impl IncrementalQueryPlan {
    pub(crate) fn leaf_patterns(&self) -> Vec<&PatternPlan> {
        let mut patterns = Vec::new();
        planner::collect_leaf_patterns(&self.where_plan, &mut patterns);
        patterns
    }
}

pub(crate) fn plan_query(query: &ParsedQuery, schema: &Schema) -> Result<IncrementalQueryPlan> {
    reject_unsupported_query_shape(query)?;
    let find_vars = find_vars(&query.find_spec)?;
    validate_query(query, &[])?;

    let descriptors = descriptor::describe_where_clauses(&query.where_clauses, schema)?;
    let where_plan = planner::plan_scope(&descriptors, None)?;
    for var in &find_vars {
        if !where_plan.output_vars.contains(var) {
            bail!(
                "Find variable {} is not bound by incremental query patterns",
                var
            );
        }
    }

    Ok(IncrementalQueryPlan {
        find_vars,
        where_plan,
    })
}

fn reject_unsupported_or_join(or: &OrJoin) -> Result<()> {
    for branch in &or.clauses {
        reject_unsupported_or_branch(branch)?;
    }
    Ok(())
}

fn reject_unsupported_or_branch(branch: &OrWhereClause) -> Result<()> {
    match branch {
        OrWhereClause::Clause(clause) => reject_unsupported_where_clause(clause),
        OrWhereClause::And(children) => {
            for child in children {
                reject_unsupported_where_clause(child)?;
            }
            Ok(())
        }
    }
}

fn reject_unsupported_pattern_shape(pattern: &Pattern) -> Result<()> {
    if pattern.source.is_some() {
        bail!("Incremental query patterns do not support source variables");
    }
    if !matches!(pattern.tx, PatternNonValuePlace::Placeholder) {
        bail!("Incremental query patterns do not support tx positions");
    }
    if !matches!(
        pattern.attribute,
        PatternNonValuePlace::Ident(_) | PatternNonValuePlace::Entid(_)
    ) {
        bail!("Incremental query pattern attributes must be constant idents or entids");
    }
    Ok(())
}

fn reject_unsupported_where_clause(clause: &WhereClause) -> Result<()> {
    match clause {
        WhereClause::Pattern(pattern) => reject_unsupported_pattern_shape(pattern),
        WhereClause::Pred(_) | WhereClause::WhereFn(_) => Ok(()),
        WhereClause::NotJoin(not) => {
            for clause in &not.clauses {
                reject_unsupported_where_clause(clause)?;
            }
            Ok(())
        }
        WhereClause::OrJoin(or) => reject_unsupported_or_join(or),
        // Rejected by `validate_query` before planning, not supported here.
        WhereClause::TypeAnnotation(_) | WhereClause::RuleExpr => Ok(()),
    }
}

// TODO: Delete this when incremental queries reach standard query parity.
fn reject_unsupported_query_shape(query: &ParsedQuery) -> Result<()> {
    if !query.with.is_empty() {
        bail!("Incremental queries do not support :with");
    }
    if !query.in_bindings.is_empty() || !query.in_sources.is_empty() {
        bail!("Incremental queries do not support :in");
    }
    if query.limit != Limit::None {
        bail!("Incremental queries do not support :limit");
    }
    if query.order.is_some() {
        bail!("Incremental queries do not support :order");
    }
    for clause in &query.where_clauses {
        reject_unsupported_where_clause(clause)?;
    }
    Ok(())
}

fn find_vars(find_spec: &FindSpec) -> Result<Vec<Variable>> {
    let elements = match find_spec {
        FindSpec::FindRel(elements) => elements,
        _ => bail!("Incremental queries support only relational :find"),
    };

    elements
        .iter()
        .map(|element| match element {
            Element::Variable(var) => Ok(var.clone()),
            _ => Err(anyhow!(
                "Incremental queries support only variables in :find"
            )),
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::HashMap;

    use edn::kw;
    use edn::query::ParsedQuery;

    use crate::ops::Entid;
    use crate::schema::{Attribute, Schema, ValueType};

    pub(crate) const NAME_ATTR_ID: Entid = 10;
    pub(crate) const AGE_ATTR_ID: Entid = 11;
    pub(crate) const FOLLOWS_ATTR_ID: Entid = 12;
    pub(crate) const TYPE_ATTR_ID: Entid = 13;
    pub(crate) const STATUS_OPEN_ID: Entid = 20;

    pub(crate) fn parse_query(input: &str) -> ParsedQuery {
        edn::parse::parse_query(input).expect("query should parse")
    }

    pub(crate) fn test_schema() -> Schema {
        let attrs = [
            (kw!(:name), NAME_ATTR_ID, ValueType::String),
            (kw!(:age), AGE_ATTR_ID, ValueType::Long),
            (kw!(:follows), FOLLOWS_ATTR_ID, ValueType::Ref),
            (kw!(:type), TYPE_ATTR_ID, ValueType::Keyword),
        ];
        let mut ident_map = HashMap::new();
        let mut entid_map = HashMap::new();
        let mut attribute_map = HashMap::new();
        for (ident, entid, value_type) in attrs {
            ident_map.insert(ident.clone(), entid);
            entid_map.insert(entid, ident);
            attribute_map.insert(
                entid,
                Attribute {
                    value_type,
                    multival: true,
                    unique: None,
                },
            );
        }
        ident_map.insert(kw!(:status/open), STATUS_OPEN_ID);
        entid_map.insert(STATUS_OPEN_ID, kw!(:status/open));
        Schema {
            entid_map,
            ident_map,
            attribute_map,
        }
    }
}

#[cfg(test)]
mod tests {
    use edn::query::ToVariable;
    use edn::{kw, Keyword};

    use super::test_support::{parse_query, test_schema};
    use super::*;
    use crate::codec::Encode;
    use crate::ops::DataType;

    fn assert_plan_err(query: &str, expected: &str) {
        let schema = test_schema();
        let err = plan_query(&parse_query(query), &schema).unwrap_err();
        assert!(
            err.to_string().contains(expected),
            "expected error containing {:?}, got {:?}",
            expected,
            err
        );
    }

    #[test]
    fn plans_single_fixed_attribute_pattern() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query("[:find ?e ?name :where [?e :name ?name]]"),
            &schema,
        )
        .unwrap();

        assert_eq!(plan.find_vars, vec!["?e".to_var(), "?name".to_var()]);
        let leaf_patterns = plan.leaf_patterns();
        assert_eq!(leaf_patterns.len(), 1);
        assert_eq!(leaf_patterns[0].attribute, 10);
        assert_eq!(
            &leaf_patterns[0].pattern_vars,
            &vec!["?e".to_var(), "?name".to_var()]
        );
        assert_eq!(plan.where_plan.incoming_vars, None);
        let RelPlanKind::Pattern(pattern) = &plan.where_plan.kind else {
            panic!("expected pattern plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(pattern.attribute, 10);
    }

    #[test]
    fn plans_entity_extension_layout() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]"),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Chain { children } = &plan.where_plan.kind else {
            panic!("expected chain plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].incoming_vars, None);
        assert_eq!(
            children[1].incoming_vars,
            Some(vec!["?e".to_var(), "?name".to_var()])
        );
        let RelPlanKind::Pattern(age) = &children[1].kind else {
            panic!("expected extending pattern");
        };
        assert_eq!(age.pattern_vars, vec!["?e".to_var(), "?age".to_var()]);
        assert_eq!(
            children[1].output_vars,
            vec!["?e".to_var(), "?name".to_var(), "?age".to_var()]
        );
    }

    #[test]
    fn plans_ref_value_extension_layout() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(
                "[:find ?friend-name :where [?e :follows ?friend] [?friend :name ?friend-name]]",
            ),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Chain { children } = &plan.where_plan.kind else {
            panic!("expected chain plan, got {:?}", &plan.where_plan.kind);
        };
        let RelPlanKind::Pattern(friend_name) = &children[1].kind else {
            panic!("expected extending pattern");
        };
        assert_eq!(
            friend_name.pattern_vars,
            vec!["?friend".to_var(), "?friend-name".to_var()]
        );
        assert_eq!(
            children[1].output_vars,
            vec!["?e".to_var(), "?friend".to_var(), "?friend-name".to_var()]
        );
    }

    #[test]
    fn plans_three_pattern_chain() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query("[:find ?name ?friend-name ?age :where [?e :name ?name] [?e :follows ?friend] [?friend :name ?friend-name] [?friend :age ?age]]"),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Chain { children } = &plan.where_plan.kind else {
            panic!("expected chain plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(children.len(), 4);
        assert_eq!(
            children
                .iter()
                .map(|child| child.output_vars.clone())
                .collect::<Vec<_>>(),
            vec![
                vec!["?e".to_var(), "?name".to_var()],
                vec!["?e".to_var(), "?name".to_var(), "?friend".to_var()],
                vec![
                    "?e".to_var(),
                    "?name".to_var(),
                    "?friend".to_var(),
                    "?friend-name".to_var()
                ],
                vec![
                    "?e".to_var(),
                    "?name".to_var(),
                    "?friend".to_var(),
                    "?friend-name".to_var(),
                    "?age".to_var()
                ]
            ]
        );
    }

    #[test]
    fn plans_constants() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(r#"[:find ?e :where [?e :name "Alice"] [?other :age 30]]"#),
            &schema,
        )
        .unwrap();

        let leaf_patterns = plan.leaf_patterns();
        assert_eq!(&leaf_patterns[0].value, &encoded_string("Alice"));
        assert_eq!(
            &leaf_patterns[1].entity,
            &PatternSlot::Variable("?other".to_var())
        );
        assert_eq!(&leaf_patterns[1].value, &encoded_long(30));
    }

    #[test]
    fn plans_ref_ident_constants_as_eids() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query("[:find ?e :where [?e :follows :status/open]]"),
            &schema,
        )
        .unwrap();

        assert_eq!(
            &plan.leaf_patterns()[0].value,
            &encoded_long(test_support::STATUS_OPEN_ID)
        );
    }

    #[test]
    fn keeps_ident_constants_as_keywords_for_keyword_attributes() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query("[:find ?e :where [?e :type :status/open]]"),
            &schema,
        )
        .unwrap();

        assert_eq!(
            &plan.leaf_patterns()[0].value,
            &PatternSlot::Constant(DataType::Keyword(kw!(:status/open)).encode())
        );
    }

    #[test]
    fn rejects_unknown_ref_ident_constants() {
        assert_plan_err(
            "[:find ?e :where [?e :follows :status/missing]]",
            "Unknown ident in ref value position: :status/missing",
        );
    }

    #[test]
    fn accepts_disconnected_patterns_as_cartesian_product() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query("[:find ?name ?age :where [?e :name ?name] [?other :age ?age]]"),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Chain { children } = &plan.where_plan.kind else {
            panic!("expected chain plan, got {:?}", &plan.where_plan.kind);
        };
        let RelPlanKind::Pattern(pattern) = &children[1].kind else {
            panic!("expected extending pattern");
        };
        assert_eq!(
            pattern.pattern_vars,
            vec!["?other".to_var(), "?age".to_var()]
        );
        assert_eq!(
            children[1].output_vars,
            vec![
                "?e".to_var(),
                "?name".to_var(),
                "?other".to_var(),
                "?age".to_var()
            ]
        );
    }

    #[test]
    fn prefers_connected_descriptor_over_earlier_disconnected_descriptor() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(
                "[:find ?name ?age ?friend :where
                 [?e :name ?name]
                 [?other :age ?age]
                 [?e :follows ?friend]]",
            ),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Chain { children } = &plan.where_plan.kind else {
            panic!("expected chain plan, got {:?}", &plan.where_plan.kind);
        };
        let attributes = children
            .iter()
            .map(|child| {
                let RelPlanKind::Pattern(pattern) = &child.kind else {
                    panic!("expected pattern child");
                };
                pattern.attribute
            })
            .collect::<Vec<_>>();
        assert_eq!(attributes, vec![10, 12, 11]);
    }

    #[test]
    fn plans_flat_or_clause() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(r#"[:find ?e :where (or [?e :name "Alice"] [?e :name "Bob"])]"#),
            &schema,
        )
        .unwrap();

        assert_eq!(plan.find_vars, vec!["?e".to_var()]);
        let RelPlanKind::Union { branches } = &plan.where_plan.kind else {
            panic!("expected union plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(plan.where_plan.incoming_vars, None);
        assert_eq!(branches.len(), 2);
        assert!(branches.iter().all(|branch| branch.incoming_vars.is_none()));
        assert!(matches!(branches[0].kind, RelPlanKind::Pattern(_)));
        assert!(matches!(branches[1].kind, RelPlanKind::Pattern(_)));
        let leaf_patterns = plan.leaf_patterns();
        assert_eq!(leaf_patterns.len(), 2);
        assert_eq!(leaf_patterns[0].attribute, 10);
        assert_eq!(&leaf_patterns[0].value, &encoded_string("Alice"));
        assert_eq!(leaf_patterns[1].attribute, 10);
        assert_eq!(&leaf_patterns[1].value, &encoded_string("Bob"));
    }

    #[test]
    fn plans_nested_or_clause() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(
                r#"[:find ?e :where (or [?e :name "Alice"] (or [?e :name "Bob"] [?e :name "Cara"]))]"#,
            ),
            &schema,
        )
        .unwrap();

        assert_eq!(plan.leaf_patterns().len(), 3);
        let RelPlanKind::Union { branches } = &plan.where_plan.kind else {
            panic!("expected union plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(plan.where_plan.incoming_vars, None);
        assert_eq!(branches.len(), 2);
        assert!(matches!(branches[0].kind, RelPlanKind::Pattern(_)));
        assert!(matches!(branches[1].kind, RelPlanKind::Union { .. }));
    }

    #[test]
    fn plans_or_clause_with_branch_specific_variable_order() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(r#"[:find ?e ?v :where (or [?e :name ?v] [?v :follows ?e])]"#),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Union { branches } = &plan.where_plan.kind else {
            panic!("expected union plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(plan.where_plan.incoming_vars, None);
        assert_eq!(
            plan.where_plan.output_vars,
            vec!["?e".to_var(), "?v".to_var()]
        );
        assert_eq!(branches[0].output_vars, &["?e".to_var(), "?v".to_var()]);
        assert_eq!(branches[1].output_vars, &["?v".to_var(), "?e".to_var()]);
    }

    #[test]
    fn plans_and_branch_as_chain_with_branch_specific_variable_order() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(
                r#"[:find ?x ?y :where (or (and [?y :name ?x] [?y :follows ?x]) [?x :follows ?y])]"#,
            ),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Union { branches } = &plan.where_plan.kind else {
            panic!("expected union plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(branches.len(), 2);
        let RelPlanKind::Chain {
            children: branch_children,
        } = &branches[0].kind
        else {
            panic!("expected chain branch, got {:?}", &branches[0].kind);
        };
        assert_eq!(branch_children.len(), 2);
        let RelPlanKind::Pattern(second) = &branch_children[1].kind else {
            panic!("expected extending pattern");
        };
        assert_eq!(second.pattern_vars, vec!["?y".to_var(), "?x".to_var()]);
        assert!(matches!(branches[1].kind, RelPlanKind::Pattern(_)));
        assert_eq!(plan.leaf_patterns().len(), 3);
        assert_eq!(
            plan.where_plan.output_vars,
            vec!["?y".to_var(), "?x".to_var()]
        );
        assert_eq!(branches[0].output_vars, &["?y".to_var(), "?x".to_var()]);
        assert_eq!(
            branch_children[1].output_vars,
            vec!["?y".to_var(), "?x".to_var()]
        );
        assert_eq!(branches[1].output_vars, &["?x".to_var(), "?y".to_var()]);
    }

    #[test]
    fn plans_outer_relation_into_every_or_branch() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(
                r#"[:find ?name
                    :where
                    [?e :name ?name]
                    (or [?e :age 30]
                        [?e :age 40])]"#,
            ),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Chain { children } = &plan.where_plan.kind else {
            panic!("expected chain plan, got {:?}", &plan.where_plan.kind);
        };
        let incoming = vec!["?e".to_var(), "?name".to_var()];
        let union_plan = &children[1];
        assert_eq!(union_plan.incoming_vars, Some(incoming.clone()));
        assert_eq!(union_plan.output_vars, incoming);
        let RelPlanKind::Union { branches } = &union_plan.kind else {
            panic!("expected union child");
        };
        for branch in branches {
            assert_eq!(
                branch.incoming_vars,
                Some(vec!["?e".to_var(), "?name".to_var()])
            );
            assert_eq!(branch.output_vars, vec!["?e".to_var(), "?name".to_var()]);
            assert!(matches!(branch.kind, RelPlanKind::Pattern(_)));
        }
    }

    #[test]
    fn threads_outer_relation_through_and_branch() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(
                r#"[:find ?name
                    :where
                    [?e :name ?name]
                    (or
                      (and [?e :follows ?friend] [?friend :age ?age])
                      (and [?e :follows ?friend] [?friend :age ?age]))]"#,
            ),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Chain { children } = &plan.where_plan.kind else {
            panic!("expected top-level chain");
        };
        let RelPlanKind::Union { branches } = &children[1].kind else {
            panic!("expected union child");
        };
        let branch_plan = &branches[0];
        let RelPlanKind::Chain {
            children: branch_children,
        } = &branch_plan.kind
        else {
            panic!("expected and branch chain");
        };
        assert_eq!(
            branch_plan.incoming_vars,
            Some(vec!["?e".to_var(), "?name".to_var()])
        );
        assert_eq!(branch_children.len(), 2);
        assert_eq!(
            branch_children[0].incoming_vars,
            Some(vec!["?e".to_var(), "?name".to_var()])
        );
        assert_eq!(
            branch_children[1].incoming_vars,
            Some(vec!["?e".to_var(), "?name".to_var(), "?friend".to_var()])
        );
        assert_eq!(
            branch_plan.output_vars,
            vec![
                "?e".to_var(),
                "?name".to_var(),
                "?friend".to_var(),
                "?age".to_var()
            ]
        );
    }

    #[test]
    fn preserves_incoming_layout_through_nested_or() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(
                r#"[:find ?name
                    :where
                    [?e :name ?name]
                    (or
                      (or [?e :age 30] [?e :age 40])
                      [?e :age 50])]"#,
            ),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Chain { children } = &plan.where_plan.kind else {
            panic!("expected top-level chain");
        };
        let RelPlanKind::Union {
            branches: outer_branches,
        } = &children[1].kind
        else {
            panic!("expected outer union");
        };
        let RelPlanKind::Union {
            branches: inner_branches,
        } = &outer_branches[0].kind
        else {
            panic!("expected inner union");
        };
        let incoming = Some(vec!["?e".to_var(), "?name".to_var()]);
        assert_eq!(children[1].incoming_vars, incoming);
        assert_eq!(outer_branches[0].incoming_vars, incoming);
        assert!(inner_branches
            .iter()
            .all(|branch| branch.incoming_vars == incoming));
    }

    #[test]
    fn plans_not_after_its_variables_are_grounded() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query("[:find ?name :where (not [?e :age 30]) [?e :name ?name]]"),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Chain { children } = &plan.where_plan.kind else {
            panic!("expected chain plan");
        };
        assert!(matches!(children[0].kind, RelPlanKind::Pattern(_)));
        let difference = &children[1];
        assert_eq!(
            difference.incoming_vars,
            Some(vec!["?e".to_var(), "?name".to_var()])
        );
        assert_eq!(
            difference.output_vars,
            vec!["?e".to_var(), "?name".to_var()]
        );
        let RelPlanKind::Difference { key_vars, negative } = &difference.kind else {
            panic!("expected difference plan");
        };
        assert_eq!(key_vars, &vec!["?e".to_var()]);
        assert_eq!(negative.incoming_vars, Some(vec!["?e".to_var()]));
        assert_eq!(negative.output_vars, vec!["?e".to_var()]);
    }

    #[test]
    fn plans_not_with_a_non_leading_key() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query("[:find ?name :where [?name :follows ?e] (not [?e :age 30])]"),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Chain { children } = &plan.where_plan.kind else {
            panic!("expected chain plan");
        };
        let RelPlanKind::Difference { key_vars, .. } = &children[1].kind else {
            panic!("expected difference plan");
        };
        assert_eq!(
            children[1].incoming_vars,
            Some(vec!["?name".to_var(), "?e".to_var()])
        );
        assert_eq!(key_vars, &vec!["?e".to_var()]);
    }

    #[test]
    fn plans_double_not_from_the_projected_outer_relation() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query("[:find ?e :where [?e :name ?name] (not (not [?e :age 30]))]"),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Chain { children } = &plan.where_plan.kind else {
            panic!("expected chain plan");
        };
        let RelPlanKind::Difference {
            key_vars: outer_key_vars,
            negative: outer_negative,
        } = &children[1].kind
        else {
            panic!("expected outer difference");
        };
        let RelPlanKind::Difference {
            key_vars: inner_key_vars,
            negative: inner_negative,
        } = &outer_negative.kind
        else {
            panic!("expected inner difference");
        };
        assert_eq!(outer_key_vars, &vec!["?e".to_var()]);
        assert_eq!(outer_negative.incoming_vars, Some(vec!["?e".to_var()]));
        assert_eq!(inner_key_vars, &vec!["?e".to_var()]);
        assert_eq!(inner_negative.incoming_vars, Some(vec!["?e".to_var()]));
    }

    #[test]
    fn plans_not_inside_an_and_or_branch() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(
                r#"[:find ?e
                    :where
                    (or
                      (and [?e :name "Alice"] (not [?e :age 30]))
                      [?e :name "Bob"])]"#,
            ),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Union { branches } = &plan.where_plan.kind else {
            panic!("expected union plan");
        };
        let RelPlanKind::Chain {
            children: branch_children,
        } = &branches[0].kind
        else {
            panic!("expected and branch");
        };
        assert!(matches!(
            branch_children[1].kind,
            RelPlanKind::Difference { .. }
        ));
    }

    #[test]
    fn plans_predicates_as_bound_filter_leaves() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(
                "[:find ?age
                  :where
                  [(< ?age 50)]
                  [?e :age ?age]
                  [?e :name ?name]]",
            ),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Chain { children } = &plan.where_plan.kind else {
            panic!("expected chain plan");
        };
        assert!(matches!(children[0].kind, RelPlanKind::Pattern(_)));
        let filter = &children[1];
        assert_eq!(
            filter.incoming_vars,
            Some(vec!["?e".to_var(), "?age".to_var()])
        );
        assert_eq!(filter.output_vars, vec!["?e".to_var(), "?age".to_var()]);
        assert!(matches!(filter.kind, RelPlanKind::Filter { .. }));
        assert!(matches!(children[2].kind, RelPlanKind::Pattern(_)));

        let plan = plan_query(
            &parse_query(
                "[:find ?age
                  :where
                  [?e :age ?age]
                  (or
                    (and [(> ?age 10)]
                         (not [(< ?age 30)]))
                    [(= ?age 42)])]",
            ),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Chain { children } = &plan.where_plan.kind else {
            panic!("expected top-level chain");
        };
        let RelPlanKind::Union { branches } = &children[1].kind else {
            panic!("expected union");
        };
        let RelPlanKind::Chain {
            children: branch_children,
        } = &branches[0].kind
        else {
            panic!("expected and branch");
        };
        assert!(matches!(
            branch_children[0].kind,
            RelPlanKind::Filter { .. }
        ));
        let RelPlanKind::Difference { negative, .. } = &branch_children[1].kind else {
            panic!("expected difference");
        };
        assert!(matches!(negative.kind, RelPlanKind::Filter { .. }));
        assert!(matches!(branches[1].kind, RelPlanKind::Filter { .. }));
    }

    #[test]
    fn plans_function_applications_after_their_inputs_are_bound() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(
                "[:find ?quarter-age
                  :where
                  [(quot ?age 2) ?half-age]
                  [(quot ?half-age 2) ?quarter-age]
                  [?e :age ?age]]",
            ),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Chain { children } = &plan.where_plan.kind else {
            panic!("expected chain plan");
        };
        assert!(matches!(children[0].kind, RelPlanKind::Pattern(_)));
        let RelPlanKind::Function { output_var, .. } = &children[1].kind else {
            panic!("expected first function plan");
        };
        assert_eq!(output_var, &"?half-age".to_var());
        assert_eq!(
            children[1].output_vars,
            vec!["?e".to_var(), "?age".to_var(), "?half-age".to_var()]
        );
        let RelPlanKind::Function { output_var, .. } = &children[2].kind else {
            panic!("expected second function plan");
        };
        assert_eq!(output_var, &"?quarter-age".to_var());
        assert_eq!(
            children[2].output_vars,
            vec![
                "?e".to_var(),
                "?age".to_var(),
                "?half-age".to_var(),
                "?quarter-age".to_var()
            ]
        );
    }

    #[test]
    fn plans_bound_function_result_without_extending_layout() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(
                "[:find ?name
                  :where
                  [?e :age ?age]
                  [?e :name ?name]
                  [(str ?age) ?name]]",
            ),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Chain { children } = &plan.where_plan.kind else {
            panic!("expected chain plan");
        };
        let function = &children[2];
        assert_eq!(
            function.incoming_vars,
            Some(vec!["?e".to_var(), "?age".to_var(), "?name".to_var()])
        );
        assert_eq!(
            function.output_vars,
            vec!["?e".to_var(), "?age".to_var(), "?name".to_var()]
        );
        let RelPlanKind::Function { output_var, .. } = &function.kind else {
            panic!("expected function plan");
        };
        assert_eq!(output_var, &"?name".to_var());
    }

    #[test]
    fn rejects_function_without_a_positive_relation() {
        assert_plan_err(
            "[:find ?result :where [(+ 1 2) ?result]]",
            "Cannot plan function without a positive relation",
        );
    }

    #[test]
    fn accepts_entid_attribute() {
        let schema = test_schema();
        let plan = plan_query(&parse_query("[:find ?e :where [?e 10 ?name]]"), &schema).unwrap();

        assert_eq!(plan.leaf_patterns()[0].attribute, 10);
    }

    #[test]
    fn rejects_variable_attribute() {
        assert_plan_err(
            "[:find ?e :where [?e ?attr ?value]]",
            "attributes must be constant",
        );
    }

    #[test]
    fn rejects_placeholder_attribute() {
        assert_plan_err(
            "[:find ?e :where [?e _ ?value]]",
            "attributes must be constant",
        );
    }

    #[test]
    fn rejects_entity_placeholder() {
        assert_plan_err(
            "[:find ?name :where [_ :name ?name]]",
            "Placeholders in entity position",
        );
    }

    #[test]
    fn rejects_value_placeholder() {
        assert_plan_err(
            "[:find ?e :where [?e :name _]]",
            "Placeholders in value position",
        );
    }

    #[test]
    fn rejects_repeated_variable_pattern() {
        assert_plan_err(
            "{:find [?x] :where [[?x :follows ?x]]}",
            "Repeated variable ?x in a single pattern is not supported",
        );
    }

    #[test]
    fn rejects_rule_expressions() {
        let schema = test_schema();
        let mut query = parse_query("[:find ?e :where [?e :age ?age]]");
        query.where_clauses.push(WhereClause::RuleExpr);

        let err = plan_query(&query, &schema).unwrap_err();
        assert!(
            err.to_string()
                .contains("Queries do not support rule expressions"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn rejects_non_relational_find() {
        assert_plan_err(
            "[:find [?e ...] :where [?e :name ?name]]",
            "relational :find",
        );
        assert_plan_err(
            "[:find (count ?e) :where [?e :name ?name]]",
            "variables in :find",
        );
        assert_plan_err(
            "[:find (pull ?e [*]) :where [?e :name ?name]]",
            "variables in :find",
        );
    }

    #[test]
    fn rejects_order_limit_and_in() {
        assert_plan_err(
            "[:find ?name :where [?e :name ?name] :order [?name :asc]]",
            ":order",
        );
        assert_plan_err("[:find ?e :where [?e :name ?name] :limit 10]", ":limit");
        assert_plan_err("[:find ?e :in ?name :where [?e :name ?name]]", ":in");
    }

    #[test]
    fn rejects_unbound_find_variable() {
        assert_plan_err(
            "[:find ?missing :where [?e :name ?name]]",
            "Find variable ?missing",
        );
    }

    #[test]
    fn rejects_unknown_ident_attribute() {
        assert_plan_err(
            "[:find ?e :where [?e :unknown ?value]]",
            "Unknown attribute",
        );
    }

    fn encoded_string(value: &str) -> PatternSlot {
        PatternSlot::Constant(DataType::String(value.to_string()).encode())
    }

    fn encoded_long(value: i64) -> PatternSlot {
        PatternSlot::Constant(DataType::Long(value).encode())
    }

    #[allow(dead_code)]
    fn keyword(namespace: &str, name: &str) -> Keyword {
        Keyword::namespaced(namespace, name)
    }
}

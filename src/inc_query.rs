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
pub(crate) use planner::{JoinPlan, JoinStep, PatternPlan, RelPlan, RelPlanKind, UnionPlan};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IncrementalQueryPlan {
    pub find_vars: Vec<Variable>,
    pub variables: Vec<Variable>,
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
    let where_plan = planner::plan_scope(&descriptors)?;
    let variables = where_plan.output_vars.clone();
    for var in &find_vars {
        if !variables.contains(var) {
            bail!(
                "Find variable {} is not bound by incremental query patterns",
                var
            );
        }
    }

    Ok(IncrementalQueryPlan {
        find_vars,
        variables,
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
        WhereClause::OrJoin(or) => reject_unsupported_or_join(or),
        _ => bail!(
            "Incremental queries currently support only triple patterns, `or` clauses, and `and` clauses in :where"
        ),
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
mod tests {
    use std::collections::HashMap;

    use edn::query::ToVariable;
    use edn::{kw, Keyword};

    use super::*;
    use crate::codec::Encode;
    use crate::ops::DataType;
    use crate::schema::{Attribute, ValueType};

    fn parse_query(input: &str) -> ParsedQuery {
        edn::parse::parse_query(input).expect("query should parse")
    }

    fn test_schema() -> Schema {
        let attrs = [
            (kw!(:name), 10, ValueType::String),
            (kw!(:age), 11, ValueType::Long),
            (kw!(:follows), 12, ValueType::Ref),
            (kw!(:type), 13, ValueType::Keyword),
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
        Schema {
            entid_map,
            ident_map,
            attribute_map,
        }
    }

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
        assert_eq!(plan.variables, vec!["?e".to_var(), "?name".to_var()]);
        let leaf_patterns = plan.leaf_patterns();
        assert_eq!(leaf_patterns.len(), 1);
        assert_eq!(leaf_patterns[0].attribute, 10);
        assert_eq!(
            &leaf_patterns[0].output_vars,
            &vec!["?e".to_var(), "?name".to_var()]
        );
        let RelPlanKind::Pattern(pattern) = &plan.where_plan.kind else {
            panic!("expected pattern plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(pattern.attribute, 10);
    }

    #[test]
    fn plans_entity_join() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]"),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Join(join) = &plan.where_plan.kind else {
            panic!("expected join plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(join.inputs.len(), 2);
        assert_eq!(join.steps.len(), 1);
        assert_eq!(join.steps[0].right_input_index, 1);
        assert_eq!(join.steps[0].key_vars, vec!["?e".to_var()]);
        assert_eq!(
            join.steps[0].output_vars,
            vec!["?e".to_var(), "?name".to_var(), "?age".to_var()]
        );
    }

    #[test]
    fn plans_ref_value_join() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(
                "[:find ?friend-name :where [?e :follows ?friend] [?friend :name ?friend-name]]",
            ),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Join(join) = &plan.where_plan.kind else {
            panic!("expected join plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(join.steps[0].right_input_index, 1);
        assert_eq!(join.steps[0].key_vars, vec!["?friend".to_var()]);
    }

    #[test]
    fn plans_three_pattern_chain() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query("[:find ?name ?friend-name ?age :where [?e :name ?name] [?e :follows ?friend] [?friend :name ?friend-name] [?friend :age ?age]]"),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Join(join) = &plan.where_plan.kind else {
            panic!("expected join plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(join.steps.len(), 3);
        assert_eq!(join.steps[0].key_vars, vec!["?e".to_var()]);
        assert_eq!(join.steps[1].key_vars, vec!["?friend".to_var()]);
        assert_eq!(join.steps[2].key_vars, vec!["?friend".to_var()]);
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
    fn accepts_disconnected_patterns_as_cartesian_product() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query("[:find ?name ?age :where [?e :name ?name] [?other :age ?age]]"),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Join(join) = &plan.where_plan.kind else {
            panic!("expected join plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(join.steps.len(), 1);
        assert_eq!(join.steps[0].right_input_index, 1);
        assert!(join.steps[0].key_vars.is_empty());
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
        assert_eq!(plan.variables, vec!["?e".to_var()]);
        let RelPlanKind::Union(union) = &plan.where_plan.kind else {
            panic!("expected union plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(union.branches.len(), 2);
        assert!(matches!(union.branches[0].kind, RelPlanKind::Pattern(_)));
        assert!(matches!(union.branches[1].kind, RelPlanKind::Pattern(_)));
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

        assert_eq!(plan.variables, vec!["?e".to_var()]);
        assert_eq!(plan.leaf_patterns().len(), 3);
        let RelPlanKind::Union(union) = &plan.where_plan.kind else {
            panic!("expected union plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(union.branches.len(), 2);
        assert!(matches!(union.branches[0].kind, RelPlanKind::Pattern(_)));
        assert!(matches!(union.branches[1].kind, RelPlanKind::Union(_)));
    }

    #[test]
    fn plans_or_clause_with_branch_specific_variable_order() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(r#"[:find ?e ?v :where (or [?e :name ?v] [?v :follows ?e])]"#),
            &schema,
        )
        .unwrap();

        assert_eq!(plan.variables, vec!["?e".to_var(), "?v".to_var()]);
        let RelPlanKind::Union(or) = &plan.where_plan.kind else {
            panic!("expected union plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(
            plan.where_plan.output_vars,
            vec!["?e".to_var(), "?v".to_var()]
        );
        assert_eq!(or.branches[0].output_vars, &["?e".to_var(), "?v".to_var()]);
        assert_eq!(or.branches[1].output_vars, &["?v".to_var(), "?e".to_var()]);
    }

    #[test]
    fn plans_and_branch_inside_or_as_join() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(
                r#"[:find ?e :where (or (and [?e :name "Alice"] [?e :age 30]) [?e :name "Bob"])]"#,
            ),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Union(or) = &plan.where_plan.kind else {
            panic!("expected union plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(or.branches.len(), 2);
        let RelPlanKind::Join(and_branch) = &or.branches[0].kind else {
            panic!("expected join branch, got {:?}", &or.branches[0].kind);
        };
        assert_eq!(and_branch.inputs.len(), 2);
        assert_eq!(and_branch.steps.len(), 1);
        assert_eq!(and_branch.steps[0].key_vars, vec!["?e".to_var()]);
        assert!(matches!(or.branches[1].kind, RelPlanKind::Pattern(_)));
        assert_eq!(plan.leaf_patterns().len(), 3);
    }

    #[test]
    fn plans_and_branch_inside_or_with_branch_specific_variable_order() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(
                r#"[:find ?x ?y :where (or (and [?y :name ?x] [?y :follows ?x]) [?x :follows ?y])]"#,
            ),
            &schema,
        )
        .unwrap();

        let RelPlanKind::Union(or) = &plan.where_plan.kind else {
            panic!("expected union plan, got {:?}", &plan.where_plan.kind);
        };
        assert_eq!(
            plan.where_plan.output_vars,
            vec!["?y".to_var(), "?x".to_var()]
        );
        assert_eq!(or.branches[0].output_vars, &["?y".to_var(), "?x".to_var()]);
        assert_eq!(or.branches[1].output_vars, &["?x".to_var(), "?y".to_var()]);
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
    fn rejects_unsupported_where_forms() {
        assert_plan_err(
            r#"[:find ?e :where [?e :name "Alice"] [(< 1 2)]]"#,
            "only triple patterns, `or` clauses, and `and` clauses",
        );
        assert_plan_err(
            r#"[:find ?e :where [?e :name "Alice"] (not [?e :age 30])]"#,
            "only triple patterns, `or` clauses, and `and` clauses",
        );
        assert_plan_err(
            r#"[:find ?e :where (or-join [?e] [?e :name "Alice"] [?e :name "Bob"])]"#,
            "explicit or-join",
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

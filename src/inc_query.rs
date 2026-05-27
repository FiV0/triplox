//! Incremental-query planning helpers.

use std::collections::HashSet;

use anyhow::{anyhow, bail, Result};
use edn::query::{
    Element, FindSpec, Limit, ParsedQuery, Pattern, PatternNonValuePlace, PatternValuePlace,
    Variable, WhereClause,
};

use crate::codec::Encode;
use crate::incremental::EncodedValue;
use crate::ops::DataType;
use crate::query::non_integer_constant_to_datatype;
use crate::schema::Schema;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IncrementalQueryPlan {
    pub find_vars: Vec<Variable>,
    pub variables: Vec<Variable>,
    pub patterns: Vec<PatternPlan>,
    pub joins: Vec<JoinPlan>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PatternPlan {
    pub attribute: i64,
    pub entity: PatternSlot,
    pub value: PatternSlot,
    pub output_vars: Vec<Variable>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PatternSlot {
    Variable(Variable),
    Constant(EncodedValue),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JoinPlan {
    pub kind: JoinKind,
    pub left_vars: Vec<Variable>,
    pub right_vars: Vec<Variable>,
    pub key_vars: Vec<Variable>,
    pub output_vars: Vec<Variable>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum JoinKind {
    Keyed,
    Cartesian,
}

pub(crate) fn plan_query(query: &ParsedQuery, schema: &Schema) -> Result<IncrementalQueryPlan> {
    reject_unsupported_query_shape(query)?;

    let find_vars = find_vars(&query.find_spec)?;
    let patterns = query
        .where_clauses
        .iter()
        .map(|clause| match clause {
            WhereClause::Pattern(pattern) => plan_pattern(pattern, schema),
            _ => unreachable!("non-pattern clauses are rejected before planning"),
        })
        .collect::<Result<Vec<_>>>()?;

    if patterns.is_empty() {
        bail!("Incremental queries require at least one triple pattern");
    }

    let variables = collect_variables(&patterns);
    for var in &find_vars {
        if !variables.contains(var) {
            bail!(
                "Find variable {} is not bound by incremental query patterns",
                var
            );
        }
    }

    let joins = plan_joins(&patterns);

    Ok(IncrementalQueryPlan {
        find_vars,
        variables,
        patterns,
        joins,
    })
}

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
        match clause {
            WhereClause::Pattern(pattern) => reject_unsupported_pattern_shape(pattern)?,
            _ => bail!("Incremental queries currently support only triple patterns in :where"),
        }
    }
    Ok(())
}

fn reject_unsupported_pattern_shape(pattern: &Pattern) -> Result<()> {
    if matches!(pattern.entity, PatternNonValuePlace::Placeholder) {
        bail!("Incremental query patterns do not support placeholders in entity position");
    }
    if matches!(pattern.value, PatternValuePlace::Placeholder) {
        bail!("Incremental query patterns do not support placeholders in value position");
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

fn plan_pattern(pattern: &Pattern, schema: &Schema) -> Result<PatternPlan> {
    if pattern.source.is_some() {
        bail!("Incremental query patterns do not support source variables");
    }
    if !matches!(pattern.tx, PatternNonValuePlace::Placeholder) {
        bail!("Incremental query patterns do not support tx positions");
    }

    let attribute = match &pattern.attribute {
        PatternNonValuePlace::Ident(ident) => {
            schema
                .get_attribute(ident.as_ref())
                .ok_or_else(|| anyhow!("Unknown attribute: {}", ident))?
                .0
        }
        PatternNonValuePlace::Entid(entid) => *entid,
        PatternNonValuePlace::Variable(_) | PatternNonValuePlace::Placeholder => {
            bail!("Incremental query pattern attributes must be constant idents or entids")
        }
    };

    let entity = non_value_slot(&pattern.entity)?;
    let value = value_slot(&pattern.value)?;
    let output_vars = pattern_output_vars(&entity, &value)?;

    Ok(PatternPlan {
        attribute,
        entity,
        value,
        output_vars,
    })
}

fn non_value_slot(place: &PatternNonValuePlace) -> Result<PatternSlot> {
    match place {
        PatternNonValuePlace::Variable(var) => Ok(PatternSlot::Variable(var.clone())),
        PatternNonValuePlace::Entid(entid) => {
            Ok(PatternSlot::Constant(DataType::Long(*entid).encode()))
        }
        // TODO This needs proper ident resolution for refs
        PatternNonValuePlace::Ident(ident) => Ok(PatternSlot::Constant(
            DataType::Keyword(ident.as_ref().clone()).encode(),
        )),
        PatternNonValuePlace::Placeholder => {
            bail!("Incremental query patterns do not support placeholders in entity position")
        }
    }
}

fn value_slot(place: &PatternValuePlace) -> Result<PatternSlot> {
    match place {
        PatternValuePlace::Variable(var) => Ok(PatternSlot::Variable(var.clone())),
        PatternValuePlace::EntidOrInteger(value) => {
            Ok(PatternSlot::Constant(DataType::Long(*value).encode()))
        }
        // TODO This needs proper ident resolution for refs
        PatternValuePlace::IdentOrKeyword(ident) => Ok(PatternSlot::Constant(
            DataType::Keyword(ident.as_ref().clone()).encode(),
        )),
        PatternValuePlace::Constant(constant) => Ok(PatternSlot::Constant(
            non_integer_constant_to_datatype(constant)
                .ok_or_else(|| anyhow!("BigInteger constant is outside Triplox i128 range"))?
                .encode(),
        )),
        PatternValuePlace::Placeholder => {
            bail!("Incremental query patterns do not support placeholders in value position")
        }
    }
}

fn pattern_output_vars(entity: &PatternSlot, value: &PatternSlot) -> Result<Vec<Variable>> {
    let mut vars = Vec::new();
    if let PatternSlot::Variable(var) = entity {
        vars.push(var.clone());
    }
    if let PatternSlot::Variable(var) = value {
        if vars.contains(var) {
            // TODO move this to some common validation step for one-shot + inc queries
            bail!(
                "Repeated variable {} inside one triple pattern is not supported",
                var
            );
        }
        vars.push(var.clone());
    }
    Ok(vars)
}

fn collect_variables(patterns: &[PatternPlan]) -> Vec<Variable> {
    let mut variables = Vec::new();
    for pattern in patterns {
        for var in &pattern.output_vars {
            if !variables.contains(var) {
                variables.push(var.clone());
            }
        }
    }
    variables
}

fn plan_joins(patterns: &[PatternPlan]) -> Vec<JoinPlan> {
    let mut joins = Vec::new();
    let mut left_vars = patterns[0].output_vars.clone();
    let mut bound: HashSet<Variable> = left_vars.iter().cloned().collect();

    for pattern in patterns.iter().skip(1) {
        let right_vars = pattern.output_vars.clone();
        let right_set: HashSet<Variable> = right_vars.iter().cloned().collect();
        let key_vars = left_vars
            .iter()
            .filter(|var| right_set.contains(*var))
            .cloned()
            .collect::<Vec<_>>();
        let kind = if key_vars.is_empty() {
            JoinKind::Cartesian
        } else {
            JoinKind::Keyed
        };

        let mut output_vars = left_vars.clone();
        for var in &right_vars {
            if bound.insert(var.clone()) {
                output_vars.push(var.clone());
            }
        }

        joins.push(JoinPlan {
            kind,
            left_vars,
            right_vars,
            key_vars,
            output_vars: output_vars.clone(),
        });
        left_vars = output_vars;
    }

    joins
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use edn::query::ToVariable;
    use edn::{kw, Keyword};

    use super::*;
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
        assert_eq!(plan.patterns.len(), 1);
        assert_eq!(plan.patterns[0].attribute, 10);
        assert_eq!(
            plan.patterns[0].output_vars,
            vec!["?e".to_var(), "?name".to_var()]
        );
        assert!(plan.joins.is_empty());
    }

    #[test]
    fn plans_entity_join() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]"),
            &schema,
        )
        .unwrap();

        assert_eq!(plan.joins.len(), 1);
        assert_eq!(plan.joins[0].kind, JoinKind::Keyed);
        assert_eq!(plan.joins[0].key_vars, vec!["?e".to_var()]);
        assert_eq!(
            plan.joins[0].output_vars,
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

        assert_eq!(plan.joins[0].kind, JoinKind::Keyed);
        assert_eq!(plan.joins[0].key_vars, vec!["?friend".to_var()]);
    }

    #[test]
    fn plans_three_pattern_chain() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query("[:find ?name ?friend-name ?age :where [?e :name ?name] [?e :follows ?friend] [?friend :name ?friend-name] [?friend :age ?age]]"),
            &schema,
        )
        .unwrap();

        assert_eq!(plan.joins.len(), 3);
        assert_eq!(plan.joins[0].key_vars, vec!["?e".to_var()]);
        assert_eq!(plan.joins[1].key_vars, vec!["?friend".to_var()]);
        assert_eq!(plan.joins[2].key_vars, vec!["?friend".to_var()]);
    }

    #[test]
    fn plans_constants() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query(r#"[:find ?e :where [?e :name "Alice"] [?other :age 30]]"#),
            &schema,
        )
        .unwrap();

        assert_eq!(plan.patterns[0].value, encoded_string("Alice"));
        assert_eq!(
            plan.patterns[1].entity,
            PatternSlot::Variable("?other".to_var())
        );
        assert_eq!(plan.patterns[1].value, encoded_long(30));
    }

    #[test]
    fn accepts_disconnected_patterns_as_cartesian_product() {
        let schema = test_schema();
        let plan = plan_query(
            &parse_query("[:find ?name ?age :where [?e :name ?name] [?other :age ?age]]"),
            &schema,
        )
        .unwrap();

        assert_eq!(plan.joins.len(), 1);
        assert_eq!(plan.joins[0].kind, JoinKind::Cartesian);
        assert!(plan.joins[0].key_vars.is_empty());
    }

    #[test]
    fn accepts_entid_attribute() {
        let schema = test_schema();
        let plan = plan_query(&parse_query("[:find ?e :where [?e 10 ?name]]"), &schema).unwrap();

        assert_eq!(plan.patterns[0].attribute, 10);
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
            "placeholders in entity position",
        );
    }

    #[test]
    fn rejects_value_placeholder() {
        assert_plan_err(
            "[:find ?e :where [?e :name _]]",
            "placeholders in value position",
        );
    }

    #[test]
    fn rejects_repeated_variable_pattern() {
        assert_plan_err(
            "{:find [?x] :where [[?x :follows ?x]]}",
            "Repeated variable ?x",
        );
    }

    #[test]
    fn rejects_unsupported_where_forms() {
        assert_plan_err(
            r#"[:find ?e :where [?e :name "Alice"] [(< 1 2)]]"#,
            "only triple patterns",
        );
        assert_plan_err(
            r#"[:find ?e :where (or [?e :name "Alice"] [?e :name "Bob"])]"#,
            "only triple patterns",
        );
        assert_plan_err(
            r#"[:find ?e :where [?e :name "Alice"] (not [?e :age 30])]"#,
            "only triple patterns",
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
            "[:find ?e :where [?e :name ?name] :order [?name :asc]]",
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

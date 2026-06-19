//! Incremental-query planning helpers.

use std::collections::HashSet;

use anyhow::{anyhow, bail, Result};
use edn::query::{
    Element, FindSpec, Limit, OrJoin, OrWhereClause, ParsedQuery, Pattern, PatternNonValuePlace,
    PatternValuePlace, UnifyVars, Variable, WhereClause,
};

use crate::codec::Encode;
use crate::incremental::EncodedValue;
use crate::ops::DataType;
use crate::query::non_integer_constant_to_datatype;
use crate::query_validation::validate_query;
use crate::schema::Schema;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IncrementalQueryPlan {
    pub find_vars: Vec<Variable>,
    pub variables: Vec<Variable>,
    pub where_terms: Vec<WhereTermPlan>,
    pub joins: Vec<JoinPlan>,
}

impl IncrementalQueryPlan {
    pub(crate) fn leaf_patterns(&self) -> Vec<&PatternPlan> {
        let mut patterns = Vec::new();
        collect_leaf_patterns(&self.where_terms, &mut patterns);
        patterns
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WhereTermPlan {
    Pattern(PatternPlan),
    Or(OrPlan),
}

impl WhereTermPlan {
    pub(crate) fn output_vars(&self) -> &[Variable] {
        match self {
            WhereTermPlan::Pattern(pattern) => &pattern.output_vars,
            WhereTermPlan::Or(or) => &or.output_vars,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PatternSlot {
    Variable(Variable),
    Constant(EncodedValue),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PatternPlan {
    pub attribute: i64,
    pub entity: PatternSlot,
    pub value: PatternSlot,
    pub output_vars: Vec<Variable>,
}


#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrPlan {
    pub branches: Vec<WhereTermPlan>,
    pub output_vars: Vec<Variable>,
}


#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JoinPlan {
    pub right_term_index: usize,
    pub left_vars: Vec<Variable>,
    pub right_vars: Vec<Variable>,
    pub key_vars: Vec<Variable>,
    pub output_vars: Vec<Variable>,
}

pub(crate) fn plan_query(query: &ParsedQuery, schema: &Schema) -> Result<IncrementalQueryPlan> {
    reject_unsupported_query_shape(query)?;
    let find_vars = find_vars(&query.find_spec)?;
    validate_query(query, &[])?;

    let where_terms = query
        .where_clauses
        .iter()
        .map(|clause| plan_where_clause(clause, schema))
        .collect::<Result<Vec<_>>>()?;

    if where_terms.is_empty() {
        bail!("Incremental queries require at least one triple pattern");
    }

    let variables = collect_variables(&where_terms);
    for var in &find_vars {
        if !variables.contains(var) {
            bail!(
                "Find variable {} is not bound by incremental query patterns",
                var
            );
        }
    }

    let joins = plan_joins(&where_terms);

    Ok(IncrementalQueryPlan {
        find_vars,
        variables,
        where_terms,
        joins,
    })
}


fn reject_unsupported_or_join(or: &OrJoin) -> Result<()> {
    if !matches!(&or.unify_vars, UnifyVars::Implicit) {
        bail!("Incremental queries do not support explicit or-join");
    }
    for branch in &or.clauses {
        reject_unsupported_or_branch(branch)?;
    }
    Ok(())
}

fn reject_unsupported_or_branch(branch: &OrWhereClause) -> Result<()> {
    match branch {
        OrWhereClause::Clause(clause) => {
            reject_unsupported_where_clause(clause)
        }
        OrWhereClause::And(_) => {
            bail!("Incremental querys currently do not support `and` patterns!")
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
            "Incremental queries currently support only triple patterns and or clauses in :where"
        ),
    }
}

// TODO: Delete this when incremental queries reach one-shot query parity.
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

fn plan_where_clause(clause: &WhereClause, schema: &Schema) -> Result<WhereTermPlan> {
    match clause {
        WhereClause::Pattern(pattern) => Ok(WhereTermPlan::Pattern(plan_pattern(pattern, schema)?)),
        WhereClause::OrJoin(or) => plan_or_join(or, schema),
        _ => unreachable!("unsupported clauses are rejected before planning"),
    }
}

fn plan_or_join(or: &OrJoin, schema: &Schema) -> Result<WhereTermPlan> {
    if !matches!(&or.unify_vars, UnifyVars::Implicit) {
        bail!("Incremental queries do not support explicit or-join");
    }

    let branches = or
        .clauses
        .iter()
        .map(|branch| plan_or_branch(branch, schema))
        .collect::<Result<Vec<_>>>()?;
    let output_vars = branches
        .first()
        .map(|branch| branch.output_vars().to_vec())
        .ok_or_else(|| anyhow!("OR clause must have at least one branch"))?;

    Ok(WhereTermPlan::Or(OrPlan {
        branches,
        output_vars,
    }))
}

fn plan_or_branch(branch: &OrWhereClause, schema: &Schema) -> Result<WhereTermPlan> {
    match branch {
        OrWhereClause::Clause(WhereClause::Pattern(pattern)) => {
            Ok(WhereTermPlan::Pattern(plan_pattern(pattern, schema)?))
        }
        OrWhereClause::Clause(WhereClause::OrJoin(or)) => plan_or_join(or, schema),
        OrWhereClause::Clause(_) | OrWhereClause::And(_) => {
            bail!("Incremental query or branches currently support only triple patterns or nested or clauses")
        }
    }
}

fn plan_pattern(pattern: &Pattern, schema: &Schema) -> Result<PatternPlan> {
    let attribute = match &pattern.attribute {
        PatternNonValuePlace::Ident(ident) => {
            schema
                .get_attribute(ident.as_ref())
                .ok_or_else(|| anyhow!("Unknown attribute: {}", ident))?
                .0
        }
        PatternNonValuePlace::Entid(entid) => *entid,
        PatternNonValuePlace::Variable(_) | PatternNonValuePlace::Placeholder => {
            unreachable!("variable and placeholder attributes are rejected before planning")
        }
    };

    let entity = non_value_slot(&pattern.entity);
    let value = value_slot(&pattern.value)?;
    let output_vars = pattern_output_vars(&entity, &value);

    Ok(PatternPlan {
        attribute,
        entity,
        value,
        output_vars,
    })
}

fn non_value_slot(place: &PatternNonValuePlace) -> PatternSlot {
    match place {
        PatternNonValuePlace::Variable(var) => PatternSlot::Variable(var.clone()),
        PatternNonValuePlace::Entid(entid) => {
            PatternSlot::Constant(DataType::Long(*entid).encode())
        }
        // TODO This needs proper ident resolution for refs
        PatternNonValuePlace::Ident(ident) => {
            PatternSlot::Constant(DataType::Keyword(ident.as_ref().clone()).encode())
        }
        PatternNonValuePlace::Placeholder => {
            unreachable!("entity placeholders are rejected before planning")
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
            unreachable!("value placeholders are rejected before planning")
        }
    }
}

fn pattern_output_vars(entity: &PatternSlot, value: &PatternSlot) -> Vec<Variable> {
    let mut vars = Vec::new();
    if let PatternSlot::Variable(var) = entity {
        vars.push(var.clone());
    }
    if let PatternSlot::Variable(var) = value {
        vars.push(var.clone());
    }
    vars
}

fn collect_variables(terms: &[WhereTermPlan]) -> Vec<Variable> {
    let mut variables = Vec::new();
    for term in terms {
        for var in term.output_vars() {
            if !variables.contains(var) {
                variables.push(var.clone());
            }
        }
    }
    variables
}

fn collect_leaf_patterns<'a>(terms: &'a [WhereTermPlan], patterns: &mut Vec<&'a PatternPlan>) {
    for term in terms {
        match term {
            WhereTermPlan::Pattern(pattern) => patterns.push(pattern),
            WhereTermPlan::Or(or) => collect_leaf_patterns(&or.branches, patterns),
        }
    }
}

fn plan_joins(terms: &[WhereTermPlan]) -> Vec<JoinPlan> {
    let mut joins = Vec::new();
    let mut left_vars = terms[0].output_vars().to_vec();
    let mut bound: HashSet<Variable> = left_vars.iter().cloned().collect();

    for (right_term_index, term) in terms.iter().enumerate().skip(1) {
        let right_vars = term.output_vars().to_vec();
        let right_set: HashSet<Variable> = right_vars.iter().cloned().collect();
        // Note: Not using intersection to preserve ordering from left_vars
        let key_vars = left_vars
            .iter()
            .filter(|var| right_set.contains(*var))
            .cloned()
            .collect::<Vec<_>>();

        let mut output_vars = left_vars.clone();
        for var in &right_vars {
            if bound.insert(var.clone()) {
                output_vars.push(var.clone());
            }
        }

        joins.push(JoinPlan {
            right_term_index,
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
        let leaf_patterns = plan.leaf_patterns();
        assert_eq!(leaf_patterns.len(), 1);
        assert_eq!(leaf_patterns[0].attribute, 10);
        assert_eq!(
            &leaf_patterns[0].output_vars,
            &vec!["?e".to_var(), "?name".to_var()]
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
        assert_eq!(plan.joins[0].right_term_index, 1);
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

        assert_eq!(plan.joins[0].right_term_index, 1);
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

        assert_eq!(plan.joins.len(), 1);
        assert_eq!(plan.joins[0].right_term_index, 1);
        assert!(plan.joins[0].key_vars.is_empty());
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
        assert!(plan.joins.is_empty());
        assert!(matches!(plan.where_terms[0], WhereTermPlan::Or(_)));
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
        let WhereTermPlan::Or(or) = &plan.where_terms[0] else {
            panic!("expected or term");
        };
        assert_eq!(or.output_vars, vec!["?e".to_var(), "?v".to_var()]);
        assert_eq!(
            or.branches[0].output_vars(),
            &["?e".to_var(), "?v".to_var()]
        );
        assert_eq!(
            or.branches[1].output_vars(),
            &["?v".to_var(), "?e".to_var()]
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
    fn rejects_unsupported_where_forms() {
        assert_plan_err(
            r#"[:find ?e :where [?e :name "Alice"] [(< 1 2)]]"#,
            "only triple patterns and or clauses",
        );
        assert_plan_err(
            r#"[:find ?e :where [?e :name "Alice"] (not [?e :age 30])]"#,
            "only triple patterns and or clauses",
        );
        assert_plan_err(
            r#"[:find ?e :where (or (and [?e :name "Alice"] [?e :age 30]) [?e :name "Bob"])]"#,
            "or branches currently support only triple patterns or nested or clauses",
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

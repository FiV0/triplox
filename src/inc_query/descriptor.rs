use std::collections::HashSet;

use anyhow::{anyhow, Result};
use edn::query::{
    NotJoin, OrJoin, OrWhereClause, Pattern, PatternNonValuePlace, PatternValuePlace, Predicate,
    Variable, WhereClause, WhereFn,
};

use crate::codec::Encode;
use crate::expr::{expr_variables, Expr};
use crate::incremental::EncodedValue;
use crate::ops::DataType;
use crate::query::{convert_predicate, convert_where_fn, non_integer_constant_to_datatype};
use crate::schema::Schema;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PatternSlot {
    Variable(Variable),
    Constant(EncodedValue),
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct ScopeDescriptor {
    pub descriptors: Vec<Descriptor>,
    pub variables: Vec<Variable>,
    pub groundable: Vec<Variable>,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Descriptor {
    pub variables: Vec<Variable>,
    pub groundable: Vec<Variable>,
    pub kind: DescriptorKind,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum DescriptorKind {
    Pattern(PatternDescriptor),
    Predicate { expr: Expr },
    Function { expr: Expr, output_var: Variable },
    Not { scope: ScopeDescriptor },
    Or { branches: Vec<ScopeDescriptor> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PatternDescriptor {
    pub attribute: i64,
    pub entity: PatternSlot,
    pub value: PatternSlot,
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

fn pattern_descriptor(pattern: &Pattern, schema: &Schema) -> Result<PatternDescriptor> {
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

    Ok(PatternDescriptor {
        attribute,
        entity: non_value_slot(&pattern.entity),
        value: value_slot(&pattern.value)?,
    })
}

fn pattern_variables(pattern: &PatternDescriptor) -> Vec<Variable> {
    let mut variables = Vec::new();
    if let PatternSlot::Variable(var) = &pattern.entity {
        variables.push(var.clone());
    }
    if let PatternSlot::Variable(var) = &pattern.value {
        variables.push(var.clone());
    }
    variables
}

fn ordered_union<'a>(variables: impl IntoIterator<Item = &'a Variable>) -> Vec<Variable> {
    let mut result = Vec::new();
    let mut seen = HashSet::new();
    for variable in variables {
        if seen.insert(variable.clone()) {
            result.push(variable.clone());
        }
    }
    result
}

fn describe_predicate(predicate: &Predicate) -> Result<Descriptor> {
    let expr = convert_predicate(predicate)?;
    let variables = expr_variables(&expr);
    Ok(Descriptor {
        variables,
        groundable: Vec::new(),
        kind: DescriptorKind::Predicate { expr },
    })
}

fn describe_function(function: &WhereFn) -> Result<Descriptor> {
    let function = convert_where_fn(function)?;
    let mut variables = function.input_variables();
    let output_is_input = variables.contains(&function.output);
    if !output_is_input {
        variables.push(function.output.clone());
    }
    let groundable = if output_is_input {
        Vec::new()
    } else {
        vec![function.output.clone()]
    };

    Ok(Descriptor {
        variables,
        groundable,
        kind: DescriptorKind::Function {
            expr: function.expr,
            output_var: function.output,
        },
    })
}

fn describe_where_clause(clause: &WhereClause, schema: &Schema) -> Result<Descriptor> {
    match clause {
        WhereClause::Pattern(pattern) => {
            let pattern = pattern_descriptor(pattern, schema)?;
            let variables = pattern_variables(&pattern);
            Ok(Descriptor {
                groundable: variables.clone(),
                variables,
                kind: DescriptorKind::Pattern(pattern),
            })
        }
        WhereClause::Pred(predicate) => describe_predicate(predicate),
        WhereClause::WhereFn(function) => describe_function(function),
        WhereClause::NotJoin(not) => describe_not(not, schema),
        WhereClause::OrJoin(or) => describe_or(or, schema),
        WhereClause::TypeAnnotation(_) | WhereClause::RuleExpr => {
            unreachable!("unsupported clauses are rejected before planning")
        }
    }
}

fn describe_not(not: &NotJoin, schema: &Schema) -> Result<Descriptor> {
    let scope = describe_where_clauses(&not.clauses, schema)?;
    Ok(Descriptor {
        variables: scope.variables.clone(),
        groundable: Vec::new(),
        kind: DescriptorKind::Not { scope },
    })
}

fn describe_or_branch(branch: &OrWhereClause, schema: &Schema) -> Result<ScopeDescriptor> {
    match branch {
        OrWhereClause::Clause(clause) => {
            describe_where_clauses(std::slice::from_ref(clause), schema)
        }
        OrWhereClause::And(clauses) => describe_where_clauses(clauses, schema),
    }
}

fn describe_or(or: &OrJoin, schema: &Schema) -> Result<Descriptor> {
    let branches = or
        .clauses
        .iter()
        .map(|branch| describe_or_branch(branch, schema))
        .collect::<Result<Vec<_>>>()?;
    let first = branches
        .first()
        .ok_or_else(|| anyhow!("OR clause must have at least one branch"))?;
    let variables = first.variables.clone();
    let groundable = first
        .groundable
        .iter()
        .filter(|variable| {
            branches
                .iter()
                .skip(1)
                .all(|branch| branch.groundable.contains(variable))
        })
        .cloned()
        .collect();

    Ok(Descriptor {
        variables,
        groundable,
        kind: DescriptorKind::Or { branches },
    })
}

pub(super) fn describe_where_clauses(
    clauses: &[WhereClause],
    schema: &Schema,
) -> Result<ScopeDescriptor> {
    let descriptors = clauses
        .iter()
        .map(|clause| describe_where_clause(clause, schema))
        .collect::<Result<Vec<_>>>()?;
    let variables = ordered_union(
        descriptors
            .iter()
            .flat_map(|descriptor| &descriptor.variables),
    );
    let groundable = ordered_union(
        descriptors
            .iter()
            .flat_map(|descriptor| &descriptor.groundable),
    );

    Ok(ScopeDescriptor {
        descriptors,
        variables,
        groundable,
    })
}

#[cfg(test)]
mod tests {
    use edn::query::ToVariable;

    use super::super::test_support::{parse_query, test_schema};
    use super::*;

    #[test]
    fn triple_variables_are_all_groundable_in_encounter_order() {
        let query = parse_query("[:find ?e ?name :where [?e :name ?name]]");
        let scope = describe_where_clauses(&query.where_clauses, &test_schema()).unwrap();

        assert_eq!(scope.variables, vec!["?e".to_var(), "?name".to_var()]);
        assert_eq!(scope.groundable, scope.variables);
        assert_eq!(scope.descriptors[0].variables, scope.variables);
        assert_eq!(scope.descriptors[0].groundable, scope.variables);
        let DescriptorKind::Pattern(pattern) = &scope.descriptors[0].kind else {
            panic!("expected pattern descriptor");
        };
        assert_eq!(pattern.attribute, 10);
    }

    #[test]
    fn scope_metadata_is_the_ordered_union_of_its_descriptors() {
        let query = parse_query("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]");
        let scope = describe_where_clauses(&query.where_clauses, &test_schema()).unwrap();

        assert_eq!(
            scope.variables,
            vec!["?e".to_var(), "?name".to_var(), "?age".to_var()]
        );
        assert_eq!(scope.groundable, scope.variables);
    }

    #[test]
    fn or_metadata_uses_first_branch_order_for_groundable_intersection() {
        let query = parse_query("[:find ?e ?v :where (or [?e :name ?v] [?v :follows ?e])]");
        let scope = describe_where_clauses(&query.where_clauses, &test_schema()).unwrap();
        let descriptor = &scope.descriptors[0];

        assert_eq!(descriptor.variables, vec!["?e".to_var(), "?v".to_var()]);
        assert_eq!(descriptor.groundable, descriptor.variables);
        let DescriptorKind::Or { branches } = &descriptor.kind else {
            panic!("expected or descriptor");
        };
        assert_eq!(branches[0].variables, vec!["?e".to_var(), "?v".to_var()]);
        assert_eq!(branches[1].variables, vec!["?v".to_var(), "?e".to_var()]);
    }

    #[test]
    fn not_metadata_mentions_variables_without_grounding_them() {
        let query = parse_query("[:find ?e :where [?e :name ?name] (not [?e :age ?age])]");
        let scope = describe_where_clauses(&query.where_clauses, &test_schema()).unwrap();
        let descriptor = &scope.descriptors[1];

        assert_eq!(descriptor.variables, vec!["?e".to_var(), "?age".to_var()]);
        assert!(descriptor.groundable.is_empty());
        let DescriptorKind::Not { scope } = &descriptor.kind else {
            panic!("expected not descriptor");
        };
        assert_eq!(scope.variables, vec!["?e".to_var(), "?age".to_var()]);
        assert_eq!(scope.groundable, scope.variables);
    }

    #[test]
    fn predicate_metadata_mentions_variables_without_grounding_them() {
        let query = parse_query("[:find ?age :where [?e :age ?age] [(< ?age 30)]]");
        let scope = describe_where_clauses(&query.where_clauses, &test_schema()).unwrap();
        let descriptor = &scope.descriptors[1];

        assert_eq!(descriptor.variables, vec!["?age".to_var()]);
        assert!(descriptor.groundable.is_empty());
        let DescriptorKind::Predicate { expr } = &descriptor.kind else {
            panic!("expected predicate descriptor");
        };
        assert_eq!(crate::expr::expr_variables(expr), vec!["?age".to_var()]);
    }

    #[test]
    fn function_metadata_grounds_only_new_result_variables() {
        let query = parse_query(
            "[:find ?next
              :where
              [(+ ?age 1) ?next]
              [(+ ?next 1) ?next]]",
        );
        let scope = describe_where_clauses(&query.where_clauses, &test_schema()).unwrap();

        let new_result = &scope.descriptors[0];
        assert_eq!(
            new_result.variables,
            vec!["?age".to_var(), "?next".to_var()]
        );
        assert_eq!(new_result.groundable, vec!["?next".to_var()]);
        let DescriptorKind::Function { expr, output_var } = &new_result.kind else {
            panic!("expected function descriptor");
        };
        assert_eq!(crate::expr::expr_variables(expr), vec!["?age".to_var()]);
        assert_eq!(output_var, &"?next".to_var());

        let self_referential = &scope.descriptors[1];
        assert_eq!(self_referential.variables, vec!["?next".to_var()]);
        assert!(self_referential.groundable.is_empty());
    }
}

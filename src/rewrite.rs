use anyhow::Result;

use edn::query::{
    OrJoin, OrWhereClause, ParsedQuery, PatternNonValuePlace, PatternValuePlace, UnifyVars,
    Variable, WhereClause,
};

use crate::query::{clause_mentioned_variables, or_branch_mentioned_variables};
use crate::query_validation::validate_join_clauses;

struct Rewriter {
    next: usize,
}

impl Rewriter {
    fn fresh_variable(&mut self) -> Variable {
        let variable = Variable::from_valid_name(&format!("?tb_placeholder{}", self.next));
        self.next += 1;
        variable
    }

    fn rewrite_clause(&mut self, clause: &mut WhereClause) {
        let previous = self.next;
        match clause {
            WhereClause::Pattern(pattern) => {
                if matches!(pattern.entity, PatternNonValuePlace::Placeholder) {
                    pattern.entity = PatternNonValuePlace::Variable(self.fresh_variable());
                }
                if matches!(pattern.value, PatternValuePlace::Placeholder) {
                    pattern.value = PatternValuePlace::Variable(self.fresh_variable());
                }
            }
            WhereClause::OrJoin(or) => {
                let variables = or
                    .clauses
                    .iter()
                    .flat_map(or_branch_mentioned_variables)
                    .collect();
                for branch in &mut or.clauses {
                    match branch {
                        OrWhereClause::Clause(clause) => self.rewrite_clause(clause),
                        OrWhereClause::And(clauses) => self.rewrite_clauses(clauses),
                    }
                }
                let unify_vars =
                    if previous != self.next && matches!(or.unify_vars, UnifyVars::Implicit) {
                        UnifyVars::Explicit(variables)
                    } else {
                        or.unify_vars.clone()
                    };
                // Rebuild the OR to discard cached mentioned variables.
                *or = OrJoin::new(unify_vars, std::mem::take(&mut or.clauses));
            }
            WhereClause::NotJoin(not) => {
                let variables = not
                    .clauses
                    .iter()
                    .flat_map(clause_mentioned_variables)
                    .collect();
                self.rewrite_clauses(&mut not.clauses);
                if previous != self.next && matches!(not.unify_vars, UnifyVars::Implicit) {
                    not.unify_vars = UnifyVars::Explicit(variables);
                }
            }
            _ => {}
        }
    }

    fn rewrite_clauses(&mut self, clauses: &mut [WhereClause]) {
        for clause in clauses {
            self.rewrite_clause(clause);
        }
    }
}

/// Lower entity/value placeholders after validating the original query's scopes.
pub(crate) fn rewrite_query(query: &ParsedQuery) -> Result<ParsedQuery> {
    let mut rewriter = Rewriter { next: 0 };
    let mut query = query.clone();
    rewriter.rewrite_clauses(&mut query.where_clauses);
    validate_join_clauses(&query.where_clauses)?;
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use edn::parse::parse_query;

    #[test]
    fn each_placeholder_gets_a_distinct_variable() {
        let query = parse_query(
            "{:find [?name]
              :where [[_ :name ?name] [_ :age _]]}",
        )
        .unwrap();
        let original = query.clone();
        let expected = parse_query(
            "{:find [?name]
              :where [[?tb_placeholder0 :name ?name]
                      [?tb_placeholder1 :age ?tb_placeholder2]]}",
        )
        .unwrap();
        assert_eq!(rewrite_query(&query).unwrap(), expected);
        assert_eq!(rewrite_query(&expected).unwrap(), expected);
        assert_eq!(query, original);
    }

    #[test]
    fn nested_placeholders_become_local_explicit_join_variables() {
        let mut query = parse_query(
            "[:find ?e :where (or [?e :name _] (and [?e :age _] (not [?e :email _])))]",
        )
        .unwrap();
        let WhereClause::OrJoin(or) = &mut query.where_clauses[0] else {
            panic!("expected OR");
        };
        assert_eq!(or.mentioned_variables().len(), 1);
        let expected = parse_query("[:find ?e :where (or-join [?e] [?e :name ?tb_placeholder0] (and [?e :age ?tb_placeholder1] (not-join [?e] [?e :email ?tb_placeholder2])))]").unwrap();
        let mut rewritten = rewrite_query(&query).unwrap();
        assert_eq!(rewritten, expected);
        let WhereClause::OrJoin(or) = &mut rewritten.where_clauses[0] else {
            panic!("expected OR");
        };
        assert_eq!(or.mentioned_variables().len(), 4);
    }

    #[test]
    fn explicit_interfaces_hide_named_locals_from_enclosing_rewrites() {
        let query = parse_query("[:find ?e :where (or [?e :name _] (or-join [?e] (and [?e :age ?local] [?e :email _])))]").unwrap();
        let expected = parse_query("[:find ?e :where (or-join [?e] [?e :name ?tb_placeholder0] (or-join [?e] (and [?e :age ?local] [?e :email ?tb_placeholder1])))]").unwrap();
        assert_eq!(rewrite_query(&query).unwrap(), expected);
    }

    #[test]
    fn rejects_empty_generated_join_interfaces() {
        for (query, message) in [
            (
                "{:find [?e]
                  :where [[?e :name _] (not [_ :age _])]}",
                "NOT-JOIN requires at least one join variable",
            ),
            (
                "{:find [?e]
                  :where [[?e :name _] (or [_ :age _] [_ :name _])]}",
                "OR-JOIN requires at least one join variable",
            ),
        ] {
            let error = rewrite_query(&parse_query(query).unwrap()).unwrap_err();
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn leaves_other_placeholder_positions_and_query_fields_unchanged() {
        let query = parse_query("[:find ?e :with ?value :in [?input _] :where [?e _ ?value _] [(identity ?value) [?result _]] :order [?e :asc] :limit 10]").unwrap();
        assert_eq!(rewrite_query(&query).unwrap(), query);
    }
}

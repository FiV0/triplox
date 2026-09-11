use std::collections::HashSet;

use edn::query::{
    OrJoin, OrWhereClause, ParsedQuery, PatternNonValuePlace, PatternValuePlace, Variable,
    WhereClause,
};

#[derive(Debug)]
pub(crate) struct RewrittenQuery {
    pub query: ParsedQuery,
    pub generated_variables: HashSet<Variable>,
}

#[derive(Default)]
struct Rewriter {
    generated_variables: HashSet<Variable>,
}

impl Rewriter {
    fn fresh_variable(&mut self) -> Variable {
        let variable =
            Variable::from_valid_name(&format!("?_internal_{}", self.generated_variables.len()));
        self.generated_variables.insert(variable.clone());
        variable
    }

    fn rewrite_clause(&mut self, clause: &mut WhereClause) {
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
                for branch in &mut or.clauses {
                    match branch {
                        OrWhereClause::Clause(clause) => self.rewrite_clause(clause),
                        OrWhereClause::And(clauses) => self.rewrite_clauses(clauses),
                    }
                }
                // Rebuild the OR to discard its cached mentioned variables.
                *or = OrJoin::new(or.unify_vars.clone(), std::mem::take(&mut or.clauses));
            }
            WhereClause::NotJoin(not) => self.rewrite_clauses(&mut not.clauses),
            WhereClause::Pred(_)
            | WhereClause::WhereFn(_)
            | WhereClause::RuleExpr
            | WhereClause::TypeAnnotation(_) => {}
        }
    }

    fn rewrite_clauses(&mut self, clauses: &mut [WhereClause]) {
        for clause in clauses {
            self.rewrite_clause(clause);
        }
    }
}

pub(crate) fn rewrite_query(query: &ParsedQuery) -> RewrittenQuery {
    let mut query = query.clone();
    let mut rewriter = Rewriter::default();
    rewriter.rewrite_clauses(&mut query.where_clauses);
    RewrittenQuery {
        query,
        generated_variables: rewriter.generated_variables,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_query(query: &str) -> ParsedQuery {
        edn::parse::parse_query(query).unwrap()
    }

    #[test]
    fn rewrites_each_entity_and_value_placeholder_independently() {
        let query = parse_query("[:find ?name :where [_ :name ?name] [_ :age _]]");
        let original = query.clone();
        let expected = parse_query(
            "[:find ?name :where [?_internal_0 :name ?name] [?_internal_1 :age ?_internal_2]]",
        );

        assert_eq!(rewrite_query(&query).query, expected);
        assert_eq!(rewrite_query(&query).query, expected);
        assert_eq!(rewrite_query(&expected).query, expected);
        assert_eq!(query, original);
        assert_eq!(rewrite_query(&query).generated_variables.len(), 3);
        assert!(rewrite_query(&expected).generated_variables.is_empty());
    }

    #[test]
    fn rewrites_nested_clauses_and_clears_or_variable_caches() {
        let mut query = parse_query(
            "[:find ?e :where (or [?e :name _]
                (and [_ :age _] (not [_ :email _]))) [_ :name _]]",
        );
        let WhereClause::OrJoin(or) = &mut query.where_clauses[0] else {
            panic!("expected OR");
        };
        assert_eq!(or.mentioned_variables().len(), 1);

        let mut rewritten = rewrite_query(&query).query;
        let expected = parse_query(
            "[:find ?e :where (or [?e :name ?_internal_0]
                (and [?_internal_1 :age ?_internal_2]
                     (not [?_internal_3 :email ?_internal_4])))
                [?_internal_5 :name ?_internal_6]]",
        );
        assert_eq!(rewritten, expected);
        let WhereClause::OrJoin(or) = &mut rewritten.where_clauses[0] else {
            panic!("expected OR");
        };
        assert_eq!(or.mentioned_variables().len(), 6);
    }

    #[test]
    fn preserves_other_positions_and_query_fields() {
        let query = parse_query(
            "[:find ?e :with ?value :in [?input _] :where
                [?e _ ?value _] [(+ ?value 1) [?result _]]
                :order [?e :asc] :limit 10]",
        );
        assert_eq!(rewrite_query(&query).query, query);
    }

    #[test]
    fn does_not_avoid_user_variable_names() {
        let query = parse_query("[:find ?_internal_0 :where [_ :name ?_internal_0]]");
        assert_eq!(
            rewrite_query(&query).query,
            parse_query("[:find ?_internal_0 :where [?_internal_0 :name ?_internal_0]]")
        );
    }
}

use std::collections::{HashMap, HashSet};

use petgraph::{algo, Directed};
use petgraph::graphmap::DiGraphMap;
use petgraph::prelude::GraphMap;
use crate::models::datalog::{Rule, Sign};
use crate::models::datalog::Sign::Negative;

type RuleGraph<'a> = GraphMap<&'a Rule, Sign, Directed>;

pub fn generate_rule_dependency_graph<'a>(program: &Vec<Rule>) -> RuleGraph {
    let mut output = DiGraphMap::new();
    let mut idb_relations = HashMap::new();
    for rule in program {
        idb_relations.insert(rule.clone().head.symbol, rule);
        output.add_node(rule);
    }
    for rule in program {
        for bodyAtom in &rule.body {
            if let Some(bodyAtomRule) = idb_relations.get(&bodyAtom.symbol) {
                output.add_edge(bodyAtomRule, rule, bodyAtom.sign.clone());
            }
        }
    }
    return output
}

pub fn stratify<'a>(rule_graph: &'a RuleGraph) -> (bool, Vec<Vec<&'a Rule>>) {
    let sccs = algo::kosaraju_scc(&rule_graph);
    for scc in &sccs {
        let mut relations = HashSet::new();
        for rule in scc {
            relations.insert(rule.head.symbol.clone());
        }
        for rule in scc {
            for atom in &rule.body {
                if relations.contains(&atom.symbol) && atom.sign == Negative {
                    return (false, sccs)
                }
            }
        }
    }
    return (true, sccs)
}



mod test {
    use std::collections::HashSet;
    use crate::implementations::rule_graph::generate_rule_dependency_graph;
    use crate::models::datalog::{Atom, Rule, Sign};

    #[test]
    fn generate_rule_dependency_graph_test() {
        let R = "R".to_string();
        let R_prime_1 = "R'_1'".to_string();
        let R_prime_2 = "R'_2".to_string();
        let R_prime_3 = "R'_3".to_string();
        let R_prime_4 = "R'_4".to_string();
        let S = "S".to_string();
        let T = "T".to_string();
        let U = "U".to_string();
        let V = "V".to_string();
        let r_1 = Rule {
            head: Atom {
                terms: vec![],
                symbol: S.clone(),
                sign: Sign::Positive
            },
            body: vec![Atom {
                terms: vec![],
                symbol: R_prime_1.clone(),
                sign: Sign::Positive
            }, Atom {
                terms: vec![],
                symbol: R.clone(),
                sign: Sign::Negative
            }]
        };

        let r_2 = Rule {
            head: Atom {
                terms: vec![],
                symbol: T.clone(),
                sign: Sign::Positive
            },
            body: vec![Atom {
                terms: vec![],
                symbol: R_prime_2.clone(),
                sign: Sign::Positive
            }, Atom {
                terms: vec![],
                symbol: R.clone(),
                sign: Sign::Negative
            }]
        };

        let r_3 = Rule {
            head: Atom {
                terms: vec![],
                symbol: U.clone(),
                sign: Sign::Positive
            },
            body: vec![Atom {
                terms: vec![],
                symbol: R_prime_3.clone(),
                sign: Sign::Positive
            }, Atom {
                terms: vec![],
                symbol: T.clone(),
                sign: Sign::Negative
            }]
        };

        let r_4 = Rule {
            head: Atom {
                terms: vec![],
                symbol: V.clone(),
                sign: Sign::Positive
            },
            body: vec![Atom {
                terms: vec![],
                symbol: R_prime_4.clone(),
                sign: Sign::Positive
            }, Atom {
                terms: vec![],
                symbol: S.clone(),
                sign: Sign::Negative
            }, Atom {
                terms: vec![],
                symbol: U.clone(),
                sign: Sign::Negative
            }]
        };

        let not_recursive_program = vec![
            r_1.clone(),
            r_2.clone(),
            r_3.clone(),
            r_4.clone()
        ];

        let graph = generate_rule_dependency_graph(&not_recursive_program);
        let edges: HashSet<(&Rule, &Rule, &Sign)> = graph.all_edges().into_iter().collect();
        assert_eq!(edges.contains(&(&r_2, &r_3, &Sign::Negative)), true);
        assert_eq!(edges.contains(&(&r_3, &r_4, &Sign::Negative)), true);
        assert_eq!(edges.contains(&(&r_1, &r_4, &Sign::Negative)), true);
    }
}


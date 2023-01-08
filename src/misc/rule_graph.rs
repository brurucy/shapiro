use std::collections::{HashMap, HashSet};

use petgraph::graphmap::DiGraphMap;
use petgraph::prelude::GraphMap;
use petgraph::{algo, Directed};
use crate::models::datalog::Rule;

type RuleGraph<'a> = GraphMap<&'a Rule, bool, Directed>;

pub fn generate_rule_dependency_graph<'a>(program: &Vec<Rule>) -> RuleGraph {
    let mut output = DiGraphMap::new();
    let mut idb_relations = HashMap::new();
    for rule in program {
        idb_relations.insert(rule.clone().head.symbol, rule);
        output.add_node(rule);
    }
    for rule in program {
        for body_atom in &rule.body {
            if let Some(body_atom_rule) = idb_relations.get(&body_atom.symbol) {
                output.add_edge(body_atom_rule, rule, body_atom.sign.clone());
            }
        }
    }
    return output;
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
                if relations.contains(&atom.symbol) && !atom.sign {
                    return (false, sccs);
                }
            }
        }
    }
    return (true, sccs);
}

pub fn sort_program(program: &Vec<Rule>) -> Vec<Rule> {
    let rule_graph = generate_rule_dependency_graph(&program);
    let (_valid, stratification) = stratify(&rule_graph);

    return stratification.iter().flatten().cloned().cloned().collect();
}

#[cfg(test)]
mod tests {
    use crate::misc::rule_graph::generate_rule_dependency_graph;
    use crate::models::datalog::{Atom, Rule};
    use std::collections::HashSet;

    #[test]
    fn generate_rule_dependency_graph_test() {
        let r = "r".to_string();
        let r_prime_1 = "r'_1'".to_string();
        let r_prime_2 = "r'_2".to_string();
        let r_prime_3 = "r'_3".to_string();
        let r_prime_4 = "r'_4".to_string();
        let s = "S".to_string();
        let t = "T".to_string();
        let u = "U".to_string();
        let v = "V".to_string();
        let r_1 = Rule {
            head: Atom {
                terms: vec![],
                symbol: s.clone(),
                sign: true,
            },
            body: vec![
                Atom {
                    terms: vec![],
                    symbol: r_prime_1.clone(),
                    sign: true,
                },
                Atom {
                    terms: vec![],
                    symbol: r.clone(),
                    sign: true,
                },
            ],
        };

        let r_2 = Rule {
            head: Atom {
                terms: vec![],
                symbol: t.clone(),
                sign: true,
            },
            body: vec![
                Atom {
                    terms: vec![],
                    symbol: r_prime_2.clone(),
                    sign: true,
                },
                Atom {
                    terms: vec![],
                    symbol: r.clone(),
                    sign: true,
                },
            ],
        };

        let r_3 = Rule {
            head: Atom {
                terms: vec![],
                symbol: u.clone(),
                sign: true,
            },
            body: vec![
                Atom {
                    terms: vec![],
                    symbol: r_prime_3.clone(),
                    sign: true,
                },
                Atom {
                    terms: vec![],
                    symbol: t.clone(),
                    sign: true,
                },
            ],
        };

        let r_4 = Rule {
            head: Atom {
                terms: vec![],
                symbol: v.clone(),
                sign: true,
            },
            body: vec![
                Atom {
                    terms: vec![],
                    symbol: r_prime_4.clone(),
                    sign: true,
                },
                Atom {
                    terms: vec![],
                    symbol: s.clone(),
                    sign: true,
                },
                Atom {
                    terms: vec![],
                    symbol: u.clone(),
                    sign: true,
                },
            ],
        };

        let not_recursive_program = vec![r_1.clone(), r_2.clone(), r_3.clone(), r_4.clone()];

        let graph = generate_rule_dependency_graph(&not_recursive_program);
        let edges: HashSet<(&Rule, &Rule, &bool)> = graph.all_edges().into_iter().collect();
        assert_eq!(edges.contains(&(&r_2, &r_3, &false)), true);
        assert_eq!(edges.contains(&(&r_3, &r_4, &false)), true);
        assert_eq!(edges.contains(&(&r_1, &r_4, &false)), true);
    }
}

mod abomonated_model;
mod abomonated_parsing;
mod abomonated_vertebra;

use std::thread;
use crate::misc::string_interning::Interner;
use crate::models::datalog::{Program, TypedValue};
use crate::models::instance::{Instance};
use crossbeam_channel::{Receiver, Sender, unbounded};
use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use std::time::{Duration};
use differential_dataflow::algorithms::identifiers::Identifiers;
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
use differential_dataflow::operators::{Consolidate, iterate, Join, JoinCore, Threshold};
use timely::communication::allocator::Generic;
use timely::dataflow::Scope;
use timely::dataflow::scopes::Child;
use timely::order::Product;
use timely::worker::Worker;
use crate::models::reasoner::{Diff, DynamicTyped, Materializer};
use crate::reasoning::reasoners::differential::abomonated_model::{AbomonatedAtom, AbomonatedRule, AbomonatedSign, AbomonatedTerm, AbomonatedTypedValue};
use crate::reasoning::reasoners::differential::abomonated_vertebra::{AbomonatedSubstitutions};

pub type AtomCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, AbomonatedAtom>;
pub type SubstitutionsCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, AbomonatedSubstitutions>;

pub type RuleSink = Sender<(AbomonatedRule, isize)>;
pub type AtomSink = Sender<(AbomonatedAtom, isize)>;

pub type RuleSource = Receiver<(AbomonatedRule, isize)>;
pub type AtomSource = Receiver<(AbomonatedAtom, isize)>;

fn make_substitutions(left: &AbomonatedAtom, right: &AbomonatedAtom) -> Option<AbomonatedSubstitutions> {
    let mut substitution: AbomonatedSubstitutions = Default::default();

    let left_and_right = left.terms.iter().zip(right.terms.iter());

    for (left_term, right_term) in left_and_right {
        match (left_term, right_term) {
            (AbomonatedTerm::Constant(left_constant), AbomonatedTerm::Constant(right_constant)) => {
                if *left_constant != *right_constant {
                    return None;
                }
            }
            (AbomonatedTerm::Variable(left_variable), AbomonatedTerm::Constant(right_constant)) => {
                if let Some(constant) = substitution.inner.get(*left_variable) {
                    if constant.clone() != *right_constant {
                        return None;
                    }
                } else {
                    substitution.inner.insert((left_variable.clone(), right_constant.clone()));
                }
            }
            _ => {}
        }
    }

    return Some(substitution);
}

fn attempt_to_rewrite(rewrite: &AbomonatedSubstitutions, atom: &AbomonatedAtom) -> AbomonatedAtom {
    return AbomonatedAtom {
        terms: atom
            .terms
            .clone()
            .into_iter()
            .map(|term| {
                if let AbomonatedTerm::Variable(identifier) = term.clone() {
                    if let Some(constant) = rewrite.inner.get(identifier) {
                        return AbomonatedTerm::Constant(constant.clone());
                    }
                }
                return term;
            })
            .collect(),
        symbol: atom.clone().symbol,
        sign: atom.clone().sign,
    };
}

fn is_ground(atom: &AbomonatedAtom) -> bool {
    for term in atom.terms.iter() {
        match term {
            AbomonatedTerm::Variable(_) => {
                return false;
            }
            _ => {}
        };
    }
    return true;
}

pub fn reason(
    rule_input_source: RuleSource,
    fact_input_source: AtomSource,
    rule_output_sink: RuleSink,
    fact_output_sink: AtomSink,
) -> () {
    timely::execute(
        timely::Config::process(8),
        move |worker: &mut Worker<Generic>| {
            let (mut rule_input_session, mut rule_trace, rule_probe) = worker
                .dataflow_named::<usize, _, _>("rule_ingestion", |local| {
                    let local_rule_output_sink = rule_output_sink.clone();

                    let (rule_input, rule_collection) = local.new_collection::<AbomonatedRule, isize>();
                    rule_collection.inspect(move |x| {
                        local_rule_output_sink.send((x.0.clone(), x.2)).unwrap();
                    });

                    (
                        rule_input,
                        rule_collection.arrange_by_self().trace,
                        rule_collection.probe(),
                    )
                });

            let (mut fact_input_session, fact_probe) = worker
                .dataflow_named::<usize, _, _>("fact_ingestion_and_reasoning", |local| {
                    let (fact_input_session, fact_collection) =
                        local.new_collection::<AbomonatedAtom, isize>();

                    let local_fact_output_sink = fact_output_sink.clone();
                    let rule_collection = rule_trace.import(local).as_collection(|x, _y| x.clone());

                    let facts_by_symbol = fact_collection
                        .map(|ground_fact| (ground_fact.symbol.clone(), ground_fact));

                    let indexed_rules = rule_collection.identifiers();
                    let goals = indexed_rules
                        .flat_map(|(rule, rule_id)| {
                            rule
                                .body
                                .into_iter()
                                .enumerate()
                                .map(move |(atom_id, atom)| {
                                    ((rule_id, atom_id), atom)
                                })
                        });

                    let head = indexed_rules
                        .map(|rule_and_id| (rule_and_id.1, rule_and_id.0.head));

                    let subs_product = head
                        .map(|(rule_id, _head)| ((rule_id, 0), AbomonatedSubstitutions::default()));

                    local
                        .iterative::<usize, _, _>(|inner| {
                            let subs_product_var = iterate::Variable::new_from(subs_product.enter(inner), Product::new(Default::default(), 1));
                            let data_var = iterate::Variable::new_from(facts_by_symbol.enter(inner), Product::new(Default::default(), 1));

                            let s_old = subs_product_var.consolidate();
                            let g = goals.enter(inner);

                            let s_old_arr = s_old.arrange_by_key();
                            let data = data_var.distinct();
                            let data_arr = data.arrange_by_key();

                            let goal_x_subs = g
                                .arrange_by_key()
                                .join_core(&s_old_arr, |key, goal, sub| {
                                    let rewrite_attempt = &attempt_to_rewrite(sub, goal);
                                    if !is_ground(rewrite_attempt) {
                                        let new_key = (key.clone(), goal.clone(), sub.clone());
                                        return Some((goal.symbol.clone(), (new_key, rewrite_attempt.clone())));
                                    }
                                    return None;
                                });

                            let current_goals = goal_x_subs
                                .arrange_by_key();

                            let new_substitutions = data_arr
                                .join_core(&current_goals, |_sym, ground_fact, (new_key, rewrite_attempt)| {
                                    let ground_terms = ground_fact
                                        .clone()
                                        .terms
                                        .into_iter()
                                        .map(|row_element| AbomonatedTerm::Constant(AbomonatedTypedValue::try_from(row_element.clone()).unwrap()))
                                        .collect();

                                    let proposed_atom = AbomonatedAtom {
                                        terms: ground_terms,
                                        symbol: rewrite_attempt.symbol.clone(),
                                        sign: AbomonatedSign::Positive,
                                    };

                                    let sub = make_substitutions(
                                        &rewrite_attempt,
                                        &proposed_atom,
                                    );

                                    match sub {
                                        None => {
                                            None
                                        }
                                        Some(sub) => {
                                            Some(((new_key.0, new_key.2.clone()), sub))
                                        }
                                    }
                                });

                            let s_new_arr = new_substitutions
                                .arrange_by_key();

                            let s_old_arr = s_old
                                .map(|(iter, sub)| ((iter.clone(), sub.clone()), sub.clone()))
                                .arrange_by_key();

                            let s_ext = s_old_arr
                                .join_core(&s_new_arr, |previous_iter, previous: &AbomonatedSubstitutions, new| {
                                    let mut previous_sub = previous.clone();
                                    let new_sub = new.clone();
                                    previous_sub.inner.extend(&new_sub.inner);

                                    Some(((previous_iter.0.0, previous_iter.0.1 + 1), previous_sub))
                                })
                                .consolidate();

                            let groundington = head
                                .enter(inner)
                                .join(&s_ext.map(|iter_sub| (iter_sub.0.0, iter_sub.1)))
                                .map(|(_left, (atom, sub))| attempt_to_rewrite(&sub, &atom))
                                .filter(|atom| is_ground(atom))
                                .map(|atom| (atom.symbol.clone(), atom))
                                .consolidate();

                            subs_product_var.set(&subs_product.enter(inner).concat(&s_ext));
                            data_var.set(&facts_by_symbol.enter(inner).concat(&groundington)).leave()
                        })
                        .consolidate()
                        .inspect_batch(move |_t, xs| {
                            for (atom, _time, diff) in xs {
                                local_fact_output_sink.send((atom.1.clone(), *diff)).unwrap()
                            }
                        });

                        (fact_input_session, fact_collection.probe())
                });
            loop {
                //if !rule_input_source.is_empty() || !fact_input_source.is_empty() {
                    // Rule
                    rule_input_source.try_iter().for_each(|triple| {
                        rule_input_session.update(triple.0, triple.1);
                    });

                    rule_input_session.advance_to(*rule_input_session.epoch() + 1);
                    rule_input_session.flush();

                    worker.step_or_park_while(Some(Duration::from_millis(50)), || {
                        rule_probe.less_than(rule_input_session.time())
                    });

                    // Data
                    fact_input_source.try_iter().for_each(|triple| {
                        fact_input_session.update(triple.0, triple.1);
                    });

                    fact_input_session.advance_to(*fact_input_session.epoch() + 1);
                    fact_input_session.flush();

                    worker.step_or_park_while(Some(Duration::from_millis(50)), || {
                        fact_probe.less_than(fact_input_session.time())
                    });
                //}
            }
        },
    )
    .unwrap();
}

pub struct DifferentialDatalog {
    pub fact_store: Instance,
    _ddflow: thread::JoinHandle<()>,
    pub rule_input_sink: RuleSink,
    pub rule_output_source: RuleSource,
    pub fact_input_sink: AtomSink,
    pub fact_output_source: AtomSource,
    pub interner: Interner,
    parallel: bool,
    intern: bool,
    materialization: Program,
}

impl Default for DifferentialDatalog {
    fn default() -> Self {
        let (rule_input_sink, rule_input_source) = unbounded();
        let (fact_input_sink, fact_input_source) = unbounded();

        let (rule_output_sink, rule_output_source) = unbounded();
        let (fact_output_sink, fact_output_source) = unbounded();

        let handle = thread::spawn(move || {
            reason(rule_input_source,
                   fact_input_source,
                   rule_output_sink,
                   fact_output_sink)
        });

        DifferentialDatalog {
            rule_input_sink,
            rule_output_source,
            fact_input_sink,
            fact_output_source,
            fact_store: Instance::new(),
            _ddflow: handle,
            interner: Default::default(),
            parallel: false,
            intern: false,
            materialization: vec![],
        }
    }
}

impl DifferentialDatalog {
    pub fn new(parallel: bool, intern: bool) -> Self {
        return Self {
            parallel,
            intern,
            ..Default::default()
        };
    }
}

impl DynamicTyped for DifferentialDatalog {
    fn insert_typed(&mut self, table: &str, row: Box<[TypedValue]>) {
        let mut typed_row = row.clone();
        if self.intern {
            typed_row = self.interner.intern_typed_values(typed_row);
        }
        let abomonated_row = typed_row
            .into_iter()
            .map(|typed_value| AbomonatedTerm::Constant(AbomonatedTypedValue::from(typed_value.clone())))
            .collect();
        let abomonated_atom = AbomonatedAtom {
                terms: abomonated_row,
                symbol: table.to_string(),
                sign: AbomonatedSign::Positive,
            };

        self.fact_input_sink.send((abomonated_atom, 1)).unwrap();
    }

    fn delete_typed(&mut self, table: &str, row: Box<[TypedValue]>) {
        let mut typed_row = row.clone();
        if self.intern {
            typed_row = self.interner.intern_typed_values(typed_row);
        }
        let abomonated_row = typed_row
            .into_iter()
            .map(|typed_value| AbomonatedTerm::Constant(AbomonatedTypedValue::from(typed_value.clone())))
            .collect();
        let abomonated_atom = AbomonatedAtom {
            terms: abomonated_row,
            symbol: table.to_string(),
            sign: AbomonatedSign::Positive,
        };

        self.fact_input_sink.send((abomonated_atom, -1)).unwrap();
    }
}

impl Materializer for DifferentialDatalog {
    fn materialize(&mut self, program: &Program) {
        program.iter().for_each(|rule| {
            let mut possibly_interned_rule = rule.clone();
            if self.intern {
                possibly_interned_rule = self.interner.intern_rule(&possibly_interned_rule);
            }
            self.materialization.push(possibly_interned_rule.clone());
            self.rule_input_sink.send((AbomonatedRule::from(possibly_interned_rule), 1)).unwrap();
        });

        while let Ok(fresh_intensional_atom) = self.fact_output_source.recv_timeout(Duration::from_millis(50)) {
            let boxed_vec = fresh_intensional_atom
                .0
                .terms
                .iter()
                .map(|abomonated_term| {
                    match abomonated_term {
                        AbomonatedTerm::Constant(inner) => {
                            inner.clone().into()
                        }
                        AbomonatedTerm::Variable(_) => unreachable!()
                    }
                })
                .collect();

            self.fact_store.insert_typed(&fresh_intensional_atom.0.symbol, boxed_vec);
        }
    }

    fn update(&mut self, changes: Vec<Diff>) {
        changes.iter().for_each(|(sign, (sym, value))| {
            let typed_row: Box<[TypedValue]> = value
                .iter()
                .map(|dyn_type| dyn_type.to_typed_value())
                .collect();

            if *sign {
                self.insert_typed(sym.clone(), typed_row);
            } else {
                self.delete_typed(sym.clone(), typed_row);
            }
        });

        while let Ok(fresh_intensional_atom) = self.fact_output_source.recv_timeout(Duration::from_millis(100)) {
            let boxed_vec = fresh_intensional_atom
                .0
                .terms
                .iter()
                .map(|abomonated_term| {
                    match abomonated_term {
                        AbomonatedTerm::Constant(inner) => {
                            inner.clone().into()
                        }
                        AbomonatedTerm::Variable(_) => unreachable!()
                    }
                })
                .collect();

            if fresh_intensional_atom.1 > 0 {
                self.fact_store.insert_typed(&fresh_intensional_atom.0.symbol, boxed_vec)
            } else {
                self.fact_store.delete_typed(&fresh_intensional_atom.0.symbol, boxed_vec)
            }
        }
    }

    fn triple_count(&self) -> usize {
        return self
            .fact_store
            .database
            .iter()
            .map(|(_sym, rel)| return rel.len())
            .sum();
    }
}
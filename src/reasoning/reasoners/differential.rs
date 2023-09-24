mod abomonated_model;
mod abomonated_vertebra;

use crate::misc::string_interning::Interner;
use crate::models::datalog::{Program, SugaredProgram, TypedValue};
use ahash::{AHasher, HashSet};
use crossbeam_channel::{select, unbounded, Receiver, Sender};
use differential_dataflow::algorithms::identifiers::Identifiers;
use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
use differential_dataflow::operators::{iterate, Consolidate, Join, JoinCore, Threshold};
use differential_dataflow::Collection;
use std::clone::Clone;
use std::hash::{Hash, Hasher};
use std::num::NonZeroU32;
use std::thread;
use std::time::{Duration, Instant};

use crate::models::instance::{Database, HashSetDatabase};
use crate::models::reasoner::{Diff, DynamicTyped, Materializer};
use crate::models::relational_algebra::Row;
use crate::reasoning::reasoners::differential::abomonated_model::{
    abomonate_rule, borrowing_mask, mask, permute_mask, AbomonatedAtom, AbomonatedRule,
    AbomonatedTerm, AbomonatedTypedValue, BorrowingMaskedAtom, MaskedAtom,
};
use crate::reasoning::reasoners::differential::abomonated_vertebra::AbomonatedSubstitutions;
use colored::Colorize;
use timely::communication::allocator::Generic;
use timely::dataflow::scopes::Child;
use timely::dataflow::Scope;
use timely::order::Product;
use timely::worker::Worker;

pub type AtomCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, AbomonatedAtom>;
pub type SubstitutionsCollection<'b> =
    Collection<Child<'b, Worker<Generic>, usize>, AbomonatedSubstitutions>;

pub type RuleSink = Sender<(AbomonatedRule, usize, isize)>;
pub type AtomSink = Sender<(AbomonatedAtom, usize, isize)>;

pub type RuleSource = Receiver<(AbomonatedRule, usize, isize)>;
pub type AtomSource = Receiver<(AbomonatedAtom, usize, isize)>;

pub type NotificationSink = Sender<usize>;
pub type NotificationSource = Receiver<usize>;

fn unify(left: &AbomonatedAtom, right: &AbomonatedAtom) -> Option<AbomonatedSubstitutions> {
    let mut substitution: AbomonatedSubstitutions = Default::default();

    let left_and_right = left.2.iter().zip(right.2.iter());

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
                    substitution
                        .inner
                        .insert((left_variable.clone(), right_constant.clone()));
                }
            }
            _ => {}
        }
    }

    return Some(substitution);
}

// [?x -> 1, ?y -> 3], (?x, 3) = (1, 3)

fn attempt_to_rewrite(rewrite: &AbomonatedSubstitutions, atom: &AbomonatedAtom) -> AbomonatedAtom {
    let terms = atom
        .2
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
        .collect();

    return (atom.0.clone(), atom.1, terms);
}

fn is_ground(atom: &AbomonatedAtom) -> bool {
    for term in atom.2.iter() {
        match term {
            AbomonatedTerm::Variable(_) => {
                return false;
            }
            _ => {}
        };
    }
    return true;
}

pub fn reason_arranged_by_relation(
    cores: usize,
    parallel: bool,
    rule_input_source: RuleSource,
    fact_input_source: AtomSource,
    rule_output_sink: RuleSink,
    fact_output_sink: AtomSink,
    notification_sink: NotificationSink,
) {
    timely::execute(
        if parallel { timely::Config::process(cores) } else { timely::Config::thread() },
        move |worker: &mut Worker<Generic>| {
            let local_notification_sink = notification_sink.clone();
            let (mut rule_input_session, mut rule_trace, rule_probe) = worker
                .dataflow_named::<usize, _, _>("rule_ingestion", |local| {
                    let local_rule_output_sink = rule_output_sink.clone();

                    let (rule_input, rule_collection) = local.new_collection::<AbomonatedRule, isize>();
                    rule_collection.inspect(move |x| {
                        local_rule_output_sink.send((x.0.clone(), x.1, x.2)).unwrap();
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

                    let rule_collection = rule_trace
                        .import(local)
                        .as_collection(|x, _y| x.clone());

                    let facts_by_symbol = fact_collection
                        .map(|ground_fact| (ground_fact.0, ground_fact));

                    let indexed_rules = rule_collection.identifiers();
                    let goals = indexed_rules
                        .flat_map(|(rule, rule_id)| {
                            rule
                                .1
                                .into_iter()
                                .enumerate()
                                .map(move |(atom_id, atom)| {
                                    ((rule_id, atom_id), atom)
                                })
                        });

                    let heads = indexed_rules
                        .map(|rule_and_id| (rule_and_id.1, rule_and_id.0.0));

                    let subs_product = heads
                        .map(|(rule_id, _head)| ((rule_id, 0), AbomonatedSubstitutions::default()));

                    let output = local
                        .iterative::<usize, _, _>(|inner| {
                            let subs_product_var = iterate::Variable::new_from(subs_product.enter(inner), Product::new(Default::default(), 1));
                            let facts_var = iterate::Variable::new_from(facts_by_symbol.enter(inner), Product::new(Default::default(), 1));

                            let g = goals.enter(inner);

                            let s_old_arr = subs_product_var.arrange_by_key();
                            let facts = facts_var.distinct();
                            let facts_by_symbol_arr = facts.arrange_by_key();

                            let goal_x_subs = g
                                .arrange_by_key()
                                .join_core(&s_old_arr, |key, goal, sub| {
                                    let rewrite_attempt = &attempt_to_rewrite(sub, goal);
                                    if !is_ground(rewrite_attempt) {
                                        return Some((goal.0, (key.clone(), rewrite_attempt.clone(), sub.clone())));
                                    }
                                    return None;
                                });

                            let current_goals = goal_x_subs
                                .arrange_by_key();

                            let new_substitutions = facts_by_symbol_arr.join_core(&current_goals, |_interned_symbol, ground_fact: &AbomonatedAtom, (new_key, rewrite_attempt, old_sub)| {
                                    let ground_terms = ground_fact
                                        .2
                                        .iter()
                                        .map(|row_element| AbomonatedTerm::Constant(AbomonatedTypedValue::try_from(row_element.clone()).unwrap()))
                                        .collect();

                                    let proposed_atom = (rewrite_attempt.0.clone(), rewrite_attempt.1, ground_terms);

                                    let sub = unify(
                                        &rewrite_attempt,
                                        &proposed_atom,
                                    );

                                    match sub {
                                        None => {
                                            None
                                        }
                                        Some(sub) => {
                                            let (previous_iter, new_sub) = ((new_key, old_sub.clone()), sub);
                                            let (iter, mut previous_sub) = previous_iter;
                                            previous_sub.inner.extend(&new_sub.inner);

                                            Some(((iter.0, iter.1 + 1), previous_sub))
                                        }
                                    }
                                });

                            let groundington = heads
                                .enter(inner)
                                .join(&new_substitutions.map(|iter_sub| (iter_sub.0.0, iter_sub.1)))
                                .map(|(_left, (atom, sub))| attempt_to_rewrite(&sub, &atom))
                                .filter(|atom| is_ground(atom))
                                .map(|atom| (atom.0, atom))
                                .consolidate();

                            subs_product_var.set(&subs_product.enter(inner).concat(&new_substitutions));
                            facts_var.set(&facts_by_symbol.enter(inner).concat(&groundington)).leave()
                        })
                        .consolidate()
                        .inspect_batch(move |_t, xs| {
                            for (atom, time, diff) in xs {
                                local_fact_output_sink.send((atom.1.clone(), *time, *diff)).unwrap()
                            }
                        });

                    (fact_input_session, output.probe())
                });
            if worker.index() == 0 {
                let mut fact_input_source = fact_input_source.iter().peekable();
                let mut rule_input_source = rule_input_source.iter().peekable();
                let mut fact_epoch = 0;
                let mut rule_epoch = 0;
                loop {
                    while let Some(fact) = rule_input_source.next_if(|cond| {
                        cond.1 == rule_epoch
                    }) {
                        rule_input_session.update(fact.0, fact.2);
                    }

                    if let Some((_atom, time, _diff)) = rule_input_source.peek() {
                        if rule_epoch < *time {
                            rule_epoch = *time;
                            rule_input_session.advance_to(rule_epoch.join(&fact_epoch));
                        }
                    }
                    rule_input_session.flush();

                    while let Some(fact) = fact_input_source.next_if(|cond| {
                        cond.1 == fact_epoch
                    }) {
                        fact_input_session.update(fact.0, fact.2);
                    }

                    if let Some((_atom, time, _diff)) = fact_input_source.peek() {
                        if fact_epoch < *time {
                            fact_epoch = *time;
                            fact_input_session.advance_to(fact_epoch.join(&rule_epoch));
                        }
                    }
                    fact_input_session.flush();

                    rule_input_session.advance_to(rule_epoch.join(&fact_epoch));
                    rule_input_session.flush();
                    worker.step_or_park_while(Some(Duration::from_millis(1)), || {
                        fact_probe.less_than(&fact_epoch.join(&rule_epoch)) || rule_probe.less_than(&rule_epoch.join(&fact_epoch))
                    });

                    if let Err(e) = local_notification_sink.send(fact_epoch.join(&rule_epoch)) {
                        println!("Warning: {}", e);
                        break;
                    };
                }
            }
        },
    ).unwrap();
}

pub fn unique_column_combinations(rule: AbomonatedRule) -> Vec<(NonZeroU32, Vec<usize>)> {
    let mut out = vec![];
    let mut variables: HashSet<u8> = Default::default();
    let mut fresh_variables: HashSet<u8> = Default::default();
    for body_atom in rule.1 {
        let index: Vec<_> = body_atom
            .2
            .iter()
            .enumerate()
            .flat_map(|(idx, term)| match term {
                AbomonatedTerm::Variable(inner) => {
                    if !variables.contains(inner) {
                        fresh_variables.insert(inner.clone());
                        None
                    } else {
                        Some(idx)
                    }
                }
                AbomonatedTerm::Constant(_) => Some(idx),
            })
            .collect();
        variables.extend(fresh_variables.iter());
        out.push((body_atom.0, index));

        fresh_variables.clear();
    }

    return out;
}

pub fn reason_with_masked_atoms(
    cores: usize,
    parallel: bool,
    rule_input_source: RuleSource,
    fact_input_source: AtomSource,
    rule_output_sink: RuleSink,
    fact_output_sink: AtomSink,
    notification_sink: NotificationSink,
) -> () {
    timely::execute(
        if parallel {
            timely::Config::process(cores)
        } else {
            timely::Config::thread()
        },
        move |worker: &mut Worker<Generic>| {
            let local_notification_sink = notification_sink.clone();
            let (mut rule_input_session, mut rule_trace, rule_probe) = worker
                .dataflow_named::<usize, _, _>("rule_ingestion", |local| {
                    let local_rule_output_sink = rule_output_sink.clone();

                    let (rule_input, rule_collection) =
                        local.new_collection::<AbomonatedRule, isize>();
                    rule_collection.inspect(move |x| {
                        local_rule_output_sink
                            .send((x.0.clone(), x.1, x.2))
                            .unwrap();
                    });

                    (
                        rule_input,
                        rule_collection.arrange_by_self().trace,
                        rule_collection.probe(),
                    )
                });

            let (mut fact_input_session, fact_probe) =
                worker.dataflow_named::<usize, _, _>("fact_ingestion_and_reasoning", |local| {
                    let (fact_input_session, fact_collection) =
                        local.new_collection::<AbomonatedAtom, isize>();

                    let local_fact_output_sink = fact_output_sink.clone();

                    let rule_collection = rule_trace.import(local).as_collection(|x, _y| x.clone());

                    // (relation_id, [positions])
                    let unique_column_combinations = rule_collection
                        .flat_map(unique_column_combinations)
                        .distinct()
                        .arrange_by_key();

                    let facts_by_relation_id = fact_collection
                        .map(|(relation_id, sign, terms)| (relation_id, (sign, terms)));

                    let facts_by_masked = unique_column_combinations.join_core(
                        &facts_by_relation_id.arrange_by_key(),
                        |&key, column_combination, right| {
                            let mut projected_row = vec![None; right.1.len()];
                            if !(column_combination.len() == 0) {
                                column_combination.iter().for_each(|column_position| {
                                    if let AbomonatedTerm::Constant(ref inner) =
                                        right.1[*column_position]
                                    {
                                        projected_row[*column_position] = Some(inner)
                                    }
                                });
                            }
                            let masked_projected_row: BorrowingMaskedAtom = (key, projected_row);

                            Some(hashisher((
                                masked_projected_row,
                                (key, right.0, right.1.clone()),
                            )))
                        },
                    );

                    let indexed_rules = rule_collection.identifiers();
                    let goals = indexed_rules.flat_map(|(rule, rule_id)| {
                        rule.1
                            .into_iter()
                            .enumerate()
                            .map(move |(atom_id, atom)| ((rule_id, atom_id), atom))
                    });

                    // (rule_identifier, (abomonated_atom, rule_body_length))
                    let heads = indexed_rules.map(|rule_and_id| {
                        (rule_and_id.1, (rule_and_id.0 .0, rule_and_id.0 .1.len()))
                    });

                    let heads_x_ucc = heads
                        .map(|(rule_identifier, (abo_atom, rule_body_length))| {
                            (abo_atom.0, (rule_identifier, (abo_atom, rule_body_length)))
                        })
                        .join_core(
                            &unique_column_combinations,
                            |&key, heads_contents, positions| {
                                Some((
                                    (heads_contents.0, heads_contents.1 .1),
                                    (heads_contents.1 .0.clone(), positions.clone()),
                                ))
                            },
                        );

                    let subs_product = heads
                        .map(|(rule_id, _head)| ((rule_id, 0), AbomonatedSubstitutions::default()));

                    let output = local
                        .iterative::<usize, _, _>(|inner| {
                            let subs_product_var = iterate::Variable::new_from(
                                subs_product.enter(inner),
                                Product::new(Default::default(), 1),
                            );
                            let facts_var = iterate::Variable::new_from(
                                facts_by_masked.enter(inner),
                                Product::new(Default::default(), 1),
                            );

                            let g = goals.enter(inner);

                            let s_old_arr = subs_product_var.arrange_by_key();
                            let facts =
                                facts_var.map(|(_, atom)| return hashisher((atom.clone(), atom)));

                            let hashed_facts_by_hashed_masked = facts_var
                                .map(|(hashed_masked_atom, atom)| {
                                    let hashished = hashisher((atom, hashed_masked_atom));

                                    return (hashished.1, hashished.0);
                                })
                                .arrange_by_key();

                            let facts_by_masked_arr = facts.arrange_by_key();

                            let goal_x_subs =
                                g.arrange_by_key().join_core(&s_old_arr, |key, goal, sub| {
                                    let rewrite_attempt = attempt_to_rewrite(sub, goal);
                                    let new_key = (key.clone(), sub.clone());

                                    Some((
                                        hashisher((borrowing_mask(&rewrite_attempt), ())).0,
                                        (new_key, rewrite_attempt),
                                    ))
                                });

                            let current_goals = goal_x_subs
                                .arrange_by_key()
                                .join_core(
                                    &hashed_facts_by_hashed_masked,
                                    |_hashed_masked_atom, left, hashed_atom| {
                                        return Some((*hashed_atom, left.clone()));
                                    },
                                )
                                .arrange_by_key();

                            let new_substitutions = facts_by_masked_arr.join_core(
                                &current_goals,
                                |_hashed_masked_atom,
                                 ground_fact: &AbomonatedAtom,
                                 (new_key, rewrite_attempt)| {
                                    let ground_terms = ground_fact
                                        .2
                                        .iter()
                                        .map(|row_element| {
                                            AbomonatedTerm::Constant(
                                                AbomonatedTypedValue::try_from(row_element.clone())
                                                    .unwrap(),
                                            )
                                        })
                                        .collect();

                                    let proposed_atom = (
                                        rewrite_attempt.0.clone(),
                                        rewrite_attempt.1,
                                        ground_terms,
                                    );

                                    let sub = unify(&rewrite_attempt, &proposed_atom);

                                    match sub {
                                        None => None,
                                        Some(sub) => {
                                            let (previous_iter, new) =
                                                ((new_key.0, new_key.1.clone()), sub);
                                            let (_iter, previous) = previous_iter;
                                            let mut previous_sub = previous;
                                            let new_sub = new;
                                            previous_sub.inner.extend(&new_sub.inner);

                                            Some((
                                                (previous_iter.0 .0, previous_iter.0 .1 + 1),
                                                previous_sub,
                                            ))
                                        }
                                    }
                                },
                            );

                            let groundington = heads_x_ucc.enter(inner).join_core(
                                &new_substitutions.arrange_by_key(),
                                |&key, (head_atom, positions), fresh_subs| {
                                    let attempt = attempt_to_rewrite(fresh_subs, head_atom);
                                    if !is_ground(&attempt) {
                                        return None;
                                    }

                                    let mut projected_row = vec![None; attempt.2.len()];
                                    if !(positions.len() == 0) {
                                        positions.iter().for_each(|column_position| {
                                            // TODO we will delete the clone
                                            if let AbomonatedTerm::Constant(inner) =
                                                attempt.2[*column_position].clone()
                                            {
                                                projected_row[*column_position] = Some(inner)
                                            }
                                        });
                                    }
                                    let masked_projected_row: MaskedAtom =
                                        (attempt.0, projected_row);

                                    Some(hashisher((masked_projected_row, attempt)))
                                },
                            );
                            let groundington = groundington.distinct();

                            subs_product_var.set_concat(&new_substitutions);
                            facts_var.set_concat(&groundington).leave()
                        })
                        .map(move |(mask, atom)| {atom})
                        .concat(&fact_collection)
                        .consolidate()
                        .inspect_batch(move |_t, xs| {
                            for (atom, time, diff) in xs {
                                local_fact_output_sink
                                    .send((atom.clone(), *time, *diff))
                                    .unwrap()
                            }
                        });

                    (fact_input_session, output.probe())
                });
            if worker.index() == 0 {
                let mut fact_input_source = fact_input_source.iter().peekable();
                let mut rule_input_source = rule_input_source.iter().peekable();
                let mut fact_epoch = 0;
                let mut rule_epoch = 0;
                loop {
                    while let Some(fact) = rule_input_source.next_if(|cond| cond.1 == rule_epoch) {
                        rule_input_session.update(fact.0, fact.2);
                    }

                    if let Some((_atom, time, _diff)) = rule_input_source.peek() {
                        if rule_epoch < *time {
                            rule_epoch = *time;
                            rule_input_session.advance_to(rule_epoch.join(&fact_epoch));
                        }
                    }
                    rule_input_session.flush();

                    while let Some(fact) = fact_input_source.next_if(|cond| cond.1 == fact_epoch) {
                        fact_input_session.update(fact.0, fact.2);
                    }

                    if let Some((_atom, time, _diff)) = fact_input_source.peek() {
                        if fact_epoch < *time {
                            fact_epoch = *time;
                            fact_input_session.advance_to(fact_epoch.join(&rule_epoch));
                        }
                    }
                    fact_input_session.flush();

                    rule_input_session.advance_to(rule_epoch.join(&fact_epoch));
                    rule_input_session.flush();
                    worker.step_or_park_while(Some(Duration::from_millis(1)), || {
                        fact_probe.less_than(&fact_epoch.join(&rule_epoch))
                            || rule_probe.less_than(&rule_epoch.join(&fact_epoch))
                    });

                    if let Err(e) = local_notification_sink.send(fact_epoch.join(&rule_epoch)) {
                        println!("Warning: {}", e);
                        break;
                    };
                }
            }
        },
    )
    .unwrap();
}

fn hashisher<K: Hash, T>((key, value): (K, T)) -> (u32, T) {
    let mut hasher = AHasher::default();
    key.hash(&mut hasher);
    let hashed_key = hasher.finish();
    (hashed_key as u32, value)
}

pub struct DifferentialDatalog {
    epoch: usize,
    pub fact_store: HashSetDatabase,
    _ddflow: thread::JoinHandle<()>,
    pub rule_input_sink: RuleSink,
    pub rule_output_source: RuleSource,
    pub fact_input_sink: AtomSink,
    pub fact_output_source: AtomSource,
    pub notification_source: NotificationSource,
    pub interner: Interner,
    materialization: Program,
}

impl Default for DifferentialDatalog {
    fn default() -> Self {
        let (rule_input_sink, rule_input_source) = unbounded();
        let (fact_input_sink, fact_input_source) = unbounded();

        let (rule_output_sink, rule_output_source) = unbounded();
        let (fact_output_sink, fact_output_source) = unbounded();

        let cores = thread::available_parallelism().unwrap().get();
        let (notification_sink, notification_source) = unbounded();

        let handle = thread::spawn(move || {
            reason_arranged_by_relation(
                cores,
                true,
                rule_input_source,
                fact_input_source,
                rule_output_sink,
                fact_output_sink,
                notification_sink,
            )
        });

        DifferentialDatalog {
            epoch: 0,
            rule_input_sink,
            rule_output_source,
            fact_input_sink,
            fact_output_source,
            notification_source,
            _ddflow: handle,
            fact_store: Default::default(),
            interner: Default::default(),
            materialization: vec![],
        }
    }
}

fn typed_row_to_abomonated_row(typed_row: Row, interner: &mut Interner) -> Vec<AbomonatedTerm> {
    let typed_row = interner.intern_row(typed_row);

    return typed_row
        .into_iter()
        .map(|typed_value| {
            AbomonatedTerm::Constant(AbomonatedTypedValue::from(typed_value.clone()))
        })
        .collect();
}

impl DifferentialDatalog {
    pub fn new(parallel: bool, index: bool) -> Self {
        let (rule_input_sink, rule_input_source) = unbounded();
        let (fact_input_sink, fact_input_source) = unbounded();

        let (rule_output_sink, rule_output_source) = unbounded();
        let (fact_output_sink, fact_output_source) = unbounded();

        let cores = thread::available_parallelism().unwrap().get();
        let (notification_sink, notification_source) = unbounded();

        let handle = thread::spawn(move || {
            if !index {
                reason_arranged_by_relation(
                    cores,
                    parallel,
                    rule_input_source,
                    fact_input_source,
                    rule_output_sink,
                    fact_output_sink,
                    notification_sink,
                )
            } else {
                reason_with_masked_atoms(
                    cores,
                    parallel,
                    rule_input_source,
                    fact_input_source,
                    rule_output_sink,
                    fact_output_sink,
                    notification_sink,
                )
            }
        });

        DifferentialDatalog {
            epoch: 0,
            rule_input_sink,
            rule_output_source,
            fact_input_sink,
            fact_output_source,
            notification_source,
            _ddflow: handle,
            ..Default::default()
        }
    }
    fn noop_typed(&mut self, table: &str, row: Row) {
        let abomonated_atom = (
            self.interner.rodeo.get_or_intern(table).into_inner(),
            true,
            typed_row_to_abomonated_row(row, &mut self.interner),
        );

        self.fact_input_sink
            .send((abomonated_atom, self.epoch, 0))
            .unwrap();
    }
}

impl DynamicTyped for DifferentialDatalog {
    fn insert_typed(&mut self, table: &str, row: Row) {
        let abomonated_atom = (
            self.interner.rodeo.get_or_intern(table).into_inner(),
            true,
            typed_row_to_abomonated_row(row, &mut self.interner),
        );

        self.fact_input_sink
            .send((abomonated_atom, self.epoch, 1))
            .unwrap();
    }

    fn delete_typed(&mut self, table: &str, row: &Row) {
        let abomonated_atom = (
            self.interner.rodeo.get_or_intern(table).into_inner(),
            true,
            typed_row_to_abomonated_row(row.clone(), &mut self.interner),
        );

        self.fact_input_sink
            .send((abomonated_atom, self.epoch, -1))
            .unwrap();
    }
}

const NOOP_DUMMY_LHS: &'static str = "NOOP";
const NOOP_DUMMY_RHS: &'static str = "SKIP";

fn insert_atom_with_diff(
    fresh_intensional_atom: AbomonatedAtom,
    multiplicity: isize,
    instance: &mut HashSetDatabase,
) {
    let boxed_vec = fresh_intensional_atom
        .2
        .into_iter()
        .map(|abomonated_term| match abomonated_term {
            AbomonatedTerm::Constant(inner) => inner.into(),
            AbomonatedTerm::Variable(_) => unreachable!(),
        })
        .collect();

    if multiplicity > 0 {
        instance.insert_at(fresh_intensional_atom.0.get(), boxed_vec)
    } else {
        instance.delete_at(fresh_intensional_atom.0.get(), &boxed_vec)
    }
}

impl Materializer for DifferentialDatalog {
    fn materialize(&mut self, program: &SugaredProgram) {
        program.iter().for_each(|rule| {
            let interned_rule = self.interner.intern_rule(rule);

            self.materialization.push(interned_rule.clone());
            self.rule_input_sink
                .send((abomonate_rule(interned_rule), self.epoch, 1))
                .unwrap();
        });
        self.epoch += 1;
        let noop_rule: AbomonatedRule = (
            (
                self.interner
                    .rodeo
                    .get_or_intern(NOOP_DUMMY_LHS.to_string())
                    .into_inner(),
                true,
                vec![AbomonatedTerm::Variable(0)],
            ),
            vec![(
                self.interner
                    .rodeo
                    .get_or_intern(NOOP_DUMMY_RHS.to_string())
                    .into_inner(),
                true,
                vec![AbomonatedTerm::Variable(0)],
            )],
        );
        self.rule_input_sink
            .send((noop_rule, self.epoch, 0))
            .unwrap();

        self.update(vec![]);
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
                self.delete_typed(sym.clone(), &typed_row);
            }
        });
        self.epoch += 1;
        let noop_row = vec![TypedValue::Bool(false)].into_boxed_slice();
        self.noop_typed("noop", noop_row);

        let noop_rule: AbomonatedRule = (
            (
                self.interner
                    .rodeo
                    .get_or_intern(NOOP_DUMMY_LHS.to_string())
                    .into_inner(),
                true,
                vec![AbomonatedTerm::Variable(0)],
            ),
            vec![(
                self.interner
                    .rodeo
                    .get_or_intern(NOOP_DUMMY_RHS.to_string())
                    .into_inner(),
                true,
                vec![AbomonatedTerm::Variable(0)],
            )],
        );
        self.rule_input_sink
            .send((noop_rule, self.epoch, 0))
            .unwrap();

        let now = Instant::now();
        loop {
            select! {
                recv(self.notification_source) -> last_epoch => {
                    let last_epoch_uw = last_epoch.unwrap();

                    if last_epoch_uw == self.epoch {
                        self
                        .fact_output_source
                        .try_iter()
                        .for_each(|fresh_intensional_atom| {
                            insert_atom_with_diff(fresh_intensional_atom.0, fresh_intensional_atom.2, &mut self.fact_store)
                        });

                        println!(
                            "{{{}: {}}}",
                            "inferencetime",
                            now.elapsed().as_millis().to_string()
                        );

                        return;
                    }
                }
                recv(self.fact_output_source) -> fact => {
                    let fresh_intensional_atom = fact.unwrap();

                    insert_atom_with_diff(fresh_intensional_atom.0, fresh_intensional_atom.2, &mut self.fact_store)
                },
            }
        }
    }

    fn triple_count(&self) -> usize {
        return self
            .fact_store
            .storage
            .iter()
            .map(|(_sym, rel)| return rel.len())
            .sum();
    }

    fn dump(&self) {
        todo!()
    }
}

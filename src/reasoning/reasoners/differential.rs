mod abomonated_model;
mod abomonated_vertebra;

use std::clone::Clone;
use std::thread;
use crate::misc::string_interning::Interner;
use crate::models::datalog::{Program, Term, TypedValue};
use crate::models::instance::{Instance};
use crossbeam_channel::{Receiver, select, Sender, unbounded};
use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use std::time::{Duration};
use differential_dataflow::algorithms::identifiers::Identifiers;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
use differential_dataflow::operators::{Consolidate, iterate, Join, JoinCore, Threshold};

use timely::communication::allocator::Generic;
use timely::dataflow::Scope;
use timely::dataflow::scopes::Child;
use timely::order::Product;
use timely::worker::Worker;
use crate::models::reasoner::{Diff, DynamicTyped, Materializer};
use crate::models::relational_algebra::Row;
use crate::reasoning::reasoners::differential::abomonated_model::{AbomonatedAtom, AbomonatedRule, AbomonatedTerm, AbomonatedTypedValue};
use crate::reasoning::reasoners::differential::abomonated_vertebra::{AbomonatedSubstitutions};

pub type AtomCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, AbomonatedAtom>;
pub type SubstitutionsCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, AbomonatedSubstitutions>;

pub type RuleSink = Sender<(AbomonatedRule, usize, isize)>;
pub type AtomSink = Sender<(AbomonatedAtom, usize, isize)>;

pub type RuleSource = Receiver<(AbomonatedRule, usize, isize)>;
pub type AtomSource = Receiver<(AbomonatedAtom, usize, isize)>;

pub type NotificationSink = Sender<usize>;
pub type NotificationSource = Receiver<usize>;

fn make_substitutions(left: &AbomonatedAtom, right: &AbomonatedAtom) -> Option<AbomonatedSubstitutions> {
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
                    substitution.inner.insert((left_variable.clone(), right_constant.clone()));
                }
            }
            _ => {}
        }
    }

    return Some(substitution);
}

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

pub fn reason(
    rule_input_source: RuleSource,
    fact_input_source: AtomSource,
    rule_output_sink: RuleSink,
    fact_output_sink: AtomSink,
    notification_sink: NotificationSink,
) -> () {
    timely::execute(
        timely::Config::process(8),
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
                    //let rule_collection = rule_trace.import(local).as_collection(|x, _y| x.clone());
                    let rule_collection = rule_trace
                        .import(local)
                        .as_collection(|x, _y| x.clone());

                    let facts_by_symbol = fact_collection
                        .map(|ground_fact| (ground_fact.0.clone(), ground_fact));

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

                    let head = indexed_rules
                        .map(|rule_and_id| (rule_and_id.1, rule_and_id.0.0));

                    let subs_product = head
                        .map(|(rule_id, _head)| ((rule_id, 0), AbomonatedSubstitutions::default()));

                    let output = local
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
                                        return Some((goal.0.clone(), (new_key, rewrite_attempt.clone())));
                                    }
                                    return None;
                                });

                            let current_goals = goal_x_subs
                                .arrange_by_key();

                            let new_substitutions = data_arr
                                .join_core(&current_goals, |_sym, ground_fact, (new_key, rewrite_attempt)| {
                                    let ground_terms = ground_fact
                                        .clone()
                                        .2
                                        .into_iter()
                                        .map(|row_element| AbomonatedTerm::Constant(AbomonatedTypedValue::try_from(row_element.clone()).unwrap()))
                                        .collect();

                                    let proposed_atom = (rewrite_attempt.0.clone(), rewrite_attempt.1, ground_terms);

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
                                .map(|atom| (atom.0.clone(), atom))
                                .consolidate();

                            subs_product_var.set(&subs_product.enter(inner).concat(&s_ext));
                            data_var.set(&facts_by_symbol.enter(inner).concat(&groundington)).leave()
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
                    // timeout not needed when channel blocking iterator is used (on both channels!)
                    worker.step_or_park_while(Some(Duration::from_millis(1)), || {
                        fact_probe.less_than(&fact_epoch.join(&rule_epoch)) || rule_probe.less_than(&rule_epoch.join(&fact_epoch))
                    });

                    local_notification_sink.send(fact_epoch.join(&rule_epoch)).unwrap();
                }
            }
        },
    )
    .unwrap();
}

pub struct DifferentialDatalog {
    epoch: usize,
    pub fact_store: Instance,
    _ddflow: thread::JoinHandle<()>,
    pub rule_input_sink: RuleSink,
    pub rule_output_source: RuleSource,
    pub fact_input_sink: AtomSink,
    pub fact_output_source: AtomSource,
    pub notification_source: NotificationSource,
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

        let (notification_sink, notification_source) = unbounded();

        let handle = thread::spawn(move || {
            reason(rule_input_source,
                   fact_input_source,
                   rule_output_sink,
                   fact_output_sink,
                   notification_sink)
        });

        DifferentialDatalog {
            epoch: 0,
            rule_input_sink,
            rule_output_source,
            fact_input_sink,
            fact_output_source,
            notification_source,
            fact_store: Instance::new(),
            _ddflow: handle,
            interner: Default::default(),
            parallel: false,
            intern: false,
            materialization: vec![],
        }
    }
}

fn typed_row_to_abomonated_row(typed_row: Row, intern: bool, interner: &mut Interner) -> Vec<AbomonatedTerm> {
    let mut typed_row = typed_row.clone();
    if intern {
        typed_row = interner.intern_typed_values(typed_row);
    }

    return typed_row
        .into_iter()
        .map(|typed_value| AbomonatedTerm::Constant(AbomonatedTypedValue::from(typed_value.clone())))
        .collect();
}

impl DifferentialDatalog {
    pub fn new(parallel: bool, intern: bool) -> Self {
        return Self {
            parallel,
            intern,
            ..Default::default()
        };
    }
    fn noop_typed(&mut self, table: &str, row: Box<[TypedValue]>) {
        let abomonated_atom = (table.to_string(), true, typed_row_to_abomonated_row(row, self.intern, &mut self.interner));

        self.fact_input_sink.send((abomonated_atom, self.epoch, 0)).unwrap();
    }
}

impl DynamicTyped for DifferentialDatalog {
    fn insert_typed(&mut self, table: &str, row: Box<[TypedValue]>) {
        let abomonated_atom = (table.to_string(), true, typed_row_to_abomonated_row(row, self.intern, &mut self.interner));

        self.fact_input_sink.send((abomonated_atom, self.epoch, 1)).unwrap();
    }

    fn delete_typed(&mut self, table: &str, row: Box<[TypedValue]>) {
        let abomonated_atom = (table.to_string(), true, typed_row_to_abomonated_row(row, self.intern, &mut self.interner));

        self.fact_input_sink.send((abomonated_atom, self.epoch, -1)).unwrap();
    }
}

const NOOP_DUMMY_LHS: &'static str = "NOOP";
const NOOP_DUMMY_RHS: &'static str = "SKIP";

fn insert_atom_with_diff(fresh_intensional_atom: AbomonatedAtom, multiplicity: isize, instance: &mut Instance) {
    let boxed_vec = fresh_intensional_atom
        .2
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

    if multiplicity > 0 {
        instance.insert_typed(&fresh_intensional_atom.0, boxed_vec)
    } else {
        instance.delete_typed(&fresh_intensional_atom.0, boxed_vec)
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
            self.rule_input_sink.send((AbomonatedRule::from(possibly_interned_rule), self.epoch, 1)).unwrap();
        });
        self.epoch += 1;
        let noop_rule: AbomonatedRule =  (
            (NOOP_DUMMY_LHS.to_string(), true, vec![AbomonatedTerm::Variable(0)]),
            vec![
                (NOOP_DUMMY_RHS.to_string(), true, vec![AbomonatedTerm::Variable(0)])
            ]
        );
        self.rule_input_sink.send((noop_rule, self.epoch, 0)).unwrap();

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
                self.delete_typed(sym.clone(), typed_row);
            }
        });
        self.epoch += 1;
        let noop_row = vec![TypedValue::Bool(false)].into_boxed_slice();
        self.noop_typed("noop", noop_row);

        let noop_rule: AbomonatedRule =  (
            (NOOP_DUMMY_LHS.to_string(), true, vec![AbomonatedTerm::Variable(0)]),
            vec![
                (NOOP_DUMMY_RHS.to_string(), true, vec![AbomonatedTerm::Variable(0)])
            ]
        );
        self.rule_input_sink.send((noop_rule, self.epoch, 0)).unwrap();

        loop {
            select! {
                recv(self.notification_source) -> last_epoch => {
                    let last_epoch_uw = last_epoch.unwrap();

                    if last_epoch_uw == self.epoch {
                        self
                        .fact_output_source
                        .try_iter()
                        .for_each(|fresh_intensional_atom| insert_atom_with_diff(fresh_intensional_atom.0, fresh_intensional_atom.2, &mut self.fact_store));

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
            .database
            .iter()
            .map(|(_sym, rel)| return rel.len())
            .sum();
    }
}
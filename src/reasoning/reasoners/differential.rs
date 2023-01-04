mod abomonated_model;
mod abomonated_parsing;
mod abomonated_vertebra;

use std::thread;
use crate::misc::string_interning::Interner;
use crate::models::datalog::{Atom, Program, Rule, Sign, Term};
use crate::models::index::ValueRowId;
use crate::models::instance::Instance;
use crossbeam_channel::{Receiver, Sender, unbounded};
use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use std::time::{Duration};
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::Threshold;
use timely::communication::allocator::Generic;
use timely::dataflow::scopes::Child;
use timely::worker::Worker;
use crate::models::reasoner::{Diff, Materializer};
use crate::reasoning::reasoners::differential::abomonated_model::{AbomonatedAtom, AbomonatedRule, AbomonatedSign, AbomonatedTerm, AbomonatedTypedValue};
use crate::reasoning::reasoners::differential::abomonated_vertebra::AbomonatedSubstitutions;

pub type AtomCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, AbomonatedAtom>;
pub type SubstitutionsCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, AbomonatedSubstitutions>;

pub type RuleSink = Sender<(AbomonatedRule, isize)>;
pub type AtomSink = Sender<(AbomonatedAtom, isize)>;

pub type RuleSource = Receiver<(AbomonatedRule, isize)>;
pub type AtomSource = Receiver<(AbomonatedAtom, isize)>;

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

                    let (mut rule_input, rule_collection) = local.new_collection::<AbomonatedRule, isize>();
                    rule_collection.inspect(move |x| {
                        local_rule_output_sink.send((x.0.clone(), x.2)).unwrap();
                        println!("evaluating: {}", x.0);
                    });
                    (
                        rule_input,
                        rule_collection.arrange_by_self().trace,
                        rule_collection.probe(),
                    )
                });

            let (mut fact_input_session, fact_probe) =
                worker.dataflow_named::<usize, _, _>("fact_ingestion_and_reasoning", |local| {
                    let (mut fact_input_session, fact_collection) =
                        local.new_collection::<AbomonatedAtom, isize>();

                    let local_fact_output_sink = fact_output_sink.clone();
                    //let rule_collection = rule_trace.import(local).as_collection(|x, y| x.clone());
                    (
                        fact_input_session,
                        fact_collection
                            .distinct()
                            .inspect_batch(move |_t, xs| {
                                for (atom, time, diff) in xs {
                                    local_fact_output_sink.send((atom.clone(), *diff)).unwrap()
                                }
                            })
                            .probe(),
                    )
                });
            loop {
                if !rule_input_source.is_empty() || !fact_input_source.is_empty() {
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
                }
            }
        },
    )
    .unwrap();
}

pub struct DifferentialDatalog {
    pub fact_store: Instance<Vec<ValueRowId>>,
    ddflow: thread::JoinHandle<()>,
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
            fact_store: Instance::new(false),
            ddflow: handle,
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

impl Materializer for DifferentialDatalog {
    fn materialize(&mut self, program: &Program) {
        program.iter().for_each(|rule| {
            let mut possibly_interned_rule = rule.clone();
            if self.intern {
                possibly_interned_rule = self.interner.intern_rule(&possibly_interned_rule);
            }
            self.materialization.push(possibly_interned_rule.clone());
            self.rule_input_sink.send((AbomonatedRule::from(&possibly_interned_rule.to_string()[..]), 1)).unwrap();
        });

        while let Ok(fresh_intensional_atom) = self.fact_output_source.recv_timeout(Duration::from_millis(50)) {
            self.fact_store.insert_atom(&Atom::from(&fresh_intensional_atom.0.to_string()[..]))
        }
    }

    fn update(&mut self, changes: Vec<Diff>) {
        changes.iter().for_each(|(sign, (sym, value))| {
            let mut terms: Vec<AbomonatedTerm> = value
                .into_iter()
                .map(|untyped_value| AbomonatedTerm::Constant(AbomonatedTypedValue::from(untyped_value.to_typed_value())))
                .collect();

            let atom = AbomonatedAtom {
                terms,
                symbol: sym.to_string(),
                sign: AbomonatedSign::Positive,
            };

            if *sign {
                self.fact_input_sink.send((atom, 1)).expect("TODO: panic message");
            } else {
                self.fact_input_sink.send((atom, -1)).expect("TODO: panic message");
            }
        });

        while let Ok(fresh_intensional_atom) = self.fact_output_source.recv_timeout(Duration::from_millis(50)) {
            if fresh_intensional_atom.1 > 0 {
                self.fact_store.insert_atom(&Atom::from(&fresh_intensional_atom.0.to_string()[..]))
            } else {
                self.fact_store.delete_atom(&Atom::from(&fresh_intensional_atom.0.to_string()[..]))
            }
        }
    }

    fn triple_count(&self) -> usize {
        return self
            .fact_store
            .database
            .iter()
            .map(|(sym, rel)| return rel.ward.len())
            .sum();
    }
}
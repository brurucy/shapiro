mod abomonated_model;
mod abomonated_parsing;

use crate::data_structures::substitutions::Substitutions;
use crate::misc::string_interning::Interner;
use crate::models::datalog::{Atom, Program, Rule};
use crate::models::index::ValueRowId;
use crate::models::instance::Instance;
use crossbeam_channel::{Receiver, Sender};
use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use std::time::{Duration};
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::Threshold;
use timely::communication::allocator::Generic;
use timely::dataflow::scopes::Child;
use timely::worker::Worker;
use crate::reasoning::reasoners::differential::abomonated_model::{AbomonatedAtom, AbomonatedRule};

pub type AtomCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, AbomonatedAtom>;
pub type SubstitutionsCollection<'b> = Collection<Child<'b, Worker<Generic>, usize>, AbomonatedSubstitutions>;

pub type RuleSink = Sender<(AbomonatedRule, isize)>;
pub type AtomSink = Sender<(AbomonatedAtom, usize, isize)>;

pub type RuleSource = Receiver<(AbomonatedRule, isize)>;
pub type AtomSource = Receiver<(AbomonatedAtom, usize, isize)>;

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
                    let (mut rule_input, rule_collection) = local.new_collection::<AbomonatedRule, isize>();
                    rule_collection.inspect(|x| {
                        rule_output_sink.send((x.0.clone(), x.2)).unwrap();
                        println!("evaluating: {}", x.0)
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

                    let data_output_receiver = fact_input_source.clone();
                    let rule_collection = rule_trace.import(local).as_collection(|x, y| x.clone());
                    (
                        fact_input_session,
                        fact_collection
                            .distinct()
                            .inspect_batch(move |_t, xs| {
                                for (atom, time, diff) in xs {
                                    fact_output_sink.send((atom.clone(), *time, *diff)).unwrap()
                                }
                            })
                            .probe(),
                    )
                });
            let mut last_rule_ts = rule_input_session.time().clone();
            let mut last_data_ts = fact_input_session.time().clone();
            loop {
                if !rule_input_source.is_empty() || !fact_input_source.is_empty() {
                    // Rule
                    rule_input_source.try_iter().for_each(|triple| {
                        rule_input_session.update(triple.0, 1);
                    });

                    rule_input_session.advance_to(*rule_input_session.epoch() + 1);
                    rule_input_session.flush();

                    worker.step_or_park_while(Some(Duration::from_millis(1)), || {
                        rule_probe.less_than(rule_input_session.time())
                    });

                    // Data
                    fact_input_source.try_iter().for_each(|triple| {
                        fact_input_session.update(triple.0, 1);
                    });

                    fact_input_session.advance_to(*fact_input_session.epoch() + 1);
                    fact_input_session.flush();

                    worker.step_or_park_while(Some(Duration::from_millis(1)), || {
                        fact_probe.less_than(fact_input_session.time())
                    });

                    last_rule_ts = rule_input_session.time().clone();
                    last_data_ts = fact_input_session.time().clone();
                }

                worker.step();

                if fact_input_session.time().clone() == last_data_ts && rule_input_session.time().clone() == last_rule_ts {
                    fact_input_session.close();
                    rule_input_session.close();

                    worker.step_while(|| rule_probe.less_than(&(last_rule_ts + 1)));
                    worker.step_while(|| fact_probe.less_than(&(last_data_ts + 1)));
                    break;
                }
            }

            println!(
                "Total latency and triples processed at worker {}:{} ms, {} triples",
                worker.index(),
            );
        },
    )
    .unwrap();
}

pub struct DifferentialDatalog {
    pub fact_store: Instance<Vec<ValueRowId>>,
    ddflow: std::thread::JoinHandle<()>,
    pub interner: Interner,
    parallel: bool,
    intern: bool,
    materialization: Program,
}

extern crate core;

pub mod lexers;
pub mod models;
pub mod parsers;
pub mod data_structures;
pub mod reasoning;
pub mod misc;

#[cfg(test)]
mod tests {
    use crate::models::reasoner::{Dynamic, Materializer, Queryable};
    use crate::models::datalog::{Atom, Rule};
    use crate::models::index::{BTreeIndex};
    use crate::reasoning::reasoners::chibi::ChibiDatalog;
    use crate::reasoning::reasoners::simple::SimpleDatalog;

    #[test]
    fn test_chibi_datalog() {
        let mut reasoner: ChibiDatalog = Default::default();
        reasoner.insert("edge", vec![Box::new(1), Box::new(2)]);
        reasoner.insert("edge", vec![Box::new(2), Box::new(3)]);
        reasoner.insert("edge", vec![Box::new(2), Box::new(4)]);
        reasoner.insert("edge", vec![Box::new(4), Box::new(5)]);

        let query = vec![
            Rule::from("reachable(?x, ?y) <- [edge(?x, ?y)]"),
            Rule::from("reachable(?x, ?z) <- [edge(?x, ?y), reachable(?y, ?z)]"),
        ];

        reasoner.materialize(&query);

        vec![
            Atom::from("reachable(1, 2)"),
            Atom::from("reachable(1, 3)"),
            Atom::from("reachable(1, 4)"),
            Atom::from("reachable(1, 5)"),
            Atom::from("reachable(2, 3)"),
            Atom::from("reachable(2, 4)"),
            Atom::from("reachable(2, 5)"),
            Atom::from("reachable(4, 5)"),
        ]
            .iter()
            .for_each(|point_query| assert!(reasoner.contains(point_query)));

        reasoner.update(vec![
            (true, ("edge", vec![Box::new(1), Box::new(3)])),
            (true, ("edge", vec![Box::new(3), Box::new(4)])),
            (false, ("edge", vec![Box::new(1), Box::new(2)])),
            (false, ("edge", vec![Box::new(2), Box::new(3)])),
            (false, ("edge", vec![Box::new(2), Box::new(4)])),
        ]);

        vec![
            Atom::from("reachable(1, 3)"),
            Atom::from("reachable(3, 4)"),
            Atom::from("reachable(3, 5)"),
        ]
            .iter()
            .for_each(|point_query| assert!(reasoner.contains(point_query)));

        vec![
            Atom::from("reachable(1, 2)"),
            Atom::from("reachable(2, 3)"),
            Atom::from("reachable(2, 4)"),
            Atom::from("reachable(2, 5)"),
        ]
            .iter()
            .for_each(|point_query| assert!(!reasoner.contains(point_query)));
    }

    #[test]
    fn test_simple_datalog() {
        let mut reasoner: SimpleDatalog<BTreeIndex> = Default::default();
        reasoner.insert("edge", vec![Box::new(1), Box::new(2)]);
        reasoner.insert("edge", vec![Box::new(2), Box::new(3)]);
        reasoner.insert("edge", vec![Box::new(2), Box::new(4)]);
        reasoner.insert("edge", vec![Box::new(4), Box::new(5)]);

        let query = vec![
            Rule::from("reachable(?x, ?y) <- [edge(?x, ?y)]"),
            Rule::from("reachable(?x, ?z) <- [edge(?x, ?y), reachable(?y, ?z)]"),
        ];

        reasoner.materialize(&query);

        vec![
            Atom::from("reachable(1, 2)"),
            Atom::from("reachable(1, 3)"),
            Atom::from("reachable(1, 4)"),
            Atom::from("reachable(1, 5)"),
            Atom::from("reachable(2, 3)"),
            Atom::from("reachable(2, 4)"),
            Atom::from("reachable(2, 5)"),
            Atom::from("reachable(4, 5)"),
        ]
            .iter()
            .for_each(|point_query| assert!(reasoner.contains(point_query)));

        reasoner.update(vec![
            (true, ("edge", vec![Box::new(1), Box::new(3)])),
            (true, ("edge", vec![Box::new(3), Box::new(4)])),
            (false, ("edge", vec![Box::new(1), Box::new(2)])),
            (false, ("edge", vec![Box::new(2), Box::new(3)])),
            (false, ("edge", vec![Box::new(2), Box::new(4)])),
        ]);

        vec![
            Atom::from("reachable(1, 3)"),
            Atom::from("reachable(3, 4)"),
            Atom::from("reachable(3, 5)"),
        ]
            .iter()
            .for_each(|point_query| assert!(reasoner.contains(point_query)));

        vec![
            Atom::from("reachable(1, 2)"),
            Atom::from("reachable(2, 3)"),
            Atom::from("reachable(2, 4)"),
            Atom::from("reachable(2, 5)"),
        ]
            .iter()
            .for_each(|point_query| assert!(!reasoner.contains(point_query)));
    }
}

use std::collections::BTreeMap;
use indexmap::map::IndexMap;
use ahash::RandomState;

pub type IndexedHashMap<K, V> =  IndexMap<K, V, RandomState>;
//pub type IndexedHashMap<K, V> =  BTreeMap<K, V>;
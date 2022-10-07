use indexmap::map::IndexMap;
use ahash::RandomState;

pub type IndexedHashMap<K, V> =  IndexMap<K, V, RandomState>;
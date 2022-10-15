use dashmap::DashMap;
use indexmap::map::IndexMap;

pub type IndexedHashMap<K, V> = IndexMap<K, V, ahash::RandomState>;
pub type ConcurrentHashMap<K, V> = DashMap<K, V, ahash::RandomState>;
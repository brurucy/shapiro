use indexmap::map::IndexMap;

pub type IndexedHashMap<K, V> = IndexMap<K, V, ahash::RandomState>;

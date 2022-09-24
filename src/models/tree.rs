use std::fmt::{Debug, Display};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Node<T>
where
    T: PartialEq + Eq + Display + Clone,
{
    pub idx: usize,
    pub value: T,
    pub parent: Option<usize>,
    pub left_child: Option<usize>,
    pub right_child: Option<usize>,
}

impl<T: PartialEq + Eq + Display + Clone> Node<T> {
    fn new(idx: usize, value: &T) -> Self {
        Self {
            idx,
            value: value.clone(),
            parent: None,
            left_child: None,
            right_child: None,
        }
    }
}

#[derive(PartialEq, Clone)]
pub struct Tree<T>
where
    T: PartialEq + Eq + Display + Clone,
{
    pub arena: Vec<Node<T>>,
    pub root: Option<usize>,
}

impl<T: PartialEq + Eq + Display + Clone> Tree<T> {
    pub fn new() -> Self {
        Self {
            arena: vec![],
            root: None,
        }
    }

    pub fn set_root(&mut self, idx: usize)
    {
        let previous_root = self.root;
        self.root = Some(idx);
        if let Some(previous_root_addr) = previous_root {
            self.arena[previous_root_addr].parent = self.root;
        };
    }

    pub fn delete(&mut self, idx: usize) {
        self.arena[idx].parent = None;
        self.arena[idx].left_child = None;
        self.arena[idx].right_child = None;
    }

    pub fn allocate(&mut self, value: &T) -> usize {
        let addr = self.arena.len();
        self.arena.push(Node::new(addr, value));
        if let None = self.root {
            self.root = Some(addr)
        }
        addr
    }

    pub fn set_parent(&mut self, addr: usize, parent_addr: usize) {
        self.arena[addr].parent = Some(parent_addr);
    }

    pub fn set_value(&mut self, addr: usize, value: &T) {
        self.arena[addr].value = value.clone();
    }

    pub fn set_left_child(&mut self, addr: usize, left_child_addr: usize) {
        self.arena[addr].left_child = Some(left_child_addr);
    }

    pub fn set_right_child(&mut self, addr: usize, right_child_addr: usize) {
        self.arena[addr].right_child = Some(right_child_addr);
    }

    pub fn pre_order(&self) -> Vec<Node<T>> {
        if let Some(root_addr) = self.root {
            let root = self.arena[root_addr].clone();
            let mut root_vec = vec![];

            root_vec.push(root.clone());

            if let Some(left_subtree_addr) = root.left_child {
                let mut left_subtree = self.clone();
                left_subtree.set_root(left_subtree_addr);

                root_vec.extend(left_subtree.pre_order())
            }

            if let Some(right_subtree_addr) = root.right_child {
                let mut right_subtree = self.clone();
                right_subtree.set_root(right_subtree_addr);

                root_vec.extend(right_subtree.pre_order())
            }

            return root_vec;
        }
        return vec![];
    }

    pub fn to_string(&self) -> String {
        return if let Some(root_addr) = self.root {
            let root_node = self.arena[root_addr].clone();

            match (root_node.left_child, root_node.right_child) {
                (Some(left_child_addr), Some(right_child_addr)) => {
                    let mut left_subtree = self.clone();
                    left_subtree.set_root(left_child_addr);

                    let mut right_subtree = self.clone();
                    right_subtree.set_root(right_child_addr);

                    return format!(
                        "{}({}, {})",
                        root_node.value,
                        left_subtree.to_string(),
                        right_subtree.to_string()
                    );
                }
                (Some(left_child_addr), None) => {
                    let mut left_subtree = self.clone();
                    left_subtree.set_root(left_child_addr);

                    format!("{}({})", root_node.value, left_subtree.to_string())
                }
                _ => return root_node.value.to_string(),
            }
        } else {
            "".to_string()
        };
    }
}

use std::{fmt, mem};
use std::fmt::Formatter;
use std::rc::Rc;

pub struct Tree<const K: usize, const B: usize, const L: usize> {
    root: Rc<Node<B>>,
}

impl<const K: usize, const B: usize, const L: usize> Tree<K, B, L> {
    const _ASSERT: () = check_levels(K, B, L);

    pub fn new() -> Self {
        Self {
            root: Node::new_directory(vec![]),
        }
    }

    pub fn insert(&mut self, k: [u8; K], v: Vec<u8>) {
        let levels = Self::encode_levels(k);
        let mut node = &self.root;
        let mut nodes_to_replace = Vec::with_capacity(L); // todo - small vec
        let mut last = None;
        for (level_num, level_val) in levels.iter().enumerate() {
            let position = node.lookup(level_val);
            // println!("num {level_num}, val {}, node {node:?}, position {position:?}", format_b(level_val));
            match position {
                Ok(p) => {
                    nodes_to_replace.push((node, p, level_val));
                    node = node.child(p);
                },
                Err(p) => {
                    last = Some((p, level_num, level_val));
                    break;
                }
            }
        }
        let mut new_node = Node::<B>::new_leaf(v);
        if let Some((last_position, last_level, last_level_val)) = last {
            for new_level in levels.iter().skip(last_level + 1).rev() {
                new_node = Node::new_directory(vec![(*new_level, new_node)]);
            }
            new_node = node.clone_insert(last_position, *last_level_val, new_node);
        }
        for (node, position, level) in nodes_to_replace.iter().rev() {
            new_node = node.clone_replace(*position, **level, new_node);
        }
        self.root = new_node;
    }

    pub fn get(&self, k: [u8; K]) -> Option<&Vec<u8>> {
        let levels = Self::encode_levels(k);
        let mut node = &self.root;
        println!("Lookup {k:?}");
        for level in levels {
            println!("Node   {node:?}");
            println!("Level  {}", format_b(&level));
            let position = node.lookup(&level);
            let Ok(position) = position else {return None};
            node = node.child(position);
        }
        Some(node.value())
    }

    #[inline]
    fn encode_levels(k: [u8; K]) -> [[u8; B]; L] {
        assert_eq!(Self::_ASSERT, ()); // without this line compiler just throws away _ASSERT
        unsafe {
            *mem::transmute::<&[u8; K], &[[u8; B]; L]>(&k)
        }
    }
}

enum Node<const B: usize> {
    Directory(Vec<([u8; B], Rc<Node<B>>)>),
    Leaf(Vec<u8>),
}

impl<const B: usize> Node<B> {
    pub fn lookup(&self, k: &[u8; B]) -> Result<usize, usize> {
        let Self::Directory(directory) = self else {panic!("Lookup only permitted on directories")};
        directory.binary_search_by_key(&k, |(k, _)|k)
    }

    pub fn child(&self, position: usize) -> &Rc<Node<B>> {
        let Self::Directory(directory) = self else {panic!("Child only permitted on directories")};
        &directory.get(position).expect("Position out of bound").1
    }

    pub fn value(&self) -> &Vec<u8> {
        let Self::Leaf(leaf) = self else {panic!("Value only permitted on leaf")};
        leaf
    }

    pub fn clone_insert(&self, position: usize, k: [u8; B], v: Rc<Node<B>>) -> Rc<Self> {
        let Self::Directory(directory) = self else {panic!("clone_insert only permitted on directories")};
        let mut directory = directory.clone();
        // todo there is probably a way to combine clone and insert in a single op
        directory.insert(position, (k, v));
        Rc::new(Self::Directory(directory))
    }

    pub fn clone_replace(&self, position: usize, k: [u8; B], v: Rc<Node<B>>) -> Rc<Self> {
        let Self::Directory(directory) = self else {panic!("clone_insert only permitted on directories")};
        let mut directory = directory.clone();
        debug_assert_eq!(directory.get(position).unwrap().0, k);
        directory[position] = (k, v);
        Rc::new(Self::Directory(directory))
    }

    pub fn new_directory(v: Vec<([u8; B], Rc<Node<B>>)>) -> Rc<Self> {
        Rc::new(Node::Directory(v))
    }

    pub fn new_leaf(v: Vec<u8>) -> Rc<Self> {
        Rc::new(Node::Leaf(v))
    }
}

impl<const B: usize> fmt::Debug for Node<B> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Node::Directory(dir) => {
                write!(f, "Directory[")?;
                for (b, _) in dir {
                    write!(f, "{},", format_b(b))?;
                }
                write!(f, "]")
            }
            Node::Leaf(_) => write!(f, "Leaf")
        }
    }
}

fn format_b<const B: usize>(b: &[u8; B]) -> String {
    if B == 8 {
        let b = u64::from_le_bytes((&b[..]).try_into().unwrap());
        format!("{b}")
    } else {
        format!("{b:?}")
    }
}

const fn check_levels(k: usize, b: usize, l: usize) {
    assert!(l * b == k, "Level size mismatch");
}

const fn checked_levels(k: usize, b: usize) -> usize {
    let levels = k / b;
    assert!(levels * b == k, "K should be divisible by B");
    levels
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_encode_levels() {
        let mut k = [1; 32];
        k[3] = 10;
        k[9] = 15;
        let levels = Tree::<32, 8, 4>::encode_levels(k);
        assert_eq!(levels, [
            [1, 1, 1, 10, 1, 1, 1, 1],
            [1, 15, 1, 1, 1, 1, 1, 1],
            [1, 1, 1, 1, 1, 1, 1, 1],
            [1, 1, 1, 1, 1, 1, 1, 1],
        ])
    }

    #[test]
    fn test_tree() {
        let mut tree = Tree::<32, 8, 4>::new();
        let k1 = key([1, 2, 3, 4]);
        let k2 = key([5, 6, 7, 8]);
        let k3 = key([1, 2, 3, 5]);
        let k4 = key([1, 2, 8, 9]);
        tree.insert(k1, vec![1]);
        assert_eq!(tree.get(k1), Some(&vec![1]));
        tree.insert(k2, vec![2]);
        assert_eq!(tree.get(k1), Some(&vec![1]));
        assert_eq!(tree.get(k2), Some(&vec![2]));
        tree.insert(k3, vec![3]);
        assert_eq!(tree.get(k1), Some(&vec![1]));
        assert_eq!(tree.get(k2), Some(&vec![2]));
        assert_eq!(tree.get(k3), Some(&vec![3]));
        tree.insert(k4, vec![4]);
        assert_eq!(tree.get(k1), Some(&vec![1]));
        assert_eq!(tree.get(k2), Some(&vec![2]));
        assert_eq!(tree.get(k3), Some(&vec![3]));
        assert_eq!(tree.get(k4), Some(&vec![4]));
    }

    fn key(v: [u64; 4]) -> [u8; 32] {
        let mut r = [0u8; 32];
        for (i, v) in v.iter().enumerate() {
            let b = v.to_le_bytes();
            r[i * 8..(i+1)*8].copy_from_slice(&b);
        }
        r
    }
}
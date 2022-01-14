use std::cmp::Ordering;
use std::ops::Index;
use std::ops::Sub;


/* Key-position */

pub type KeyT = i64;
pub type PositionT = usize;
pub const KEY_LENGTH: usize = std::mem::size_of::<KeyT>();
pub const POSITION_LENGTH: usize = std::mem::size_of::<PositionT>();

#[derive(Clone, Debug)]
pub struct KeyPosition {
  pub key: KeyT,  // TODO: generic Num + PartialOrd type
  pub position: PositionT,
}

impl KeyPosition {
  pub fn interpolate_with(&self, other: &KeyPosition, key: &KeyT) -> PositionT {
    self.position + (
      ((*key - self.key) as f64
      / (other.key - self.key) as f64)
      * (other.position - self.position) as f64
    ).floor() as PositionT
  }

  pub fn cross_product(&self, other: &KeyPosition) -> f64 {
    self.key as f64 * other.position as f64 - self.position as f64 * other.key as f64
  }

  pub fn is_lower_slope_than(&self, other: &KeyPosition, pov: &KeyPosition) -> bool {
    (self - pov).cross_product(&(other - pov)) > 0.0
  }
}

impl<'a, 'b> Sub<&'b KeyPosition> for &'a KeyPosition {
    type Output = KeyPosition;

    fn sub(self, other: &'b KeyPosition) -> Self::Output {
        KeyPosition {
            key: self.key - other.key,
            position: self.position - other.position,
        }
    }
}


/* Key-position-length */

#[derive(Debug, PartialEq)]
pub struct KeyPositionRange {
  pub key: KeyT,
  pub offset: PositionT,
  pub length: PositionT,
}


/* Key interval */

#[derive(Debug, PartialEq)]
pub struct KeyInterval {
  pub left_key: KeyT,
  pub right_key: KeyT,
}

impl KeyInterval {
  pub fn greater_than(&self, key: &KeyT) -> bool {
    *key < self.left_key
  }

  pub fn less_than(&self, key: &KeyT) -> bool {
    self.right_key < *key
  }

  pub fn cover(&self, key: &KeyT) -> bool {
    self.left_key <= *key && *key <= self.right_key
  }
}


/* Key-position Collections */

pub struct KeyPositionCollection {
  kps: Vec<KeyPosition>,
  start_position: PositionT,
  end_position: PositionT,
}

impl Default for KeyPositionCollection {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyPositionCollection {
  pub fn new() -> KeyPositionCollection {
    KeyPositionCollection{
      kps: Vec::new(),
      start_position: 0,
      end_position: 0,
    }
  }

  pub fn push(&mut self, key: KeyT, position: PositionT) {
    self.kps.push(KeyPosition{ key, position })
  }

  pub fn len(&self) -> usize {
    self.kps.len()
  } 

  pub fn is_empty(&self) -> bool {
    self.kps.is_empty()
  }

  pub fn set_position_range(&mut self, start_position: usize, end_position: usize) {
    self.start_position = start_position;
    self.end_position = end_position;
  }

  pub fn position_for(&self, key: KeyT) -> Result<usize, &str> {
    for kp in &self.kps {
      if kp.key == key {
        return Ok(kp.position);
      }
    }
    Err("Key not contained in this key-position collection")
  }

  pub fn range_at(&self, idx: usize) -> Result<KeyPositionRange, &str> {
    match idx.cmp(&(self.len() - 1)) {
      Ordering::Less => Ok(KeyPositionRange{
        key: self.kps[idx].key,
        offset: self.kps[idx].position,
        length: self.kps[idx+1].position - self.kps[idx].position,
      }),
      Ordering::Equal => Ok(KeyPositionRange{
        key: self.kps[idx].key,
        offset: self.kps[idx].position,
        length: self.end_position - self.kps[idx].position,
      }),
      Ordering::Greater => Err("Index out of range"),
    }
  }

  pub fn iter(&self) -> std::slice::Iter<KeyPosition> {
    self.kps.iter()
  }

  pub fn range_iter(&self) -> KeyPositionRangeIterator {
    KeyPositionRangeIterator::new(self)
  }
}

impl Index<usize> for KeyPositionCollection {
  type Output = KeyPosition;
  fn index(&self, idx: usize) -> &Self::Output {
    &self.kps[idx]
  }
}


/* Range iterator */
pub struct KeyPositionRangeIterator<'a> {
  kps: &'a KeyPositionCollection,
  current_idx: usize,
}

impl<'a> KeyPositionRangeIterator<'a> {
  fn new(kps: &'a KeyPositionCollection) -> KeyPositionRangeIterator {
    KeyPositionRangeIterator{ kps, current_idx: 0 }
  }
}

impl<'a> Iterator for KeyPositionRangeIterator<'a> {
  type Item = KeyPositionRange;

  fn next(&mut self) -> Option<Self::Item> {
    match self.kps.range_at(self.current_idx) {
      Ok(kr) => {
        self.current_idx += 1;
        Some(kr)
      },
      Err(_) => None,
    }
  }
}
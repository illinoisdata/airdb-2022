use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use serde::{Serialize, Deserialize};
use std::cmp;
use std::io;

use crate::common::error::GResult;
use crate::meta::Context;
use crate::model::BuilderFinalReport;
use crate::model::MaybeKeyBuffer;
use crate::model::Model;
use crate::model::ModelBuilder;
use crate::model::ModelDrafter;
use crate::model::ModelRecon;
use crate::model::ModelReconMeta;
use crate::model::ModelReconMetaserde;
use crate::model::toolkit::BuilderAsDrafter;
use crate::model::toolkit::MultipleDrafter;
use crate::store::key_buffer::KeyBuffer;
use crate::store::key_position::KEY_LENGTH;
use crate::store::key_position::KeyPosition;
use crate::store::key_position::KeyPositionRange;
use crate::store::key_position::KeyT;
use crate::store::key_position::POSITION_LENGTH;
use crate::store::key_position::PositionT;



/*

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

WARNING:

this model building has a bug where the line cuts through the key-position box

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

*/

/* The Model */

#[derive(Debug)]
struct LinearModel {
  left_kp: KeyPosition,
  right_kp: KeyPosition,
}

impl LinearModel {
  fn evaluate(&self, key: &KeyT) -> PositionT {
    // PANIC: this would overflow if key is outside of the coverage...
    self.left_kp.interpolate_with(&self.right_kp, key)
  }
}

// TODO: can save 50% space if ranges are equal
#[derive(Debug)]
struct DoubleLinearModel {
  lower_line: LinearModel,
  upper_line: LinearModel,
}

impl Model for DoubleLinearModel {
  fn predict(&self, key: &KeyT) -> KeyPositionRange {
    // PANIC: this would overflow if key is outside of the coverage...
    let left_offset = self.lower_line.evaluate(key).saturating_sub(128);  // HACK: the box is always less than 8 byte high...
    let right_offset = self.upper_line.evaluate(key) + 128;  // HACK: the box is always less than 8 byte high...
    KeyPositionRange::from_bound(*key, *key, left_offset, right_offset)
  }
}


/* Serialization */

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DoubleLinearModelRecon {
  max_load: Option<usize>,
}

impl DoubleLinearModelRecon {
  fn new() -> DoubleLinearModelRecon {
    DoubleLinearModelRecon { max_load: None }
  }

  fn sketch(&mut self, dlm: &DoubleLinearModel) -> io::Result<Vec<u8>> {
    // turn the model into a buffer
    let mut model_buffer = vec![];
    model_buffer.write_uint::<BigEndian>(dlm.lower_line.left_kp.key, KEY_LENGTH)?;
    model_buffer.write_uint::<BigEndian>(dlm.lower_line.left_kp.position as u64, POSITION_LENGTH)?;
    model_buffer.write_uint::<BigEndian>(dlm.lower_line.right_kp.key, KEY_LENGTH)?;
    model_buffer.write_uint::<BigEndian>(dlm.lower_line.right_kp.position as u64, POSITION_LENGTH)?;
    model_buffer.write_uint::<BigEndian>(dlm.upper_line.left_kp.key, KEY_LENGTH)?;
    model_buffer.write_uint::<BigEndian>(dlm.upper_line.left_kp.position as u64, POSITION_LENGTH)?;
    model_buffer.write_uint::<BigEndian>(dlm.upper_line.right_kp.key, KEY_LENGTH)?;
    model_buffer.write_uint::<BigEndian>(dlm.upper_line.right_kp.position as u64, POSITION_LENGTH)?;
    Ok(model_buffer)
  }

  fn set_max_load(&mut self, max_load: usize) {
    self.max_load = Some(max_load)
  }
}

impl ModelRecon for DoubleLinearModelRecon {
  fn reconstruct(&self, buffer: &[u8]) -> GResult<Box<dyn Model>> {
    let mut model_buffer = io::Cursor::new(buffer);
    let model = DoubleLinearModel {
      lower_line: LinearModel {
        left_kp: KeyPosition {
          key: model_buffer.read_uint::<BigEndian>(KEY_LENGTH)?,
          position: model_buffer.read_uint::<BigEndian>(POSITION_LENGTH)? as PositionT,
        },
        right_kp: KeyPosition {
          key: model_buffer.read_uint::<BigEndian>(KEY_LENGTH)?,
          position: model_buffer.read_uint::<BigEndian>(POSITION_LENGTH)? as PositionT,
        }, 
      },
      upper_line: LinearModel {
        left_kp: KeyPosition {
          key: model_buffer.read_uint::<BigEndian>(KEY_LENGTH)?,
          position: model_buffer.read_uint::<BigEndian>(POSITION_LENGTH)? as PositionT,
        },
        right_kp: KeyPosition {
          key: model_buffer.read_uint::<BigEndian>(KEY_LENGTH)?,
          position: model_buffer.read_uint::<BigEndian>(POSITION_LENGTH)? as PositionT,
        }, 
      },
    };
    Ok(Box::new(model))
  }
}

pub type DoubleLinearModelReconMeta = DoubleLinearModelRecon;

impl ModelReconMetaserde for DoubleLinearModelRecon {  // for Metaserde
  fn to_meta(&self, _ctx: &mut Context) -> GResult<ModelReconMeta> {
    Ok(ModelReconMeta::DoubleLinear{ meta: self.clone() })
  }
}

impl DoubleLinearModelRecon {  // for Metaserde
  pub fn from_meta(meta: DoubleLinearModelReconMeta, _ctx: &Context) -> GResult<DoubleLinearModelRecon> {
    Ok(meta)
  }
}


/* Builder */

fn sort_anchor<'a>(anc_1: &'a KeyPosition, anc_2: &'a KeyPosition, pov: &'a KeyPosition) -> (&'a KeyPosition, &'a KeyPosition) {
  if anc_1.is_lower_slope_than(anc_2, pov) {
    (anc_1, anc_2)
  } else {
    (anc_2, anc_1)
  }
}

#[derive(Debug)]
struct Corridor {
  key: KeyT,
  lower_kp: KeyPosition,
  upper_kp: KeyPosition,
}

impl Corridor {
  fn intersect(&self, other: &Corridor, pov: &KeyPosition) -> Corridor {
    let new_key = cmp::max(self.key, other.key);
    let (_, new_lower_kp) = sort_anchor(&self.lower_kp, &other.lower_kp, pov);
    let (new_upper_kp, _) = sort_anchor(&self.upper_kp, &other.upper_kp, pov);
    Corridor {
      key: new_key,
      lower_kp: new_lower_kp.clone(),
      upper_kp: new_upper_kp.clone(),
    }
  }

  fn lower_anchor(&self, pov: &KeyPosition) -> KeyPosition {
    let lower_position = pov.interpolate_with(&self.lower_kp, &self.key);
    KeyPosition { key: self.key, position: lower_position }
  }

  fn upper_anchor(&self, pov: &KeyPosition) -> KeyPosition {
    let upper_position = pov.interpolate_with(&self.upper_kp, &self.key);
    KeyPosition { key: self.key, position: upper_position }
  }

  // fn middle_anchor(&self, pov: &KeyPosition) -> KeyPosition {
  //   let lower_position = pov.interpolate_with(&self.lower_kp, &self.key);
  //   let upper_position = pov.interpolate_with(&self.upper_kp, &self.key);
  //   KeyPosition { key: self.key, position: (lower_position + upper_position) / 2 }
  // }

  fn is_valid(&self, pov: &KeyPosition) -> bool {
    !self.upper_kp.is_lower_slope_than(&self.lower_kp, pov)
  }
}

pub struct DoubleLinearGreedyCorridorBuilder {
  max_error: PositionT,
  max_length: PositionT,
  serde: DoubleLinearModelRecon,
  lower_pov: Option<KeyPosition>,
  lower_corridor: Option<Corridor>,
  upper_pov: Option<KeyPosition>,
  upper_corridor: Option<Corridor>,
}

impl std::fmt::Debug for DoubleLinearGreedyCorridorBuilder {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("DoubleLinearGCB")
      .field("max_error", &self.max_error)
      .finish()
  }
}

impl DoubleLinearGreedyCorridorBuilder {
  pub fn new(max_error: PositionT) -> DoubleLinearGreedyCorridorBuilder {
    DoubleLinearGreedyCorridorBuilder {
      max_error,
      max_length: 0,
      serde: DoubleLinearModelRecon::new(),
      lower_pov: None,
      lower_corridor: None,
      upper_pov: None,
      upper_corridor: None,
    }
  }

  fn generate_segment(&mut self) -> GResult<MaybeKeyBuffer> {
    let lower_line = match &self.lower_pov {
      Some(left_kp) => {
        match &self.lower_corridor {
          Some(corridor) => {
            let right_kp = corridor.upper_anchor(left_kp);  // <--- difference
            LinearModel { left_kp: left_kp.clone(), right_kp }
          }
          None => LinearModel { left_kp: left_kp.clone(), right_kp: left_kp.clone() },
        }
      },
      None => return Ok(None),
    };
    let upper_line = match &self.upper_pov {
      Some(left_kp) => {
        match &self.upper_corridor {
          Some(corridor) => {
            let right_kp = corridor.lower_anchor(left_kp);  // <--- difference
            LinearModel { left_kp: left_kp.clone(), right_kp }
          }
          None => LinearModel { left_kp: left_kp.clone(), right_kp: left_kp.clone() },
        }
      },
      None => return Ok(None),
    };
    let dlinear_model = DoubleLinearModel { lower_line, upper_line };
    let dlinear_buffer = self.serde.sketch(&dlinear_model)?;
    Ok(Some(KeyBuffer::new(dlinear_model.lower_line.left_kp.key, dlinear_buffer)))
  }
}

impl ModelBuilder for DoubleLinearGreedyCorridorBuilder {
  fn consume(&mut self, kpr: &KeyPositionRange) -> GResult<MaybeKeyBuffer> {
    // update largest data for load calculation
    self.max_length = cmp::max(self.max_length, kpr.length);

    // update pov + corridor states
    match &self.lower_pov {
      Some(lower_pov) => {
        let upper_pov = self.upper_pov.as_ref().expect("Bug: out-of-sync lower and upper povs");

        // Update lower corridor
        let new_lower_corridor = {
          let lower_kp = KeyPosition{ key: kpr.key_l, position: kpr.offset.saturating_sub(self.max_error) };
          let upper_kp = KeyPosition{ key: kpr.key_l, position: kpr.offset };
          let kp_corridor = Corridor{ key: kpr.key_l, lower_kp, upper_kp };
          match &self.lower_corridor {
            Some(corridor) => corridor.intersect(&kp_corridor, lower_pov),
            None => kp_corridor,
          }
        };
        let new_upper_corridor = {
          let upper_offset =  kpr.offset + kpr.length;
          let lower_kp = KeyPosition{ key: kpr.key_l, position: upper_offset };
          let upper_kp = KeyPosition{ key: kpr.key_l, position: upper_offset + self.max_error };
          let kp_corridor = Corridor{ key: kpr.key_l, lower_kp, upper_kp };
          match &self.upper_corridor {
            Some(corridor) => corridor.intersect(&kp_corridor, upper_pov),
            None => kp_corridor,
          }
        };

        if new_lower_corridor.is_valid(lower_pov) && new_upper_corridor.is_valid(upper_pov) {
          // ok to include
          self.lower_corridor = Some(new_lower_corridor);
          self.upper_corridor = Some(new_upper_corridor);
          Ok(None)
        } else {
          // including this point would violate the constraint
          let new_buffer = self.generate_segment()?;

          // restart new segment
          self.lower_pov = Some(KeyPosition{ key: kpr.key_l, position: kpr.offset });
          self.upper_pov = Some(KeyPosition{ key: kpr.key_l, position: kpr.offset + kpr.length + self.max_error });
          self.lower_corridor = None;
          self.upper_corridor = None;

          Ok(new_buffer)
        }
      },
      None => {
        assert!(self.upper_pov.is_none());
        self.lower_pov = Some(KeyPosition{ key: kpr.key_l, position: kpr.offset });
        self.upper_pov = Some(KeyPosition{ key: kpr.key_l, position: kpr.offset + kpr.length + self.max_error });
        Ok(None)
      },
    }
  }

  fn finalize(mut self: Box<Self>) -> GResult<BuilderFinalReport> {
    let maybe_last_kb = self.generate_segment()?;
    self.serde.set_max_load(self.max_length + (2 * self.max_error));
    Ok(BuilderFinalReport {
      maybe_model_kb: maybe_last_kb,
      serde: Box::new(self.serde),
      model_loads: vec![self.max_length + (2 * self.max_error)],
    })
  }
}

impl DoubleLinearGreedyCorridorBuilder {
  fn drafter(max_error: usize) -> Box<dyn ModelDrafter> {
    let dlm_producer = Box::new(
      move || {
        Box::new(DoubleLinearGreedyCorridorBuilder::new(max_error)) as Box<dyn ModelBuilder>
      });
    Box::new(BuilderAsDrafter::wrap(dlm_producer))
  }
}


/* Drafter */

// drafter that tries models with all these max errors and picks cheapest one
// it offers different choices of linear builders
pub struct DoubleLinearMultipleDrafter;

impl DoubleLinearMultipleDrafter {
  pub fn exponentiation(low_error: PositionT, high_error: PositionT, exponent: f64) -> MultipleDrafter {
    let mut dlm_drafters = Vec::new();
    let mut current_error = low_error;
    while current_error < high_error {
      dlm_drafters.push(DoubleLinearGreedyCorridorBuilder::drafter(current_error));
      current_error = ((current_error as f64) * exponent) as PositionT;
    }
    dlm_drafters.push(DoubleLinearGreedyCorridorBuilder::drafter(high_error));
    MultipleDrafter::from(dlm_drafters)
  }
}


/* Tests */

#[cfg(test)]
mod tests {
  use super::*;

  use crate::common::SharedByteSlice;
  use crate::model::KeyPositionCollection;


  fn test_same_model(model_1: &Box<dyn Model>, model_2: &Box<DoubleLinearModel>) {
    // test coverage
    // assert_eq!(model_1.coverage(), model_2.coverage(), "Models have different coverage");
    // let coverage = model_1.coverage();

    // test predict
    for test_key in 0..1000 {
      assert_eq!(
        model_1.predict(&test_key),
        model_2.predict(&test_key),
        "Models predict differently"
      ); 
    }
  }
  
  #[test]
  fn serde_test() -> GResult<()> {
    let mut dlm_serde = DoubleLinearModelRecon::new();
    let dlm = Box::new(DoubleLinearModel {
      lower_line: LinearModel {
        left_kp: KeyPosition { key: 123, position: 4567 },
        right_kp: KeyPosition { key: 456, position: 7438 }, 
      },
      upper_line: LinearModel {
        left_kp: KeyPosition { key: 123, position: 4570 },
        right_kp: KeyPosition { key: 456, position: 7499 }, 
      },
    });

    // sketch this model
    let dlm_buffer = dlm_serde.sketch(&dlm)?;
    assert!(dlm_buffer.len() > 0);

    // reconstruct
    let dlm_recon = dlm_serde.reconstruct(&dlm_buffer)?;
    test_same_model(&dlm_recon, &dlm);

    Ok(())
  }

  fn generate_test_kprs() -> Vec<KeyPositionRange> {
    let mut kps = KeyPositionCollection::new();
    kps.push(0, 0);
    kps.push(50, 7);
    kps.push(100, 10);
    kps.push(105, 30);
    kps.push(110, 50);
    kps.push(115, 70);
    kps.push(120, 90);  // jump, should split here
    kps.push(131, 1000);
    kps.set_position_range(0, 1915);
    kps.range_iter().collect()
  }

  fn assert_none_buffer(buffer: MaybeKeyBuffer) -> MaybeKeyBuffer {
    assert!(buffer.is_none());
    None
  }

  fn assert_some_buffer(buffer: MaybeKeyBuffer) -> SharedByteSlice {
    assert!(buffer.is_some());
    buffer.unwrap().buffer
  }
  
  #[test]
  fn greedy_corridor_test() -> GResult<()> {
    let kprs = generate_test_kprs();
    let mut dlm_builder = Box::new(DoubleLinearGreedyCorridorBuilder::new(0));

    // start adding points
    let _model_kb_0 = assert_none_buffer(dlm_builder.consume(&kprs[0])?);
    let _model_kb_1 = assert_none_buffer(dlm_builder.consume(&kprs[1])?);
    let model_kb_2 = assert_some_buffer(dlm_builder.consume(&kprs[2])?);
    let _model_kb_3 = assert_none_buffer(dlm_builder.consume(&kprs[3])?);
    let _model_kb_4 = assert_none_buffer(dlm_builder.consume(&kprs[4])?);
    let _model_kb_5 = assert_none_buffer(dlm_builder.consume(&kprs[5])?);
    let model_kb_6 = assert_some_buffer(dlm_builder.consume(&kprs[6])?);
    let _model_kb_7 = assert_none_buffer(dlm_builder.consume(&kprs[7])?);

    // finalize the builder
    let BuilderFinalReport {
      maybe_model_kb: last_buffer,
      serde: dlm_serde,
      model_loads: dlm_loads,
    } = dlm_builder.finalize()?;
    let model_kb_8 = assert_some_buffer(last_buffer);
    assert_eq!(dlm_loads, vec![915]);

    // check buffers
    test_same_model(&dlm_serde.reconstruct(&model_kb_2[..])?, &Box::new(DoubleLinearModel {
      lower_line: LinearModel {
        left_kp: KeyPosition { key: 0, position: 0 },
        right_kp: KeyPosition { key: 50, position: 7 }, 
      },
      upper_line: LinearModel {
        left_kp: KeyPosition { key: 0, position: 7 },
        right_kp: KeyPosition { key: 50, position: 10 }, 
      },
    }));
    test_same_model(&dlm_serde.reconstruct(&model_kb_6[..])?, &Box::new(DoubleLinearModel {
      lower_line: LinearModel {
        left_kp: KeyPosition { key: 100, position: 10 },
        right_kp: KeyPosition { key: 115, position: 70 }, 
      },
      upper_line: LinearModel {
        left_kp: KeyPosition { key: 100, position: 30 },
        right_kp: KeyPosition { key: 115, position: 90 }, 
      },
    }));
    test_same_model(&dlm_serde.reconstruct(&model_kb_8[..])?, &Box::new(DoubleLinearModel {
      lower_line: LinearModel {
        left_kp: KeyPosition { key: 120, position: 90 },
        right_kp: KeyPosition { key: 131, position: 1000 }, 
      },
      upper_line: LinearModel {
        left_kp: KeyPosition { key: 120, position: 1000 },
        right_kp: KeyPosition { key: 131, position: 1915 }, 
      },
    }));
    Ok(())
  }
  
  #[test]
  fn greedy_corridor_with_error_test() -> GResult<()> {
    let kprs = generate_test_kprs();
    let mut dlm_builder = Box::new(DoubleLinearGreedyCorridorBuilder::new(100));

    // start adding points
    let _model_kb_0 = assert_none_buffer(dlm_builder.consume(&kprs[0])?);
    let _model_kb_1 = assert_none_buffer(dlm_builder.consume(&kprs[1])?);
    let _model_kb_2 = assert_none_buffer(dlm_builder.consume(&kprs[2])?);
    let _model_kb_3 = assert_none_buffer(dlm_builder.consume(&kprs[3])?);
    let _model_kb_4 = assert_none_buffer(dlm_builder.consume(&kprs[4])?);
    let _model_kb_5 = assert_none_buffer(dlm_builder.consume(&kprs[5])?);
    let model_kb_6 = assert_some_buffer(dlm_builder.consume(&kprs[6])?);
    let _model_kb_7 = assert_none_buffer(dlm_builder.consume(&kprs[7])?);

    // finalize the builder
    let BuilderFinalReport {
      maybe_model_kb: last_buffer,
      serde: dlm_serde,
      model_loads: dlm_loads,
    } = dlm_builder.finalize()?;
    let model_kb_8 = assert_some_buffer(last_buffer);
    assert_eq!(dlm_loads, vec![1115]);

    // check buffers
    test_same_model(&dlm_serde.reconstruct(&model_kb_6[..])?, &Box::new(DoubleLinearModel {
      lower_line: LinearModel {
        left_kp: KeyPosition { key: 0, position: 0 },
        right_kp: KeyPosition { key: 115, position: 11 }, 
      },
      upper_line: LinearModel {
        left_kp: KeyPosition { key: 0, position: 107 },
        right_kp: KeyPosition { key: 115, position: 90 }, 
      },
    }));
    test_same_model(&dlm_serde.reconstruct(&model_kb_8[..])?, &Box::new(DoubleLinearModel {
      lower_line: LinearModel {
        left_kp: KeyPosition { key: 120, position: 90 },
        right_kp: KeyPosition { key: 131, position: 1000 }, 
      },
      upper_line: LinearModel {
        left_kp: KeyPosition { key: 120, position: 1100 },
        right_kp: KeyPosition { key: 131, position: 1915 }, 
      },
    }));
    Ok(())
  }
}

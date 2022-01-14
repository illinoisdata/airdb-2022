use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use std::error::Error;
use std::cmp;
use std::io;

use crate::common::error::GResult;
use crate::model::MaybeKeyBuffer;
use crate::model::Model;
use crate::model::ModelBuilder;
use crate::model::ModelRecon;
use crate::db::key_buffer::KeyBuffer;
use crate::db::key_position::KEY_LENGTH;
use crate::db::key_position::KeyInterval;
use crate::db::key_position::KeyPosition;
use crate::db::key_position::KeyPositionRange;
use crate::db::key_position::KeyT;
use crate::db::key_position::POSITION_LENGTH;
use crate::db::key_position::PositionT;


/* The Model */

struct LowerLinearModel {
  left_kp: KeyPosition,
  right_kp: KeyPosition,
  max_error: PositionT,
}

impl Model for LowerLinearModel {
  fn coverage(&self) -> KeyInterval {
    KeyInterval{ left_key: self.left_kp.key, right_key: self.right_kp.key }
  }

  fn predict(&self, key: &KeyT) -> KeyPositionRange {
    if self.left_kp.key == self.right_kp.key {
      KeyPositionRange{
        key: *key,
        offset: self.left_kp.position,
        length: self.max_error,
      }
    } else { 
      KeyPositionRange{
        key: *key,
        offset: self.left_kp.interpolate_with(&self.right_kp, key),
        length: self.max_error,
      }
    }
  }
}


/* Serialization */

struct LowerLinearModelSerde {
  max_error: PositionT,
}

impl LowerLinearModelSerde {
  fn new() -> LowerLinearModelSerde {
    LowerLinearModelSerde{ max_error: 0 }
  }

  fn sketch(&mut self, llm: &LowerLinearModel) -> io::Result<Vec<u8>> {
    // update max error
    self.max_error = cmp::max(self.max_error, llm.max_error);

    // turn the model into a buffer
    let mut model_buffer = vec![];
    model_buffer.write_int::<BigEndian>(llm.left_kp.key, KEY_LENGTH)?;
    model_buffer.write_uint::<BigEndian>(llm.left_kp.position as u64, POSITION_LENGTH)?;
    model_buffer.write_int::<BigEndian>(llm.right_kp.key, KEY_LENGTH)?;
    model_buffer.write_uint::<BigEndian>(llm.right_kp.position as u64, POSITION_LENGTH)?;
    Ok(model_buffer)
  }
}

impl ModelRecon for LowerLinearModelSerde {
  fn reconstruct(&self, buffer: &[u8]) -> Result<Box<dyn Model>, Box<dyn Error>> {
    let mut model_buffer = io::Cursor::new(buffer);
    Ok(Box::new(LowerLinearModel {
      left_kp: KeyPosition {
        key: model_buffer.read_int::<BigEndian>(KEY_LENGTH)?,
        position: model_buffer.read_uint::<BigEndian>(POSITION_LENGTH)? as PositionT,
      },
      right_kp: KeyPosition {
        key: model_buffer.read_int::<BigEndian>(KEY_LENGTH)?,
        position: model_buffer.read_uint::<BigEndian>(POSITION_LENGTH)? as PositionT,
      },
      max_error: self.max_error,
    }))
  }
}


/* Builder */

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

  fn top_anchor(&self, pov: &KeyPosition) -> KeyPosition {
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

pub struct LowerLinearGreedyCorridorBuilder {
  serde: LowerLinearModelSerde,
  cur_anc: Option<KeyPosition>,
  corridor: Option<Corridor>,
  max_error: PositionT,
}


fn sort_anchor<'a>(anc_1: &'a KeyPosition, anc_2: &'a KeyPosition, pov: &'a KeyPosition) -> (&'a KeyPosition, &'a KeyPosition) {
  if anc_1.is_lower_slope_than(anc_2, pov) {
    (anc_1, anc_2)
  } else {
    (anc_2, anc_1)
  }
}

impl LowerLinearGreedyCorridorBuilder {
  pub fn new(max_error: PositionT) -> LowerLinearGreedyCorridorBuilder {
    LowerLinearGreedyCorridorBuilder {
      serde: LowerLinearModelSerde::new(),
      cur_anc: None,
      corridor: None,
      max_error,
    }
  }

  fn generate_segment(&mut self) -> GResult<MaybeKeyBuffer> {
    match &self.cur_anc {
      Some(left_kp) => {
        let last_model = match &self.corridor {
          Some(corridor) => {
            let right_kp = corridor.top_anchor(left_kp);
            LowerLinearModel { left_kp: left_kp.clone(), right_kp, max_error: self.max_error }
          }
          None => LowerLinearModel { left_kp: left_kp.clone(), right_kp: left_kp.clone(), max_error: self.max_error },
        };
        Ok(Some(KeyBuffer{ key: left_kp.key, buffer: self.serde.sketch(&last_model)? }))
      },
      None => Ok(None),
    }
  }
}

impl ModelBuilder for LowerLinearGreedyCorridorBuilder {
  fn consume(&mut self, kp: &KeyPosition) -> GResult<MaybeKeyBuffer> {
    match &self.cur_anc {
      Some(pov) => {
        // compute new upper and lower anchors
        let lower_kp = KeyPosition{ key: kp.key, position: kp.position.saturating_sub(self.max_error) };
        let upper_kp = KeyPosition{ key: kp.key, position: kp.position };
        let kp_corridor = Corridor{ key: kp.key, lower_kp, upper_kp };
        let new_corridor = match &self.corridor {
          Some(corridor) => corridor.intersect(&kp_corridor, pov),
          None => kp_corridor,
        };

        if new_corridor.is_valid(pov) {
          // ok to include
          self.corridor = Some(new_corridor);
          Ok(None)
        } else {
          // including this point would violate the constraint
          let new_buffer = self.generate_segment()?;

          // restart new segment
          self.cur_anc = Some(KeyPosition { key: kp.key, position: kp.position });
          self.corridor = None;

          Ok(new_buffer)
        }
      },
      None => {
        self.cur_anc = Some(KeyPosition { key: kp.key, position: kp.position });
        Ok(None)
      },
    }
  }

  fn finalize(mut self: Box<Self>) -> GResult<(MaybeKeyBuffer, Box<dyn ModelRecon>)> {
    let maybe_last_kb = self.generate_segment()?;
    Ok((maybe_last_kb, Box::new(self.serde)))
  }
}



/* Tests */

#[cfg(test)]
mod tests {
  use super::*;

  fn test_same_model(model_1: &Box<dyn Model>, model_2: &Box<LowerLinearModel>) {
    // test coverage
    assert_eq!(model_1.coverage(), model_2.coverage(), "Models have different coverage");

    // test predict
    const TEST_KEYS: [KeyT; 14] = [
      -50,
      100,
      200,
      1,
      2,
      4,
      8,
      16,
      32,
      64,
      128,
      256,
      512,
      1024,
    ];
    for test_key in TEST_KEYS {
      assert_eq!(
        model_1.predict(&test_key),
        model_2.predict(&test_key),
        "Models predict differently"
      );
    }
  }
  
  #[test]
  fn serde_test() -> GResult<()> {
    let mut llm_serde = LowerLinearModelSerde::new();
    let llm = Box::new(LowerLinearModel {
      left_kp: KeyPosition{ key: -123, position: 456718932 },
      right_kp: KeyPosition{ key: 456, position: 743819208 },
      max_error: 100,
    });

    // sketch this model
    let llm_buffer = llm_serde.sketch(&llm)?;
    assert_eq!(llm_serde.max_error, llm.max_error);
    assert!(llm_buffer.len() > 0);

    // reconstruct
    let llm_recon = llm_serde.reconstruct(&llm_buffer)?;
    test_same_model(&llm_recon, &llm);

    Ok(())
  }
  
  #[test]
  fn sketch_state_test() -> Result<(), Box<dyn Error>> {
    let mut llm_serde = LowerLinearModelSerde::new();
    let mut llm_1 = Box::new(LowerLinearModel {
      left_kp: KeyPosition{ key: -999, position: 1000 },
      right_kp: KeyPosition{ key: 999, position: 10000 },
      max_error: 123,
    });
    let llm_2 = Box::new(LowerLinearModel {
      left_kp: KeyPosition{ key: -888, position: 1000 },
      right_kp: KeyPosition{ key: 888, position: 10000 },
      max_error: 456,  // MAX ERROR
    });
    let mut llm_3 = Box::new(LowerLinearModel {
      left_kp: KeyPosition{ key: -777, position: 1000 },
      right_kp: KeyPosition{ key: 777, position: 10000 },
      max_error: 44,
    });

    // sketch many models
    let llm_buffer_1 = llm_serde.sketch(&llm_1)?;
    let llm_buffer_2 = llm_serde.sketch(&llm_2)?;
    let llm_buffer_3 = llm_serde.sketch(&llm_3)?;
    assert_eq!(llm_serde.max_error, 456);

    // reconstruct
    llm_1.max_error = 456;  // update max error
    llm_3.max_error = 456;  // update max error
    test_same_model(&llm_serde.reconstruct(&llm_buffer_1)?, &llm_1);
    test_same_model(&llm_serde.reconstruct(&llm_buffer_2)?, &llm_2);
    test_same_model(&llm_serde.reconstruct(&llm_buffer_3)?, &llm_3);

    Ok(())
  }

  fn generate_test_kps() -> [KeyPosition; 8] {
    [
      KeyPosition{ key: -100, position: 0},
      KeyPosition{ key: -50, position: 7},
      KeyPosition{ key: 0, position: 10},
      KeyPosition{ key: 5, position: 20},
      KeyPosition{ key: 15, position: 40},
      KeyPosition{ key: 25, position: 60},
      KeyPosition{ key: 30, position: 70},
      KeyPosition{ key: 31, position: 1000},
    ]
  }

  fn assert_none_buffer(buffer: MaybeKeyBuffer) -> MaybeKeyBuffer {
    assert!(buffer.is_none());
    None
  }

  fn assert_some_buffer(buffer: MaybeKeyBuffer) -> Vec<u8> {
    assert!(buffer.is_some());
    buffer.unwrap().buffer
  }
  
  #[test]
  fn greedy_corridor_test() -> Result<(), Box<dyn Error>> {
    let kps = generate_test_kps();
    let mut llm_builder = Box::new(LowerLinearGreedyCorridorBuilder::new(0));

    // start adding points
    let _model_buffer_0 = assert_none_buffer(llm_builder.consume(&kps[0])?);
    let _model_buffer_1 = assert_none_buffer(llm_builder.consume(&kps[1])?);
    let model_buffer_2 = assert_some_buffer(llm_builder.consume(&kps[2])?);
    let _model_buffer_3 = assert_none_buffer(llm_builder.consume(&kps[3])?);
    let _model_buffer_4 = assert_none_buffer(llm_builder.consume(&kps[4])?);
    let _model_buffer_5 = assert_none_buffer(llm_builder.consume(&kps[5])?);
    let _model_buffer_6 = assert_none_buffer(llm_builder.consume(&kps[6])?);
    let model_buffer_7 = assert_some_buffer(llm_builder.consume(&kps[7])?);

    // finalize the builder
    let (last_buffer, llm_serde) = llm_builder.finalize()?;
    let model_buffer_8 = assert_some_buffer(last_buffer);

    // check buffers
    test_same_model(&llm_serde.reconstruct(&model_buffer_2)?, &Box::new(LowerLinearModel {
      left_kp: KeyPosition{ key: -100, position: 0 },
      right_kp: KeyPosition{ key: -50, position: 7 },
      max_error: 0,
    }));
    test_same_model(&llm_serde.reconstruct(&model_buffer_7)?, &Box::new(LowerLinearModel {
      left_kp: KeyPosition{ key: 0, position: 10 },
      right_kp: KeyPosition{ key: 30, position: 70 },
      max_error: 0,
    }));
    test_same_model(&llm_serde.reconstruct(&model_buffer_8)?, &Box::new(LowerLinearModel {
      left_kp: KeyPosition{ key: 31, position: 1000 },
      right_kp: KeyPosition{ key: 31, position: 1000 },
      max_error: 0,
    }));
    Ok(())
  }
  
  #[test]
  fn greedy_corridor_with_error_test() -> Result<(), Box<dyn Error>> {
    let kps = generate_test_kps();
    let mut llm_builder = Box::new(LowerLinearGreedyCorridorBuilder::new(5));

    // start adding points
    let _model_buffer_0 = assert_none_buffer(llm_builder.consume(&kps[0])?);
    let _model_buffer_1 = assert_none_buffer(llm_builder.consume(&kps[1])?);
    let _model_buffer_2 = assert_none_buffer(llm_builder.consume(&kps[2])?);
    let model_buffer_3 = assert_some_buffer(llm_builder.consume(&kps[3])?);
    let _model_buffer_4 = assert_none_buffer(llm_builder.consume(&kps[4])?);
    let _model_buffer_5 = assert_none_buffer(llm_builder.consume(&kps[5])?);
    let _model_buffer_6 = assert_none_buffer(llm_builder.consume(&kps[6])?);
    let model_buffer_7 = assert_some_buffer(llm_builder.consume(&kps[7])?);

    // finalize the builder
    let (last_buffer, llm_serde) = llm_builder.finalize()?;
    let model_buffer_8 = assert_some_buffer(last_buffer);

    // check buffers
    test_same_model(&llm_serde.reconstruct(&model_buffer_3)?, &Box::new(LowerLinearModel {
      left_kp: KeyPosition{ key: -100, position: 0 },
      right_kp: KeyPosition{ key: 0, position: 10 },
      max_error: 5,
    }));
    test_same_model(&llm_serde.reconstruct(&model_buffer_7)?, &Box::new(LowerLinearModel {
      left_kp: KeyPosition{ key: 5, position: 20 },
      right_kp: KeyPosition{ key: 30, position: 70 },
      max_error: 5,
    }));
    test_same_model(&llm_serde.reconstruct(&model_buffer_8)?, &Box::new(LowerLinearModel {
      left_kp: KeyPosition{ key: 31, position: 1000 },
      right_kp: KeyPosition{ key: 31, position: 1000 },
      max_error: 5,
    }));
    Ok(())
  }
}

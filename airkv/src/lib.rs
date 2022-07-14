#[macro_use]
extern crate lazy_static;
//Top-down
pub mod cache;
pub mod common;
pub mod consistency;
pub mod compaction;
pub mod transaction;
pub mod db;
pub mod io;
pub mod lsmt;
pub mod storage;
pub mod test;

use common::error::GResult;
use db::rw_db::{DBFactory, RWDB};
use std::collections::HashMap;
use std::mem;
use storage::segment::Entry;

use jni::JNIEnv;

// These objects are what you should use as arguments to your native
// function. They carry extra lifetime information to prevent them escaping
// this context and getting used after being GC'd.
use jni::objects::{JClass, JString};

use jni::sys::{jbyteArray, jobject};

fn vector_to_array<T, const N: usize>(v: Vec<T>) -> [T; N] {
    v.try_into()
        .unwrap_or_else(|v: Vec<T>| panic!("Expected a Vec of length {} but it was {}", N, v.len()))
}

fn get_db_from_ref(env: &JNIEnv, db_ref: jbyteArray) -> Box<dyn RWDB> {
    let db_array: [u8; 16] = vector_to_array(
        env.convert_byte_array(db_ref)
            .expect("failed to convert byte array"),
    );
    unsafe { mem::transmute::<[u8; 16], Box<dyn RWDB>>(db_array) }
}

#[no_mangle]
pub extern "system" fn Java_site_ycsb_db_airkv_RWDB_newRWDB(
    env: JNIEnv,
    _class: JClass,
    input_path: JString,
    input_db_type: JString,
) -> jbyteArray {
    let path: String = env
        .get_string(input_path)
        .expect("Java_site_ycsb_db_airkv_RWDB_newRWDB: Couldn't get string for path")
        .into();

    let db_type: String = env
        .get_string(input_db_type)
        .expect("Java_site_ycsb_db_airkv_RWDB_newRWDB: Couldn't get string for path")
        .into();
    let db = DBFactory::new_rwdb_from_str(path, db_type);

    let db_array = &mut unsafe { mem::transmute::<Box<dyn RWDB>, [u8; 16]>(db) };

    env.byte_array_from_slice(db_array)
        .expect("failed to get jbytearray")
}

#[no_mangle]
pub extern "system" fn Java_site_ycsb_db_airkv_RWDB_open(
    env: JNIEnv,
    _class: JClass,
    db_ref: jbyteArray,
    block_limit: JString,
) {
    let seg_block_num_limit= env
    .get_string(block_limit)
    .expect("Java_site_ycsb_db_airkv_RWDB_open: Couldn't get string for block_limit")
    .into();
    let mut db = get_db_from_ref(&env, db_ref);
    let mut fake_props: HashMap<String, String> = HashMap::new();
    //TODO: remove this fake props, get props from parameter instead
    fake_props.insert(
        "SEG_BLOCK_NUM_LIMIT".to_string(),
        // seg_block_num_limit.to_string(),
        seg_block_num_limit,
    );
    db.open(&fake_props).expect("open failed");

    let db_array = &unsafe { mem::transmute::<Box<dyn RWDB>, [u8; 16]>(db) };
    env.byte_array_from_slice(db_array)
        .expect("failed to get jbytearray");
}

#[no_mangle]
pub extern "system" fn Java_site_ycsb_db_airkv_RWDB_close(
    env: JNIEnv,
    _class: JClass,
    db_ref: jbyteArray,
) {
    let mut db = get_db_from_ref(&env, db_ref);
    db.close().expect("close failed");
}

#[no_mangle]
pub extern "system" fn Java_site_ycsb_db_airkv_RWDB_put(
    env: JNIEnv,
    _class: JClass,
    db_ref: jbyteArray,
    key: jbyteArray,
    value: jbyteArray,
) {
    let closure = || -> GResult<()> {
        let mut db = get_db_from_ref(&env, db_ref);
        //TODO: replace convert_byte_array to avoid memory copy
        // let key_primitive = env.get_primitive_array_critical(key, ReleaseMode::NoCopyBack)?;
        // let len = key_primitive.size()? as usize;
        // let key_slice = unsafe { std::slice::from_raw_parts(key_primitive.as_ptr() as *const u8, len) };
        let key_bytes = env.convert_byte_array(key)?;
        let value_bytes = env.convert_byte_array(value)?;
        db.put(key_bytes, value_bytes)?;

        let db_array = &unsafe { mem::transmute::<Box<dyn RWDB>, [u8; 16]>(db) };

        env.byte_array_from_slice(db_array)
            .expect("failed to get jbytearray");
        Ok(())
    };
    if let Err(err) = closure() {
        println!(
            "ERROR: Java_site_ycsb_db_airkv_RWDB_put encounters error {}",
            err
        );
        let java_exception = env
            .find_class("site/ycsb/db/airkv/AirKVException")
            .expect("failed to find java exception");
        env.throw_new(java_exception, err.to_string())
            .expect("failed to throw exception");
    }
}

#[no_mangle]
pub extern "system" fn Java_site_ycsb_db_airkv_RWDB_get(
    env: JNIEnv,
    _class: JClass,
    db_ref: jbyteArray,
    key: jbyteArray,
) -> jobject {
    let closure = || -> GResult<Option<Entry>> {
        let mut db = get_db_from_ref(&env, db_ref);
        //TODO: replace convert_byte_array to avoid memory copy
        let key_bytes = env.convert_byte_array(key)?;
        let res = db.get(&key_bytes);
        let db_array = &unsafe { mem::transmute::<Box<dyn RWDB>, [u8; 16]>(db) };

        env.byte_array_from_slice(db_array)
            .expect("failed to get jbytearray");
        res
    };
    let get_res = closure();
    if let Err(err) = get_res {
        println!(
            "ERROR: Java_site_ycsb_db_airkv_RWDB_put encounters error {}",
            err
        );
        let java_exception = env
            .find_class("site/ycsb/db/airkv/AirKVException")
            .expect("failed to find java exception");
        env.throw_new(java_exception, err.to_string())
            .expect("failed to throw exception");
        //return null
        std::ptr::null_mut() as jobject
    } else {
        let entry = get_res.unwrap();
        if let Some(entry_value) = entry {
            let value_slice = entry_value.get_value_slice();
            //TODO: replace byte_array_from_slice to avoid memory copy
            env.byte_array_from_slice(value_slice)
                .expect("failed to get jbytearray")
        } else {
            //cannot find the key, return null
            std::ptr::null_mut() as jobject
        }
    }
}

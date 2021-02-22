extern crate jni;
extern crate libc;
extern crate weld;
extern crate regex;

use libc::c_void;

use jni::JNIEnv;
use jni::objects::{JByteBuffer, JString, JClass, JObject};
use jni::sys::{jstring, jlong, jint};
use jni::strings::*;

mod weld_lib;

pub use weld_lib::*;
// TODO Temporary measure. We should spin this of in a different library.
pub mod utf8lib;

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1context_1new(_: JNIEnv, _: JClass, confPtr: jlong) -> jlong {
    let conf = confPtr as weld_conf_t;
    let value = weld_context_new(conf);
    value as jlong
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1context_1memory_1usage(_: JNIEnv, _: JClass, contextPtr: jlong) -> jlong {
    let context = contextPtr as weld_context_t;
    let value = weld_context_memory_usage(context);
    value as jlong
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1context_1free(_: JNIEnv, _: JClass, contextPtr: jlong) {
    let context = contextPtr as weld_context_t;
    weld_context_free(context)
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1value_1new(_: JNIEnv, _: JClass, dataPtr: jlong) -> jlong {
    let data = dataPtr as *const c_void;
    let value = weld_value_new(data);
    value as jlong
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1value_1pointer(_: JNIEnv, _: JClass, valuePtr: jlong) -> jlong {
    let value = valuePtr as weld_value_t;
    let data = weld_value_data(value);
    data as jlong
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1value_1context(_: JNIEnv, _: JClass, valuePtr: jlong) -> jlong {
    let value = valuePtr as weld_value_t;
    let data = weld_value_context(value);
    data as jlong
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1get_1buffer_1pointer(env: JNIEnv, _: JObject, buffer: JByteBuffer) -> jlong {
    let data = env.get_direct_buffer_address(buffer);
    data.unwrap().as_ptr() as jlong
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1value_1run(_: JNIEnv, _: JClass, valuePtr: jlong) -> jlong {
    let value = valuePtr as weld_value_t;
    weld_value_run(value)
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1value_1free(_: JNIEnv, _: JClass, valuePtr: jlong) {
    let value = valuePtr as weld_value_t;
    weld_value_free(value)
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1module_1compile(env: JNIEnv, _: JClass, jcode: JString, confPtr: jlong, errorPtr: jlong) -> jlong {
    let conf = confPtr as weld_conf_t;
    let error = errorPtr as weld_error_t;
    let code = env.get_string(jcode).unwrap();
    let module = weld_module_compile(code.get_raw(), conf, error);
    module as jlong
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1module_1run(_: JNIEnv, _: JClass, modulePtr: jlong, contextPtr: jlong, inputPtr: jlong, errorPtr: jlong) -> jlong {
    let module = modulePtr as weld_module_t;
    let context = contextPtr as weld_context_t;
    let input = inputPtr as weld_value_t;
    let error = errorPtr as weld_error_t;
    let result = weld_module_run(module, context, input, error);
    let x= result as jlong;
    x
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1module_1free(_: JNIEnv, _: JClass, modulePtr: jlong) {
    let module = modulePtr as weld_module_t;
    weld_module_free(module)
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1error_1new(_: JNIEnv, _: JClass) -> jlong {
    let error = weld_error_new();
    error as jlong
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1error_1free(_: JNIEnv, _: JClass, errorPtr: jlong) {
    let error = errorPtr as weld_error_t;
    weld_error_free(error)
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1error_1code(_: JNIEnv, _: JClass, errorPtr: jlong) -> jint {
    let error = errorPtr as weld_error_t;
    weld_error_code(error) as jint
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1error_1message(env: JNIEnv, _: JClass, errorPtr: jlong) -> jstring {
    let error = errorPtr as weld_error_t;
    let message = JNIStr::from_ptr(weld_error_message(error)).to_owned();
    env.new_string(message).unwrap().into_inner()
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1conf_1new(_: JNIEnv, _: JClass) -> jlong {
    let conf = weld_conf_new();
    conf as jlong
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1conf_1free(_: JNIEnv, _: JClass, confPtr: jlong) {
    let conf = confPtr as weld_conf_t;
    weld_conf_free(conf)
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1conf_1get(env: JNIEnv, _: JClass, confPtr: jlong, jkey: JString) -> jstring {
    let conf = confPtr as weld_conf_t;
    let key = env.get_string(jkey).unwrap();
    let valuePtr = weld_conf_get(conf, key.get_raw());
    if valuePtr.is_null() {
        std::ptr::null_mut()
    } else {
        let value = JNIStr::from_ptr(valuePtr).to_owned();
        env.new_string(value).unwrap().into_inner()
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1conf_1set(env: JNIEnv, _: JClass, confPtr: jlong, jkey: JString, jvalue: JString) {
    let conf = confPtr as weld_conf_t;
    let key = env.get_string(jkey).unwrap();
    let value = env.get_string(jvalue).unwrap();
    weld_conf_set(conf, key.get_raw(), value.get_raw())
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1load_1library(env: JNIEnv, _: JClass, jfilename: JString, errorPtr: jlong) {
    let filename = env.get_string(jfilename).unwrap();
    let error = errorPtr as weld_error_t;
    weld_load_library(filename.get_raw(), error)
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn Java_org_apache_spark_weld_WeldJNI_weld_1set_1log_1level(_env: JNIEnv, _: JClass, _level_str: JString) {
    weld_set_log_level(3)
}

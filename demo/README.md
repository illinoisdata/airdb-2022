# AirIndex Demo

### How to use
- Launch Flask server:
```
cd {SOMEWHERE}/airindex/demo/server
export FLASK_APP=server
export FLASK_ENV=development
flask run
```
- Open http://127.0.0.1:5000/index.html to see demo

### Connect Rust functions with Python
```
cargo build
cd demo/server
cp ../../target/debug/libairindex.dylib .; mv libairindex.dylib airindex.so
```

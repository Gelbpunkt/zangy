#!/usr/bin/env bash
export RUSTFLAGS="-C target-cpu=native"
maturin build --no-sdist --release --strip --manylinux off --interpreter python3
pip install target/wheels/*.whl -U

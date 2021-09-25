#!/usr/bin/env bash
export RUSTFLAGS="-C target-cpu=native -Z tune-cpu=native -Z mir-opt-level=3"
maturin build --no-sdist --release --strip --manylinux off --interpreter python3
pip install target/wheels/*.whl -U --force-reinstall

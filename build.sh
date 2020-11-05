#!/usr/bin/env bash
maturin build --no-sdist --release --strip --manylinux off --interpreter python3.8
pip install target/wheels/*.whl -U

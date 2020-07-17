#!/usr/bin/env bash
maturin build --no-sdist --release --strip --manylinux off
pip install target/wheels/*.whl -U

#!/usr/bin/env bash
maturin build --release --strip --manylinux off --interpreter python3
pip install target/wheels/*.whl -U --force-reinstall

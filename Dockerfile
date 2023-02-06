FROM quay.io/pypa/manylinux_2_28_x86_64

ENV PATH /root/.cargo/bin:$PATH
# Add all supported python versions
ENV PATH /opt/python/cp311-cp311/bin/:/opt/python/cp310-cp310/bin/:/opt/python/cp36-cp36m/bin/:/opt/python/cp37-cp37m/bin/:/opt/python/cp39-cp39/bin/:$PATH
# Otherwise `cargo new` errors
ENV USER root

RUN curl -sSf https://sh.rustup.rs | sh -s -- --profile minimal --default-toolchain nightly --component rust-src -y \
    && python3 -m pip install --no-cache-dir cffi maturin \
    && mkdir /io

WORKDIR /io

ENTRYPOINT ["/opt/python/cp311-cp311/bin/maturin"]

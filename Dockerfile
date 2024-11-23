FROM ubuntu:latest
LABEL authors="karol"

RUN apt-get update && apt-get install -y \
      build-essential  \
      autoconf  \
      libtool  \
      pkg-config \
      protobuf-compiler \
      libprotobuf-dev   \
      git               \
      libprotoc-dev     \
      meson             \
      cmake             \
      wget              \
      libprotobuf-c-dev

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    lsb-release \
    wget \
    gnupg2 \
    && rm -rf /var/lib/apt/lists/*

# Add Apache Arrow's official APT source
RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && apt-get install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && apt-get update \
    && rm apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

# Install the required Apache Arrow libraries for C++ and GLib
RUN apt-get install -y --no-install-recommends \
    libarrow-dev \
    libarrow-glib-dev

# Optionally install Parquet and other optional dependencies (uncomment if needed)
 RUN apt-get install -y --no-install-recommends \
     libparquet-dev \
     libparquet-glib-dev

# protobuf c installation
RUN git clone https://github.com/protobuf-c/protobuf-c.git
RUN cd protobuf-c && ./autogen.sh && ./configure && make && make install
RUN cd protobuf-c && make check

# arrow c_lib installation
RUN wget 'https://www.apache.org/dyn/closer.lua?action=download&filename=arrow/arrow-17.0.0/apache-arrow-17.0.0.tar.gz' \
    --output-document apache-arrow-17.0.0.tar.gz
RUN tar xf apache-arrow-17.0.0.tar.gz
RUN cd apache-arrow-17.0.0 && meson setup c_glib.build c_glib --buildtype=release
RUN cd apache-arrow-17.0.0 meson compile -C c_glib.build
RUN cd apache-arrow-17.0.0 meson install -C c_glib.build

WORKDIR /app
COPY . /app
RUN mkdir -p build
WORKDIR /app/build
RUN cmake ..
RUN make

EXPOSE 8080
EXPOSE 8081


CMD ["./src/ExecutorApp"]
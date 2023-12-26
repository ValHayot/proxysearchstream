FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt -y update

RUN apt-get install -y vim g++ gfortran git vim \
    wget build-essential libncursesw5-dev libssl-dev \
    libsqlite3-dev tk-dev libgdbm-dev libc6-dev \
    libbz2-dev libffi-dev zlib1g-dev unzip software-properties-common

RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt -y install python3.11

RUN git clone --depth=100 --branch=releases/v0.21 https://github.com/spack/spack.git ~/spack
RUN git clone https://github.com/mochi-hpc/mochi-spack-packages.git ~/mochi-spack-packages

ADD ./spack.yml /root/spack.yml

RUN . ~/spack/share/spack/setup-env.sh \
    && spack env create mofka /root/spack.yml \
    && spack env activate mofka \
    && spack repo add /root/mochi-spack-packages \
    && spack compiler find \
    && spack install \
    && pip install proxystore[all] kafka-python

RUN apt-get install openjdk-11-jdk -y \
    && wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz \
    && tar xvf kafka_2.13-3.6.1.tgz \
    && mv kafka_2.13-3.6.1 /usr/local/kafka

RUN echo "/usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties &" >> ~/.bashrc
RUN echo "/usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &" >> ~/.bashrc
RUN echo ". ~/spack/share/spack/setup-env.sh" >> ~/.bashrc
RUN echo "spack env activate mofka -p" >> ~/.bashrc
SHELL ["/bin/bash -l -c"]


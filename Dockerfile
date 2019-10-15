FROM atlas/centos7-atlasos:latest
USER root
RUN source /opt/lcg/binutils/*/x86_64-*/setup.sh && source /opt/lcg/gcc/*/x86_64-*/setup.sh

RUN yum -y install gcc python3-pip python3-devel
RUN pip3 install --upgrade pip setuptools wheel
RUN pip3 install https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev6-cp36-cp36m-manylinux1_x86_64.whl
ARG BUILD_DIR=/tmpbuild

ENV LANG=en_US.utf8 LC_ALL=en_US.utf8
RUN mkdir ${BUILD_DIR}
COPY . ${BUILD_DIR}/
RUN  cd ${BUILD_DIR} && python3 setup.py bdist_wheel && pip3 install dist/*.whl && rm -rf ${BUILD_DIR}
RUN yum -y clean all
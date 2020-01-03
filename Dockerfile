FROM atlas/athena:22.0.5_2019-09-24T2128_100.0.2
USER root

ARG BUILD_DIR=/tmpbuild 
ARG ENTRYPOINT_BIN=/usr/local/bin/entrypoint
ARG CONDA_INSTALLER=/opt/conda_installer.sh
ARG CONDA_DOWNLOAD_URL=https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
ENV CONDA_HOME=/opt/miniconda
ENV RAYTHENA_CONDA_BIN=${CONDA_HOME}/bin
ENV LANG=en_US.utf8 \
    LC_ALL=en_US.utf8 \
    PATH=${RAYTHENA_CONDA_BIN}:${PATH} \
    RAYTHENA_RAY_VENV=raythena \
    RAYTHENA_PAYLOAD_VENV=pilot \
    ENTRYPOINT_BIN=${ENTRYPOINT_BIN} \
    RAYTHENA_PAYLOAD_BINDIR=/opt/pilot2
ENV RAYTHENA_CONF_DIR=${CONDA_HOME}/envs/${RAYTHENA_RAY_VENV}/conf
ENV RAYTHENA_CONFIG=${RAYTHENA_CONF_DIR}/incontainer.yaml

RUN curl -o ${CONDA_INSTALLER} ${CONDA_DOWNLOAD_URL}; \
    chmod +x ${CONDA_INSTALLER}; \
    ${CONDA_INSTALLER} -b -p ${CONDA_HOME}; \
    rm ${CONDA_INSTALLER}


RUN conda create -y -n${RAYTHENA_PAYLOAD_VENV} python=2.7; \
    conda create -y -n${RAYTHENA_RAY_VENV} python=3.7; \
    mkdir ${BUILD_DIR}

COPY . ${BUILD_DIR}/
RUN git clone https://github.com/esseivaju/pilot2.git ${RAYTHENA_PAYLOAD_BINDIR}

RUN mkdir -p /opt/yampl/lib; \
    mkdir /opt/yampl/python-lib; \
    cp ${BUILD_DIR}/yampl/lib/libyampl.so /opt/yampl/lib; \
    cp ${BUILD_DIR}/yampl/python-lib/yampl.so /opt/yampl/python-lib

COPY entrypoint.sh ${ENTRYPOINT_BIN}
RUN chmod +x ${ENTRYPOINT_BIN}; \
    source ${RAYTHENA_CONDA_BIN}/activate; \
    conda activate ${RAYTHENA_RAY_VENV}; \
    source /opt/lcg/binutils/*/x86_64-*/setup.sh; \
    source /opt/lcg/gcc/*/x86_64-*/setup.sh; \
    cd ${BUILD_DIR}; \
    python setup.py bdist_wheel; \
    pip install dist/*.whl; \
    rm -rf ${BUILD_DIR}

ENTRYPOINT ["/usr/local/bin/entrypoint"]

FROM ubuntu:22.04

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive && apt-get -y install pip git curl zip

WORKDIR /workspaces/aoupy2

RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="${PATH}:/root/.local/bin"
RUN ~/.local/bin/poetry install

ENV force_color_prompt=yes

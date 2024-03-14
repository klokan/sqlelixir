FROM python:3.12

RUN python -m venv /usr/local/lib/poetry \
    && /usr/local/lib/poetry/bin/pip install poetry==1.8.2 \
    && ln -s /usr/local/lib/poetry/bin/poetry /usr/local/bin/poetry

RUN groupadd --gid 1000 debian \
    && useradd --uid 1000 --gid 1000 -m debian \
    && chown debian:debian /opt
USER debian

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV VIRTUAL_ENV=/opt/venv

COPY --chown=debian:debian pyproject.toml poetry.lock /opt/
RUN cd /opt && poetry install --all-extras

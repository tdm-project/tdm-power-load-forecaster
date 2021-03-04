ARG APP_HOME=/opt/power-load-forecasting/

# ============================================================================ #
# Final stage: lean, intended to be used as execute container                  #
# docker build --target=final -f docker/Dockerfile \                           #
#              -t tdmproject/power-load-forecaster .                           #
# ============================================================================ #
FROM tdmproject/alpine-python-3:arm32v7-3.13-sklearn AS final

LABEL maintainer="Andrea Pinna <andrea dot pinna at crs4 dot it>"

ARG APP_HOME

# Install required packages
RUN apk add --no-cache git && \
    mkdir -p ${APP_HOME} && \
    cd ${APP_HOME} && \
    virtualenv --system-site-packages venv && \
    . venv/bin/activate && \
    pip3 install --no-cache-dir influxdb

ENV APP_HOME=${APP_HOME}

WORKDIR ${APP_HOME}

COPY src src

ENTRYPOINT ["src/entrypoint.sh"]

CMD [ "-c", "/opt/configs/tdm.conf" ]

# ============================================================================ #
# Testing Stage: final stage for testing purposes                              #
# docker build --target=testing -f docker/Dockerfile \                         #
#              -t tdmproject/power-load-forecaster .                           #
# ============================================================================ #
FROM final AS testing

ARG APP_HOME

ENV APP_HOME=${APP_HOME}

WORKDIR ${APP_HOME}

COPY tests tests

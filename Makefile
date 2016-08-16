PROJECT = amqp_client

DEPS = rabbit_common

ERLC_OPTS = +debug_info \
            +compressed \
            +report \
            +warn_export_all \
            +warn_export_vars \
            +warn_shadow_vars \
            +warn_unused_function \
            +warn_deprecated_function \
            +warn_obsolete_guard \
            +warn_unused_import \
            +nowarn_export_vars \
            +warnings_as_errors


COMPILE_FIRST = amqp_gen_consumer \
                amqp_gen_connection

dep_rabbit_common = git git://github.com/woldan/rabbit_common.git rabbitmq-3.5.6-scoped

include erlang.mk

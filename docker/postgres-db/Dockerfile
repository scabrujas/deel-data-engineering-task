FROM postgres:15

# Instalar o pg_cron
USER root
RUN apt-get update && apt-get install -y postgresql-15-cron

RUN echo "shared_preload_libraries = 'pg_cron'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "cron.database_name = 'finance_db'" >> /usr/share/postgresql/postgresql.conf.sample

USER postgres
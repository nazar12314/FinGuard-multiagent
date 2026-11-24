-- Runs once, when the data volume is first initialised. FinGuard's own schemas and
-- extensions are created by its migrations, so tests on a bare Postgres get them too.
CREATE DATABASE langfuse;
CREATE DATABASE apicurio;

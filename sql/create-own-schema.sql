CREATE SCHEMA IF NOT EXISTS "@@ownSchema";

CREATE EXTENSION IF NOT EXISTS pgcrypto
    SCHEMA "@@ownSchema"
    VERSION "1.3";
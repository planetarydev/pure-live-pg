CREATE SCHEMA IF NOT EXISTS "@@ownSchema";

CREATE EXTENSION IF NOT EXISTS pgcrypto
    SCHEMA "@@pgcryptoSchema"
    VERSION "1.3";

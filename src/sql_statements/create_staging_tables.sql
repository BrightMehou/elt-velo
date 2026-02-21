CREATE TABLE IF NOT EXISTS staging_raw (
    nom varchar NOT NULL,
    date date NOT NULL,
    data jsonb NOT NULL,
    PRIMARY KEY (nom, date)
);
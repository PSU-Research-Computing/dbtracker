-- Table: stats

-- DROP TABLE stats;

CREATE TABLE stats
(
  datetime timestamp with time zone NOT NULL DEFAULT now(),
  db_name character varying(255) NOT NULL,
  schema_name character varying(255) NOT NULL,
  table_name character varying(255) NOT NULL,
  row_count integer NOT NULL,
  CONSTRAINT stats_pkey PRIMARY KEY (datetime, db_name, schema_name, table_name)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE stats
  OWNER TO root;

// Questo file JS crea automaticamente tutta la struttura dei file Dataform risolvendo la dipendenza circolare

const config_global = {
  project: "ornate-crossbar-499808-h8",
  location: "europe-west8",
  stg_schema: "DATAVAULT_STG_CAM",
  dv_schema: "DATAVAULT_CAM",
  dwh_schema: "DATAWAREHOUSE_CAM"
};

// Stringhe fisse per i target incrementali (evitano la dipendenza circolare nel grafo di Dataform)
const target_h_agenzia = `\`${config_global.project}.${config_global.dv_schema}.H_AGENZIA\``;
const target_h_conto_gl = `\`${config_global.project}.${config_global.dv_schema}.H_CONTO_GL\``;
const target_l_saldo_gl = `\`${config_global.project}.${config_global.dv_schema}.L_SALDO_GL_CONT\``;

// 1. STAGING LAYER
declare({ schema: config_global.stg_schema, name: "STG_AGENZIA" });
declare({ schema: config_global.stg_schema, name: "STG_CONTO_GL" });
declare({ schema: config_global.stg_schema, name: "STG_SALDO_GL" });

// 2. CORE DATA VAULT LAYER

// Hub Agenzia
publish("H_AGENZIA", {
  type: "incremental",
  schema: config_global.dv_schema,
  uniqueKey: ["SEQ_AGENZIA"],
  assertions: { uniqueKey: ["COD_AGENZIA"], nonNull: ["SEQ_AGENZIA", "COD_AGENZIA"] }
}).query(ctx => `
  WITH staging_filtrata AS (
    SELECT DISTINCT
      TRIM(COD_AGENZIA) AS COD_AGENZIA,
      PARSE_DATE('%Y-%m-%d', '${dataform.projectConfig.vars.V_DAT_CARICAMENTO}') AS DAT_CARICAMENTO,
      SOURCE
    FROM ${ctx.ref(config_global.stg_schema, "STG_AGENZIA")}
    WHERE COD_AGENZIA IS NOT NULL
  ),
  nuovi_record AS (
    SELECT s.COD_AGENZIA, s.DAT_CARICAMENTO, s.SOURCE
    FROM staging_filtrata s
    ${ctx.when(ctx.incremental(), 
      `LEFT JOIN ${target_h_agenzia} t ON s.COD_AGENZIA = t.COD_AGENZIA WHERE t.COD_AGENZIA IS NULL`,
      `WHERE true`
    )}
  ),
  max_sequenza AS (
    SELECT ${ctx.when(ctx.incremental(), `COALESCE(MAX(SEQ_AGENZIA), 0)`, `0`)} AS max_seq 
    ${ctx.when(ctx.incremental(), `FROM ${target_h_agenzia}`)}
  )
  SELECT
    (m.max_seq + ROW_NUMBER() OVER(ORDER BY n.COD_AGENZIA)) AS SEQ_AGENZIA,
    n.COD_AGENZIA,
    n.DAT_CARICAMENTO,
    n.SOURCE
  FROM nuovi_record n
  CROSS JOIN max_sequenza m
`);

// Hub Conto GL
publish("H_CONTO_GL", {
  type: "incremental",
  schema: config_global.dv_schema,
  uniqueKey: ["SEQ_CONTO_GL"],
  assertions: { uniqueKey: ["COD_CONTO_GL"], nonNull: ["SEQ_CONTO_GL", "COD_CONTO_GL"] }
}).query(ctx => `
  WITH staging_filtrata AS (
    SELECT DISTINCT
      TRIM(COD_CONTO_GL) AS COD_CONTO_GL,
      PARSE_DATE('%Y-%m-%d', '${dataform.projectConfig.vars.V_DAT_CARICAMENTO}') AS DAT_CARICAMENTO,
      SOURCE
    FROM ${ctx.ref(config_global.stg_schema, "STG_CONTO_GL")}
    WHERE COD_CONTO_GL IS NOT NULL
  ),
  nuovi_record AS (
    SELECT s.COD_CONTO_GL, s.DAT_CARICAMENTO, s.SOURCE
    FROM staging_filtrata s
    ${ctx.when(ctx.incremental(), 
      `LEFT JOIN ${target_h_conto_gl} t ON s.COD_CONTO_GL = t.COD_CONTO_GL WHERE t.COD_CONTO_GL IS NULL`,
      `WHERE true`
    )}
  ),
  max_sequenza AS (
    SELECT ${ctx.when(ctx.incremental(), `COALESCE(MAX(SEQ_CONTO_GL), 0)`, `0`)} AS max_seq 
    ${ctx.when(ctx.incremental(), `FROM ${target_h_conto_gl}`)}
  )
  SELECT
    (m.max_seq + ROW_NUMBER() OVER(ORDER BY n.COD_CONTO_GL)) AS SEQ_CONTO_GL,
    n.COD_CONTO_GL,
    n.DAT_CARICAMENTO,
    n.SOURCE
  FROM nuovi_record n
  CROSS JOIN max_sequenza m
`);

// Link Saldi
publish("L_SALDO_GL_CONT", {
  type: "incremental",
  schema: config_global.dv_schema,
  uniqueKey: ["SEQ_SALDO_GL_CONT"]
}).query(ctx => `
  WITH staging_chiavi AS (
    SELECT DISTINCT
      hc.SEQ_CONTO_GL,
      ha.SEQ_AGENZIA,
      TRIM(s.COD_DIVISA) AS COD_DIVISA,
      TRIM(s.COD_FIN_YEAR) AS COD_FIN_YEAR,
      TRIM(s.COD_PERIOD_CODE) AS COD_PERIOD_CODE,
      PARSE_DATE('%Y-%m-%d', '${dataform.projectConfig.vars.V_DAT_CARICAMENTO}') AS DAT_CARICAMENTO,
      s.SOURCE
    FROM ${ctx.ref(config_global.stg_schema, "STG_SALDO_GL")} s
    INNER JOIN ${ctx.ref(config_global.dv_schema, "H_CONTO_GL")} hc ON TRIM(s.COD_CONTO_GL) = hc.COD_CONTO_GL
    INNER JOIN ${ctx.ref(config_global.dv_schema, "H_AGENZIA")} ha ON TRIM(s.COD_AGENZIA) = ha.COD_AGENZIA
  ),
  nuovi_link AS (
    SELECT s.* FROM staging_chiavi s
    ${ctx.when(ctx.incremental(), 
      `LEFT JOIN ${target_l_saldo_gl} t ON s.SEQ_CONTO_GL = t.SEQ_CONTO_GL 
        AND s.SEQ_AGENZIA = t.SEQ_AGENZIA 
        AND s.COD_DIVISA = t.COD_DIVISA 
        AND s.COD_FIN_YEAR = t.COD_FIN_YEAR 
        AND s.COD_PERIOD_CODE = t.COD_PERIOD_CODE
       WHERE t.SEQ_SALDO_GL_CONT IS NULL`,
      `WHERE true`
    )}
  ),
  max_sequenza AS (
    SELECT ${ctx.when(ctx.incremental(), `COALESCE(MAX(SEQ_SALDO_GL_CONT), 0)`, `0`)} AS max_seq 
    ${ctx.when(ctx.incremental(), `FROM ${target_l_saldo_gl}`)}
  )
  SELECT
    (m.max_seq + ROW_NUMBER() OVER(ORDER BY n.SEQ_CONTO_GL, n.SEQ_AGENZIA)) AS SEQ_SALDO_GL_CONT,
    n.SEQ_CONTO_GL,
    n.SEQ_AGENZIA,
    n.COD_DIVISA,
    n.COD_FIN_YEAR,
    n.COD_PERIOD_CODE,
    n.DAT_CARICAMENTO,
    n.SOURCE
  FROM nuovi_link n
  CROSS JOIN max_sequenza m
`);

// Satelliti
publish("S_CONTO_GL", { type: "incremental", schema: config_global.dv_schema, uniqueKey: ["SEQ_CONTO_GL", "DAT_INIZIO_VALIDITA"] }).query(ctx => `
  SELECT DISTINCT h.SEQ_CONTO_GL, TRIM(s.COD_CONTO_GL) AS COD_CONTO_GL, TRIM(s.DESC_CONTO_GL) AS DESC_CONTO_GL, TRIM(s.TIP_CONTO_GL) AS TIPO_CONTO_GL, PARSE_DATE('%Y-%m-%d', '${dataform.projectConfig.vars.V_DAT_CARICAMENTO}') AS DAT_INIZIO_VALIDITA, CAST(NULL AS DATE) AS DAT_FINE_VALIDITA, s.SOURCE
  FROM ${ctx.ref(config_global.stg_schema, "STG_CONTO_GL")} s INNER JOIN ${ctx.ref(config_global.dv_schema, "H_CONTO_GL")} h ON TRIM(s.COD_CONTO_GL) = h.COD_CONTO_GL
`);

publish("S_SALDO_GL_CONT", { type: "incremental", schema: config_global.dv_schema, uniqueKey: ["SEQ_SALDO_GL_CONT", "DAT_INIZIO_VALIDITA"] }).query(ctx => `
  SELECT DISTINCT l.SEQ_SALDO_GL_CONT, PARSE_DATE('%Y-%m-%d', '${dataform.projectConfig.vars.V_DAT_CARICAMENTO}') AS DAT_INIZIO_VALIDITA, CAST(NULL AS DATE) AS DAT_FINE_VALIDITA, s.SOURCE, s.IMP_SALDO_DARE, s.IMP_SALDO_DARE_EUR, s.IMP_SALDO_AVERE, s.IMP_SALDO_AVERE_EUR
  FROM ${ctx.ref(config_global.stg_schema, "STG_SALDO_GL")} s
  INNER JOIN ${ctx.ref(config_global.dv_schema, "H_CONTO_GL")} hc ON TRIM(s.COD_CONTO_GL) = hc.COD_CONTO_GL
  INNER JOIN ${ctx.ref(config_global.dv_schema, "H_AGENZIA")} ha ON TRIM(s.COD_AGENZIA) = ha.COD_AGENZIA
  INNER JOIN ${ctx.ref(config_global.dv_schema, "L_SALDO_GL_CONT")} l ON hc.SEQ_CONTO_GL = l.SEQ_CONTO_GL AND ha.SEQ_AGENZIA = l.SEQ_AGENZIA AND TRIM(s.COD_DIVISA) = l.COD_DIVISA AND TRIM(s.COD_FIN_YEAR) = l.COD_FIN_YEAR AND TRIM(s.COD_PERIOD_CODE) = l.COD_PERIOD_CODE
`);

// 3. DATAWAREHOUSE LAYER
publish("DIM_AGENZIA", { type: "table", schema: config_global.dwh_schema, assertions: { uniqueKey: ["COD_AGENZIA", "DAT_INIZIO_VALIDITA"] } }).query(ctx => `
  SELECT ROW_NUMBER() OVER(ORDER BY h.COD_AGENZIA) AS ID_AGENZIA, h.COD_AGENZIA, h.DAT_CARICAMENTO AS DAT_INIZIO_VALIDITA, PARSE_DATE('%Y-%m-%d', '2400-01-01') AS DAT_FINE_VALIDITA, s.DESC_AGENZIA, s.COD_ABI, s.COD_CAB, s.COD_PROVINCIA, s.COD_NAZIONE, s.DESC_LOCALITA, s.COD_CIN
  FROM ${ctx.ref(config_global.dv_schema, "H_AGENZIA")} h LEFT JOIN ${ctx.ref(config_global.stg_schema, "STG_AGENZIA")} s ON h.COD_AGENZIA = TRIM(s.COD_AGENZIA)
`);

publish("DIM_CONTO_GL", { type: "table", schema: config_global.dwh_schema, assertions: { uniqueKey: ["COD_CONTO_GL", "DAT_INIZIO_VALIDITA"] } }).query(ctx => `
  SELECT ROW_NUMBER() OVER(ORDER BY h.COD_CONTO_GL) AS ID_CONTO_GL, h.COD_CONTO_GL, s.DAT_INIZIO_VALIDITA, s.DAT_FINE_VALIDITA, s.DESC_CONTO_GL, s.TIPO_CONTO_GL
  FROM ${ctx.ref(config_global.dv_schema, "H_CONTO_GL")} h LEFT JOIN ${ctx.ref(config_global.dv_schema, "S_CONTO_GL")} s ON h.SEQ_CONTO_GL = s.SEQ_CONTO_GL
`);

publish("FACT_SALDI_GL", { type: "table", schema: config_global.dwh_schema }).query(ctx => `
  SELECT PARSE_DATE('%Y-%m-%d', '${dataform.projectConfig.vars.V_DAT_CARICAMENTO}') AS DAT_RIFERIMENTO, dc.ID_CONTO_GL, da.ID_AGENZIA, l.COD_DIVISA, hc.COD_CONTO_GL, ha.COD_AGENZIA, s.IMP_SALDO_DARE, s.IMP_SALDO_DARE_EUR, s.IMP_SALDO_AVERE, s.IMP_SALDO_AVERE_EUR
  FROM ${ctx.ref(config_global.dv_schema, "L_SALDO_GL_CONT")} l
  INNER JOIN ${ctx.ref(config_global.dv_schema, "S_SALDO_GL_CONT")} s ON l.SEQ_SALDO_GL_CONT = s.SEQ_SALDO_GL_CONT
  INNER JOIN ${ctx.ref(config_global.dv_schema, "H_CONTO_GL")} hc ON l.SEQ_CONTO_GL = hc.SEQ_CONTO_GL
  INNER JOIN ${ctx.ref(config_global.dv_schema, "H_AGENZIA")} ha ON l.SEQ_AGENZIA = ha.SEQ_AGENZIA
  LEFT JOIN ${ctx.ref(config_global.dwh_schema, "DIM_CONTO_GL")} dc ON hc.COD_CONTO_GL = dc.COD_CONTO_GL
  LEFT JOIN ${ctx.ref(config_global.dwh_schema, "DIM_AGENZIA")} da ON ha.COD_AGENZIA = da.COD_AGENZIA
`);
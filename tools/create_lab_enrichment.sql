CREATE TABLE IF NOT EXISTS lab_enrichment (
  qr_code    TEXT NOT NULL,             -- normalized QR, e.g. ABCD-1234
  param      TEXT NOT NULL,             -- e.g. "MgO", "P2O5", "Cd"
  value      TEXT,                      -- the number as text (we can cast later)
  unit       TEXT,                      -- "%", "mg/kg", etc.
  user_id    TEXT,                      -- WHO uploaded it (Keycloak userId or service id)
  raw_row    TEXT,                      -- optional: original CSV row as JSON
  updated_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (qr_code, param)
);
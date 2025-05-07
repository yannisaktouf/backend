// db.js
const { Pool } = require('pg');

// Si DATABASE_URL est défini (en prod sur Railway), on l’utilise,
// sinon on tombe sur ta config locale.
const connectionString = process.env.DATABASE_URL || 
  'postgresql://workflow_promo_user:3R4UXo5sAPn4Kr5maxplL9Otjj6VplHB@dpg-d0dlh22dbo4c738meg10-a.oregon-postgres.render.com/workflow_promo';

const pool = new Pool({
  connectionString,

  ssl: process.env.DATABASE_URL 
    ? { rejectUnauthorized: false } 
    : false,
});

module.exports = pool;

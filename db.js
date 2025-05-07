// db.js
const { Pool } = require('pg');

// Si DATABASE_URL est défini (en prod sur Railway), on l’utilise,
// sinon on tombe sur ta config locale.
const connectionString = process.env.DATABASE_URL || 
  'postgres://postgres:123456@localhost:5432/Workflow_Promo';

const pool = new Pool({
  connectionString,

  ssl: process.env.DATABASE_URL 
    ? { rejectUnauthorized: false } 
    : false,
});

module.exports = pool;

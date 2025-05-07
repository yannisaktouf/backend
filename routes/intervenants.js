const express = require('express');
const router = express.Router();
const pool = require('../db');

// GET un intervenant par nom
router.get('/', async (req, res) => {
  const { name } = req.query;

  if (!name) {
    return res.status(400).json({ error: 'Nom manquant dans la requête' });
  }

  try {
    const result = await pool.query(
      `SELECT id, name, email FROM intervenants WHERE LOWER(name) = LOWER($1) LIMIT 1`,
      [name]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Intervenant non trouvé' });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error('Erreur API intervenant par nom :', err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

module.exports = router;

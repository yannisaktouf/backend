const express = require('express');
const router = express.Router();
const pool = require('../db');
// const { sendEmailToNextIntervenant } = require('./email'); // mis en pause pour le moment

// GET toutes les promos
router.get('/', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        p.id,
        p.title,
        p.start_date,
        p.end_date,
        p.status,
        p.promo_code,
        p.date_cloture_game,
        COUNT(e.id) AS total_steps,
        SUM(CASE WHEN e.status = 'Terminé' THEN 1 ELSE 0 END) AS steps_finished,
        ROUND(
          CASE 
            WHEN COUNT(e.id) = 0 THEN 0
            ELSE (SUM(CASE WHEN e.status = 'Terminé' THEN 1 ELSE 0 END) * 100.0 / COUNT(e.id))
          END,
        2) AS progress_percent
      FROM promos p
      LEFT JOIN etapes e ON p.id = e.promo_id
      GROUP BY p.id
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET détails d'une promo
router.get('/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const result = await pool.query(`
      SELECT
        p.id,
        p.title,
        p.start_date,
        p.end_date,
        p.status,
        p.promo_code,
        p.date_cloture_game,
        COUNT(e.id) AS total_steps,
        SUM(CASE WHEN e.status = 'Terminé' THEN 1 ELSE 0 END) AS steps_finished,
        ROUND(
          CASE 
            WHEN COUNT(e.id) = 0 THEN 0
            ELSE (SUM(CASE WHEN e.status = 'Terminé' THEN 1 ELSE 0 END) * 100.0 / COUNT(e.id))
          END,
        2) AS progress_percent
      FROM promos p
      LEFT JOIN etapes e ON p.id = e.promo_id
      WHERE p.id = $1
      GROUP BY p.id
    `, [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Promo non trouvée' });
    }

    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET étapes d'une promo
router.get('/:promoId/etapes', async (req, res) => {
  const { promoId } = req.params;
  try {
    const result = await pool.query(`
      SELECT 
        e.id,
        e.name AS etape_name,
        e.status AS etape_status,
        i.name AS responsable_name,
        i.email AS responsable_email
      FROM etapes e
      JOIN intervenants i ON e.intervenant_id = i.id
      WHERE e.promo_id = $1
      ORDER BY e.ordre
    `, [promoId]);

    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET toutes les sous-étapes d'une promo
router.get('/:promoId/sous_etapes', async (req, res) => {
  const { promoId } = req.params;
  try {
    const result = await pool.query(`
      SELECT 
        se.id AS sous_etape_id,
        se.name AS sous_etape_name,
        se.status AS sous_etape_status,
        e.name AS etape_name
      FROM sous_etapes se
      JOIN etapes e ON se.etape_id = e.id
      WHERE e.promo_id = $1
      ORDER BY se.ordre
    `, [promoId]);

    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET promos pour un intervenant : promos où il a une étape non terminée
router.get('/intervenants/:intervenantId/promos', async (req, res) => {
  const { intervenantId } = req.params;

  try {
    const result = await pool.query(`
      SELECT DISTINCT
        p.id,
        p.title,
        p.start_date,
        p.end_date,
        p.status,
        p.promo_code,
        p.date_cloture_game
      FROM promos p
      JOIN etapes e ON p.id = e.promo_id
      WHERE e.intervenant_id = $1
        AND e.status != 'Terminé'
      ORDER BY p.start_date DESC;
    `, [intervenantId]);

    res.json(result.rows);
  } catch (err) {
    console.error('Erreur API intervenant promos :', err);
    res.status(500).json({ error: 'Erreur lors de la récupération des promotions de l’intervenant.' });
  }
});


// GET sous-étapes d'une étape
router.get('/:promoId/etapes/:etapeId/sous_etapes', async (req, res) => {
  const { promoId, etapeId } = req.params;
  try {
    const result = await pool.query(`
      SELECT 
        se.id AS sous_etape_id,
        se.name AS sous_etape_name,
        se.status AS sous_etape_status
      FROM sous_etapes se
      WHERE se.promo_id = $1 AND se.etape_id = $2
      ORDER BY se.ordre
    `, [promoId, etapeId]);

    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PUT mettre à jour le statut d'une étape
router.put('/:promoId/etapes/:etapeId', async (req, res) => {
  const { promoId, etapeId } = req.params;
  const { statut } = req.body;

  try {
    const result = await pool.query(
      `UPDATE etapes SET status = $1 WHERE id = $2 AND promo_id = $3 RETURNING *`,
      [statut, etapeId, promoId]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ error: "Étape non trouvée ou promo incorrecte" });
    }

    res.json({ message: "Statut de l'étape mis à jour", etape: result.rows[0] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PUT mettre à jour le statut d'une sous-étape
router.put('/:promoId/etapes/:etapeId/sous_etapes/:sousEtapeId', async (req, res) => {
  const { promoId, etapeId, sousEtapeId } = req.params;
  const { status } = req.body;

  try {
    const result = await pool.query(`
      UPDATE sous_etapes
      SET status = $1
      WHERE id = $2 AND promo_id = $3 AND etape_id = $4
      RETURNING *
    `, [status, sousEtapeId, promoId, etapeId]);

    if (result.rowCount === 0) {
      return res.status(404).json({ error: "Sous-étape non trouvée" });
    }

    res.json({ message: "Statut de la sous-étape mis à jour", sous_etape: result.rows[0] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;

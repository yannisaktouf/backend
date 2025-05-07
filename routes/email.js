// routes/mail.js
// Étape 1 - Import des modules nécessaires
const express = require('express');
const nodemailer = require('nodemailer');
const fs = require('fs');

// Étape 2 - Lecture des credentials pour l'authentification SMTP
let credentials;
try {
  credentials = JSON.parse(
    fs.readFileSync('C:/Users/aktoufy/Documents/credentials.json', 'utf8')
  );
} catch (err) {
  console.error('Impossible de lire credentials.json :', err);
  process.exit(1);
}

// Étape 3 - Création d'un transporteur Nodemailer
const transporter = nodemailer.createTransport({
  host: 'faxsmtp.carrefour.com',
  port: 25,
  secure: false, // pas de TLS
  auth: {
    user: credentials.sender_email,
    pass: credentials.password,
  },
});

// Étape 4 - Fonction utilitaire pour envoyer un email
async function sendEmail(to, subject, text) {
  const mailOptions = {
    from: credentials.sender_email,
    to,
    subject,
    text,
    html: `<p>${text.replace(/\n/g, '<br>')}</p>`,
  };

  try {
    await transporter.sendMail(mailOptions);
    console.log(`Email envoyé à ${to} (sujet: ${subject})`);
  } catch (error) {
    console.error('Erreur envoi email :', error);
    throw error;
  }
}

// Étape 5 - Création du router Express
const router = express.Router();

// Étape 6 - Route POST /api/send-mail
// Reçoit JSON { email, subject, message }
router.post('/send-mail', async (req, res) => {
  const { email, subject, message } = req.body;
  if (!email || !subject || !message) {
    return res.status(400).json({
      error: 'Champs email, subject et message obligatoires',
    });
  }

  const text = message;

  try {
    await sendEmail(email, subject, text);
    return res.json({ success: true, info: 'Email envoyé' });
  } catch (err) {
    return res.status(500).json({ error: 'Échec de l’envoi du mail' });
  }
});

// Étape 7 - Export du routeur pour montage dans l'app principale
module.exports = router;

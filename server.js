// server.js 

const express            = require('express');
const cors               = require('cors');
const promosRoutes       = require('./routes/promos');
const intervenantsRoutes = require('./routes/intervenants');
const mailRoutes         = require('./routes/email');

const app = express();

const FRONTEND_URL = process.env.FRONTEND_URL || '*';
app.use(cors({ origin: FRONTEND_URL }));

app.use(express.json());

// Monter en premier vos routes métier
app.use('/api/intervenants', intervenantsRoutes);
app.use('/api/promos', promosRoutes);
// 👉 monter le router mail sous /api
app.use('/api', mailRoutes);

app.get('/', (req, res) => {
  res.send('API is running!');
});

// Utilise process.env.PORT en prod, sinon 3000 en local
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API démarrée sur http://localhost:${PORT}`);
});

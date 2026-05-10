const express = require('express');
const { Pool } = require('pg');
require('dotenv').config();

const app = express();
app.use(express.json());

const pool = new Pool({
  host:     process.env.DB_HOST     || 'localhost',
  port:     parseInt(process.env.DB_PORT) || 5433,
  user:     process.env.DB_USER     || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres',
  database: process.env.DB_NAME     || 'tp9_kafka',
});

pool.connect((err, client, release) => {
  if (err) {
    console.error('ERREUR connexion PostgreSQL:', err.message);
    process.exit(1);
  }
  console.log('PostgreSQL connecte avec succes');
  release();
});

app.get('/messages', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT * FROM kafka_messages ORDER BY id DESC'
    );
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/messages/:id', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT * FROM kafka_messages WHERE id = $1',
      [req.params.id]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Message non trouve' });
    }
    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

const port = 3000;
app.listen(port, () => {
  console.log('API REST en cours d execution sur le port ' + port);
});

process.on('unhandledRejection', (err) => {
  console.error('Erreur non geree:', err.message);
});
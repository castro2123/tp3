const express = require("express");
const multer = require("multer");
const csv = require("csv-parser");
const fs = require("fs");
const axios = require("axios");
const { Pool } = require("pg");
const js2xmlparser = require("js2xmlparser");
const { v4: uuid } = require("uuid");

const app = express();
const upload = multer({ dest: "uploads/" });

const pool = new Pool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT
});

app.post("/convert", upload.single("file"), async (req, res) => {
  const { ID_Requisicao, MAPPER_VERSION, WEBHOOK_URL } = req.body;
  const idDoc = uuid();
  const rows = [];

  try {
    fs.createReadStream(req.file.path)
      .pipe(csv())
      .on("data", row => rows.push(row))
      .on("end", async () => {
        const xml = js2xmlparser.parse("Acoes", { Acao: rows });

        await pool.query(
          "INSERT INTO XML_Documents (id, xml_documento, mapper_version) VALUES ($1,$2,$3)",
          [idDoc, xml, MAPPER_VERSION]
        );

        await axios.post(WEBHOOK_URL, {
          ID_Requisicao,
          Status: "OK",
          ID_Documento: idDoc
        });

        res.json({ idDoc });
      });

  } catch (e) {
    await axios.post(WEBHOOK_URL, {
      ID_Requisicao,
      Status: "ERRO_PERSISTENCIA",
      ID_Documento: null
    });
    res.status(500).send(e.message);
  }
});

app.listen(8000, () => console.log("XML Service online"));

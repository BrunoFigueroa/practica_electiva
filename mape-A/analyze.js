import express from "express";
import axios from "axios";
import fs from "fs";

const app = express();
app.use(express.json());

const PLAN_URL = "http://mape-p:3002/plan"; // servicio Plan
const OUTPUT_FILE = "/app/output/metrics.json";

// Acumuladores para calcular promedio por segundo
let buffer = [];
let metricsLog = [];

app.post("/analyze", (req, res) => {
  const { produced, rate, latency } = req.body;

  buffer.push({ produced: Number(produced), rate: Number(rate), latency: Number(latency) });

  res.sendStatus(200);
});

// Cada segundo, procesar buffer y enviar promedio a Plan
setInterval(async () => {
  if (buffer.length === 0) return;

  const sumProduced = buffer.reduce((sum, m) => sum + m.produced, 0);
  const sumRate = buffer.reduce((sum, m) => sum + m.rate, 0);
  const sumLatency = buffer.reduce((sum, m) => sum + m.latency, 0);
  const count = buffer.length;

  const avgProduced = sumProduced / count;
  const avgRate = sumRate / count;
  const avgLatency = sumLatency / count;

  const timestamp = new Date().toISOString();

  console.log(
    `Analyze: promedio del último segundo | produced=${avgProduced.toFixed(
      2
    )} | rate=${avgRate.toFixed(2)} | latency=${avgLatency.toFixed(3)}`
  );

  // Guardar en JSON
  metricsLog.push({
    timestamp,
    produced: avgProduced,
    rate: avgRate,
    latency: avgLatency,
    parallelism: -1 // no disponible en Analyze
  });

  try {
    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(metricsLog, null, 2));
  } catch (err) {
    console.error("Error escribiendo metrics.json:", err.message);
  }

  // Enviar a Plan
  try {
    await axios.post(PLAN_URL, {
      produced: avgProduced,
      rate: avgRate,
      latency: avgLatency,
    });
  } catch (err) {
    console.error("Error enviando a Plan:", err.message);
  }

  buffer = []; // limpiar buffer para el próximo segundo
}, 1000);

app.listen(3001, () => console.log("Analyze corriendo en 3001"));

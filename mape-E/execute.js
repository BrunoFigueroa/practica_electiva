import express from "express";
import fetch from "node-fetch";
import fs from "fs";
import FormData from "form-data";

const app = express();
app.use(express.json());

const FLINK = "http://flink-jobmanager:8081";
let currentJobId = null;
let JAR_ID = null;
let currentParallelism = 1;
let lastScaleTs = 0;

const SCALE_COOLDOWN_MS = 30000; // 30 segundos
const MIN_PARALLELISM = 1;
const MAX_PARALLELISM = 8;

async function uploadJar() {
  const form = new FormData();
  form.append("jarfile", fs.createReadStream("./app.jar"));
  const res = await fetch(`${FLINK}/jars/upload`, { method: "POST", body: form });
  const data = await res.json();
  JAR_ID = data.files?.[0]?.id || data.filename?.split('/').pop();
  console.log("JAR subido con ID:", JAR_ID);
}

async function deployJob(parallelism, jarId) {
  const res = await fetch(`${FLINK}/jars/${jarId}/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ entryClass: "flink.job.NthPrimeJob", parallelism })
  });
  const data = await res.json();
  return data.jobid || data.jobID || data['jobid'];
}

async function cancelJob(jobId) {
  if (!jobId) return;
  await fetch(`${FLINK}/jobs/${jobId}?mode=cancel`, { method: "PATCH" });
}

// Subir JAR inicial
async function init() {
  await uploadJar();
  currentJobId = await deployJob(currentParallelism, JAR_ID);
  console.log("Job inicial desplegado con paralelismo", currentParallelism);
}

init().catch(err => { console.error(err); process.exit(1); });

// Endpoint HTTP para recibir decisiones de Plan
app.post("/execute", async (req, res) => {
  const { action } = req.body;
  const now = Date.now();

  // Verificar cooldown
  if (now - lastScaleTs < SCALE_COOLDOWN_MS) {
    console.log(`Execute: cooldown activo, acción '${action}' ignorada`);
    return res.sendStatus(200);
  }

  if (action === "scale_up" && currentParallelism < MAX_PARALLELISM) {
    await cancelJob(currentJobId);
    currentParallelism++;
    currentJobId = await deployJob(currentParallelism, JAR_ID);
    console.log(`Execute: escaló hacia arriba -> paralelismo=${currentParallelism}`);
    lastScaleTs = now;
  } else if (action === "scale_down" && currentParallelism > MIN_PARALLELISM) {
    await cancelJob(currentJobId);
    currentParallelism--;
    currentJobId = await deployJob(currentParallelism, JAR_ID);
    console.log(`Execute: escaló hacia abajo -> paralelismo=${currentParallelism}`);
    lastScaleTs = now;
  } else {
    console.log(`Execute: acción '${action}' no requiere cambio`);
  }

  res.sendStatus(200);
});

app.listen(3003, () => console.log("Execute corriendo en 3003"));

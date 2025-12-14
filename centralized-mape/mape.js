import { Kafka } from "kafkajs";
import fetch from "node-fetch";
import fs from "fs";

const kafka = new Kafka({
  clientId: "mape-controller",
  brokers: ["kafka:9092"]
});

const admin = kafka.admin();

const TOPIC = "input";
const INTERVAL_MS = 1000; // 1 segundo
const FLINK = "http://flink-jobmanager:8081";

const MAX_PARALLELISM = 8;
const MIN_PARALLELISM = 1;
const SCALE_COOLDOWN_MS = 30000; // 30 segundos

const LATENCY_HIGH = 4.0;
const LATENCY_LOW = 1.0;

let currentJobId = null;
let JAR_ID = null;
let currentParallelism = MIN_PARALLELISM;
let lastScaleTs = 0;

let metricsLog = [];
let latencySamples = [];

// ---------------- FUNCIONES DE FLINK ----------------
async function uploadJar() {
  const FormData = await import("form-data");
  const fs = await import("fs");
  const form = new FormData.default();
  form.append("jarfile", fs.createReadStream("./app.jar"));
  const res = await fetch(`${FLINK}/jars/upload`, { method: "POST", body: form });
  const data = await res.json();
  const jarId = data.files?.[0]?.id || data.filename?.split('/').pop();
  if (!jarId) throw new Error("No se pudo extraer el JAR ID de la respuesta de Flink");
  return jarId;
}

async function deployJob(parallelism, jarId) {
  if (!jarId) return null;
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
  await fetch(`${FLINK}/jobs/${jobId}?mode=cancel`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" }
  });
}

// ---------------- CONSUMO DE LATENCIA ----------------
const consumer = kafka.consumer({ groupId: "mape-latency" });

async function consumeLatency() {
  await consumer.connect();
  await consumer.subscribe({ topic: "output", fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ message }) => {
      const value = message.value.toString();
      const parts = value.split(",");
      if (parts.length < 3) return;

      const tsSent = parseFloat(parts[2]);
      if (!isNaN(tsSent)) {
        latencySamples.push(tsSent);
      }
    }
  });
}

// ---------------- MAPE LOOP ----------------
async function run() {
  await admin.connect();
  consumeLatency();

  if (!JAR_ID) JAR_ID = await uploadJar();
  currentJobId = await deployJob(currentParallelism, JAR_ID);

  let lastProduced = 0;
  let lastTs = Date.now();

  while (true) {
    const topicOffsets = await admin.fetchTopicOffsets(TOPIC);
    const produced = topicOffsets.reduce((sum, p) => sum + Number(p.high), 0);

    const now = Date.now();
    const deltaMsgs = produced - lastProduced;
    const deltaTimeSec = (now - lastTs) / 1000;
    const rate = deltaTimeSec > 0 ? deltaMsgs / deltaTimeSec : 0;

    // Calcular latencia promedio con los timestamps reales
    let avgLatency = 0;
    if (latencySamples.length > 0) {
      const nowSec = Date.now() / 1000;
      avgLatency = latencySamples.map(ts => nowSec - ts).reduce((a, b) => a + b, 0) / latencySamples.length;
    }
    latencySamples = []; // limpiar solo después de calcular promedio

    // Guardar JSON
    metricsLog.push({
      timestamp: new Date().toISOString(),
      produced,
      rate,
      latency: avgLatency,
      parallelism: currentParallelism
    });
    fs.writeFileSync("/app/output/metrics.json", JSON.stringify(metricsLog, null, 2));

    lastProduced = produced;
    lastTs = now;

    // ---------------- ESCALADO ----------------
    const nowTs = Date.now();
    if (nowTs - lastScaleTs >= SCALE_COOLDOWN_MS) {
      if (avgLatency > LATENCY_HIGH && currentParallelism < MAX_PARALLELISM) {
        console.log(`Latencia alta (${avgLatency.toFixed(3)}s), aumentando paralelismo`);
        await cancelJob(currentJobId);
        currentParallelism++;
        currentJobId = await deployJob(currentParallelism, JAR_ID);
        lastScaleTs = nowTs;
      } else if (avgLatency < LATENCY_LOW && currentParallelism > MIN_PARALLELISM) {
        console.log(`Latencia baja (${avgLatency.toFixed(3)}s), reduciendo paralelismo`);
        await cancelJob(currentJobId);
        currentParallelism--;
        currentJobId = await deployJob(currentParallelism, JAR_ID);
        lastScaleTs = nowTs;
      }
    }

    await new Promise(r => setTimeout(r, INTERVAL_MS));
  }
}

run().catch(err => {
  console.error("MAPE error:", err);
  process.exit(1);
});

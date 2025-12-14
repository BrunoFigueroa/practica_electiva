import { Kafka } from "kafkajs";
import fetch from "node-fetch";
import fs from "fs";
import express from "express";
import axios from "axios";

/* ===================== CONFIG RING ===================== */
const MAPE_ID = process.env.MAPE_ID;
const NEXT_MAPE_URL = process.env.NEXT_MAPE_URL;
/* ======================================================= */

const kafka = new Kafka({
  clientId: `mape-controller-${MAPE_ID}`,
  brokers: ["kafka:9092"]
});

const admin = kafka.admin();

const TOPIC = "input";
const FLINK = "http://flink-jobmanager:8081";

/* ========= CONFIGURACIÓN GLOBAL (MUTABLE) ========= */
let INTERVAL_MS = 1000;
let MAX_PARALLELISM = 6;
let MIN_PARALLELISM = 1;
let SCALE_COOLDOWN_MS = 30000;
let LATENCY_HIGH = 4.0;
let LATENCY_LOW = 1.0;
/* ================================================ */

let currentJobId = null;
let JAR_ID = null;
let currentParallelism = MIN_PARALLELISM;
let lastScaleTs = 0;

let metricsLog = [];

/* ======= LATENCY WINDOW ======= */
let latencySamples = [];
let lastLatencyFlush = Date.now();
const LATENCY_WINDOW_MS = 5000;
/* ============================= */

/* ===================== FLINK ===================== */
async function uploadJar() {
  const FormData = await import("form-data");
  const fs = await import("fs");
  const form = new FormData.default();
  form.append("jarfile", fs.createReadStream("./app.jar"));

  const res = await fetch(`${FLINK}/jars/upload`, { method: "POST", body: form });
  const data = await res.json();
  const jarId = data.files?.[0]?.id || data.filename?.split("/").pop();
  if (!jarId) throw new Error("No se pudo obtener JAR ID");
  return jarId;
}

async function deployJob(parallelism, jarId) {
  const res = await fetch(`${FLINK}/jars/${jarId}/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      entryClass: "flink.job.NthPrimeJob",
      parallelism
    })
  });
  const data = await res.json();
  return data.jobid || data.jobID;
}

async function cancelJob(jobId) {
  if (!jobId) return;
  await fetch(`${FLINK}/jobs/${jobId}?mode=cancel`, { method: "PATCH" });
}

/* ===================== KAFKA ===================== */
const consumer = kafka.consumer({
  groupId: `mape-latency-${MAPE_ID}`
});


async function consumeLatency() {
  while (true) {
    try {
      await consumer.connect();
      await consumer.subscribe({ topic: "output", fromBeginning: false });

      await consumer.run({
        eachMessage: async ({ message }) => {
          const parts = message.value.toString().split(",");
          if (parts.length < 3) return;
          const ts = parseFloat(parts[2]);
          if (!isNaN(ts)) latencySamples.push(ts);
        }
      });
      break;
    } catch (err) {
      console.error("Kafka error, retrying in 5s:", err.message);
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

/* ===================== RING SERVER ===================== */
const app = express();
app.use(express.json());

app.post("/ring", async (req, res) => {
  const { origin, config } = req.body;

  if (origin === MAPE_ID) return res.sendStatus(200);

  if (config) {
    INTERVAL_MS = config.INTERVAL_MS ?? INTERVAL_MS;
    MAX_PARALLELISM = config.MAX_PARALLELISM ?? MAX_PARALLELISM;
    MIN_PARALLELISM = config.MIN_PARALLELISM ?? MIN_PARALLELISM;
    SCALE_COOLDOWN_MS = config.SCALE_COOLDOWN_MS ?? SCALE_COOLDOWN_MS;
    LATENCY_HIGH = config.LATENCY_HIGH ?? LATENCY_HIGH;
    LATENCY_LOW = config.LATENCY_LOW ?? LATENCY_LOW;

    console.log(`MAPE ${MAPE_ID}: configuración actualizada desde ring`);
  }

  try {
    await axios.post(NEXT_MAPE_URL, { origin, config });
  } catch (err) {
    console.error("Error reenviando ring:", err.message);
  }

  res.sendStatus(200);
});

app.listen(4000, () =>
  console.log(`MAPE ${MAPE_ID} escuchando ring en 4000`)
);

/* ===================== MAPE LOOP ===================== */
async function run() {
  await admin.connect();
  consumeLatency();

  JAR_ID = await uploadJar();
  currentJobId = await deployJob(currentParallelism, JAR_ID);

  let lastProduced = 0;
  let lastTs = Date.now();

  while (true) {
    let produced = lastProduced;

    try {
      const offsets = await admin.fetchTopicOffsets(TOPIC);
      produced = offsets.reduce((s, p) => s + Number(p.high), 0);
    } catch (err) {
      console.error(
        `MAPE ${MAPE_ID}: fetchTopicOffsets failed, keeping last value`,
        err.type || err.code || err.message
      );
    }


    const now = Date.now();
    const rate =
      (produced - lastProduced) / ((now - lastTs) / 1000 || 1);

    /* ===== CALCULO DE LATENCIA CON VENTANA ===== */
    let avgLatency = null;

    if (latencySamples.length > 0) {
      const nowSec = now / 1000;
      avgLatency =
        latencySamples
          .map(ts => nowSec - ts)
          .reduce((a, b) => a + b, 0) / latencySamples.length;

      // limpiar ventana solo cada LATENCY_WINDOW_MS
      if (now - lastLatencyFlush >= LATENCY_WINDOW_MS) {
        latencySamples = [];
        lastLatencyFlush = now;
      }
    }

    /* ========================================== */

    const metric = {
      mape: MAPE_ID,
      timestamp: new Date().toISOString(),
      produced,
      rate: Number(rate.toFixed(2)),
      latency: avgLatency !== null ? Number(avgLatency.toFixed(3)) : null,
      parallelism: currentParallelism
    };

    metricsLog.push(metric);

    fs.writeFileSync(
    `/app/output/metrics-${MAPE_ID}.json`,
      JSON.stringify(metricsLog, null, 2)
    );

    console.log(JSON.stringify(metric));

    lastProduced = produced;
    lastTs = now;

    const nowTs = Date.now();
    if (
      avgLatency !== null &&
      nowTs - lastScaleTs >= SCALE_COOLDOWN_MS
    ) {
      if (avgLatency > LATENCY_HIGH && currentParallelism < MAX_PARALLELISM) {
        await cancelJob(currentJobId);
        currentParallelism++;
        currentJobId = await deployJob(currentParallelism, JAR_ID);
        lastScaleTs = nowTs;
        console.log(`MAPE ${MAPE_ID}: scale UP -> ${currentParallelism}`);
      } else if (
        avgLatency < LATENCY_LOW &&
        currentParallelism > MIN_PARALLELISM
      ) {
        await cancelJob(currentJobId);
        currentParallelism--;
        currentJobId = await deployJob(currentParallelism, JAR_ID);
        lastScaleTs = nowTs;
        console.log(`MAPE ${MAPE_ID}: scale DOWN -> ${currentParallelism}`);
      }
    }

    await new Promise(r => setTimeout(r, INTERVAL_MS));
  }
}

run().catch(err => {
  console.error("MAPE error:", err);
  process.exit(1);
});

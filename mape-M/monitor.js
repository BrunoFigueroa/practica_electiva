import express from "express";
import axios from "axios";
import { Kafka } from "kafkajs";

const kafka = new Kafka({ clientId: "monitor", brokers: ["kafka:9092"] });
const consumer = kafka.consumer({ groupId: "monitor" });

const ANALYZE_URL = "http://mape-a:3001/analyze"; // servicio analyze

async function consumeAndSend() {
  await consumer.connect();
  await consumer.subscribe({ topic: "output", fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const value = message.value.toString();
      const parts = value.split(",");
      if (parts.length < 3) return;

      const produced = parseInt(parts[0]);
      const rate = parseFloat(parts[1]);
      const tsSent = parseFloat(parts[2]);
      const latency = Date.now() / 1000 - tsSent;

      // Mostrar métricas en terminal
      console.log(
        `produced=${produced} | rate=${rate.toFixed(2)} msg/s | latency=${latency.toFixed(3)} s`
      );

      // Enviar por HTTP a Analyze
      try {
        await axios.post(ANALYZE_URL, { produced, rate, latency });
      } catch (err) {
        console.error("Error enviando a Analyze:", err.message);
      }
    }
  });
}

consumeAndSend().catch(console.error);
console.log("Monitor corriendo, mostrando métricas y enviando a Analyze");

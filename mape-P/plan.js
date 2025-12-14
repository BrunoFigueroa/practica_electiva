import express from "express";
import axios from "axios";

const app = express();
app.use(express.json());

const EXECUTE_URL = "http://mape-e:3003/execute"; // servicio Execute
const LATENCY_HIGH = 4.0;
const LATENCY_LOW = 1.0;

app.post("/plan", async (req, res) => {
  const { score, latency } = req.body;

  let action = "nothing"; // por defecto no hacer nada

  if (latency > LATENCY_HIGH) {
    action = "scale_up";
  } else if (latency < LATENCY_LOW) {
    action = "scale_down";
  }

  try {
    await axios.post(EXECUTE_URL, { action });
    console.log(`Plan envió acción ${action}`);
  } catch (err) {
    console.error("Error enviando a Execute:", err.message);
  }

  res.sendStatus(200);
});

app.listen(3002, () => console.log("Plan corriendo en 3002"));

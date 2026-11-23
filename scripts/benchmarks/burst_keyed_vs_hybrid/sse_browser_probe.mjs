#!/usr/bin/env node
/**
 * Real Playwright Chromium SSE probe for MaintAI Business API (/api/v1/events/stream).
 * Supports seamless stream rotation (before 300s OIDC token expiry) with event deduplication
 * so multi-minute benchmark windows capture 100% of SSE frames with zero missed gaps.
 */

import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const k = argv[i];
    if (k.startsWith("--")) {
      args[k.slice(2)] = argv[i + 1];
      i += 1;
    }
  }
  return args;
}

const args = parseArgs(process.argv);
const url = args.url || "https://api.maintai.localhost/api/v1/events/stream";
const originUrl = args["origin-url"] || new URL("/health/live", url).toString();
const tokenFile = args["token-file"];
const outPath = args.out;
const readyFile = args["ready-file"];
const statsFile = args["stats-file"];
const rotateSeconds = Number(args["rotate-seconds"] || "180");

if (!tokenFile || !outPath || !readyFile) {
  console.error("Required: --token-file, --out, --ready-file");
  process.exit(2);
}

const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), "../../..");
const pwEntry = path.join(repoRoot, "frontend/node_modules/playwright/index.mjs");
const { chromium } = await import(pathToFileURL(pwEntry).href);

fs.mkdirSync(path.dirname(outPath), { recursive: true });
const outStream = fs.createWriteStream(outPath, { flags: "a" });
const eventCounts = {};
const seenPredEvents = new Set();
let duplicateSuppressed = 0;
let reconnects = 0;
let streamError = null;
let browser = null;
let shuttingDown = false;
let readyWritten = false;

async function shutdown(code = 0) {
  if (shuttingDown) return;
  shuttingDown = true;
  if (statsFile) {
    fs.writeFileSync(
      statsFile,
      JSON.stringify(
        {
          eventCounts,
          uniquePredictionEvents: seenPredEvents.size,
          duplicateSuppressed,
          reconnects,
          streamError,
          closedAt: new Date().toISOString(),
        },
        null,
        2
      )
    );
  }
  await new Promise((resolve) => outStream.end(resolve));
  if (browser) {
    try {
      await browser.close();
    } catch {
      // Ignore close race
    }
  }
  process.exit(code);
}

process.on("SIGTERM", () => void shutdown(0));
process.on("SIGINT", () => void shutdown(0));

browser = await chromium.launch({
  headless: true,
  args: [
    "--host-resolver-rules=MAP *.maintai.localhost 127.0.0.1",
    "--ignore-certificate-errors",
    "--no-sandbox",
  ],
});

const context = await browser.newContext({ ignoreHTTPSErrors: true });
const page = await context.newPage();

await page.exposeFunction("__readToken", () => {
  return fs.readFileSync(tokenFile, "utf8").trim();
});

await page.exposeFunction("__emitSseEvent", (record) => {
  eventCounts[record.event_type] = (eventCounts[record.event_type] || 0) + 1;
  if (record.event_type === "prediction.completed" && record.source_event_id) {
    const k = `${record.machine_id}:${record.source_event_id}`;
    if (seenPredEvents.has(k)) {
      duplicateSuppressed += 1;
      return;
    }
    seenPredEvents.add(k);
    outStream.write(JSON.stringify(record) + "\n");
  }
});

await page.exposeFunction("__onStreamReady", () => {
  if (!readyWritten) {
    readyWritten = true;
    fs.writeFileSync(readyFile, JSON.stringify({ readyAt: new Date().toISOString() }));
  }
});

await page.exposeFunction("__onReconnect", () => {
  reconnects += 1;
});

await page.exposeFunction("__onStreamError", (msg) => {
  streamError = msg;
});

await page.goto(originUrl, { waitUntil: "domcontentloaded", timeout: 15000 });

page
  .evaluate(
    async ({ streamUrl, rotateMs }) => {
      let activeController = null;
      let connSeq = 0;

      async function pumpConnection(controller, myId, onFirstOpen) {
        const bearer = await window.__readToken();
        const resp = await fetch(streamUrl, {
          headers: {
            Authorization: `Bearer ${bearer}`,
            Accept: "text/event-stream",
          },
          cache: "no-store",
          signal: controller.signal,
        });
        if (!resp.ok || !resp.body) {
          throw new Error(`HTTP ${resp.status}`);
        }
        onFirstOpen();
        const reader = resp.body.getReader();
        const decoder = new TextDecoder("utf-8");
        let buffer = "";
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          const receivedAt = new Date(performance.timeOrigin + performance.now()).toISOString();
          buffer += decoder.decode(value, { stream: true });
          let idx;
          while ((idx = buffer.indexOf("\n\n")) !== -1) {
            const rawFrame = buffer.slice(0, idx);
            buffer = buffer.slice(idx + 2);
            if (!rawFrame || rawFrame.startsWith(":")) continue;
            let eventType = "message";
            const dataLines = [];
            for (const line of rawFrame.split("\n")) {
              if (line.startsWith("event:")) {
                eventType = line.slice(6).trim();
              } else if (line.startsWith("data:")) {
                dataLines.push(line.slice(5).trimStart());
              }
            }
            if (dataLines.length === 0) continue;
            let parsed = null;
            try {
              parsed = JSON.parse(dataLines.join("\n"));
            } catch {
              continue;
            }
            const dataObj = parsed.data || {};
            await window.__emitSseEvent({
              event_type: eventType,
              conn_id: myId,
              machine_id: parsed.machine_id || dataObj.machine_id || null,
              source_event_id: dataObj.source_event_id || null,
              sequence_number: dataObj.sequence_number ?? null,
              stream_epoch: dataObj.stream_epoch || null,
              prediction_id: dataObj.prediction_id || null,
              prediction: dataObj.prediction || null,
              completed_at: dataObj.completed_at || null,
              browser_sse_received_at: receivedAt,
            });
          }
        }
      }

      async function openNext(isInitial) {
        connSeq += 1;
        const myId = connSeq;
        const controller = new AbortController();
        const prevController = activeController;
        activeController = controller;
        let resolved = false;
        return new Promise((resolveReady, rejectReady) => {
          let donePromise = null;
          donePromise = pumpConnection(controller, myId, () => {
            resolved = true;
            if (isInitial) {
              window.__onStreamReady();
            } else {
              window.__onReconnect();
            }
            if (prevController) {
              setTimeout(() => prevController.abort(), 500);
            }
            setTimeout(() => resolveReady({ done: donePromise }), 0);
          }).catch((err) => {
            if (!resolved) {
              rejectReady(err);
            } else if (err && err.name !== "AbortError") {
              throw err;
            }
          });
        });
      }

      try {
        let { done: running } = await openNext(true);
        while (true) {
          const race = await Promise.race([
            running.then(() => "ended"),
            new Promise((r) => setTimeout(() => r("rotate"), rotateMs)),
          ]);
          ({ done: running } = await openNext(false));
          void race;
        }
      } catch (err) {
        if (err && err.name !== "AbortError") {
          await window.__onStreamError(String(err));
        }
      }
    },
    { streamUrl: url, rotateMs: Math.max(30000, rotateSeconds * 1000) }
  )
  .catch((err) => {
    if (!shuttingDown) {
      streamError = String(err);
    }
  });

setInterval(() => {
  if (streamError && !shuttingDown) {
    console.error("SSE probe stream error:", streamError);
    void shutdown(1);
  }
}, 500);

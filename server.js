import express from "express";
import fs from "fs";
import path from "path";
import os from "os";
import { Storage } from "@google-cloud/storage";
import fetch from "node-fetch";
import { GoogleAuth } from "google-auth-library";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(express.json());

// ===== GCP Config =====
const PROJECT_ID = "gcp-steve-123";
const LOCATION = "global";
const BUCKET = "vertex-ai-search-bucket-123";

const auth = new GoogleAuth({
  keyFile: path.join(process.cwd(), process.env.GCP_KEY_FILE),
  scopes: ["https://www.googleapis.com/auth/cloud-platform"],
});

const storage = new Storage({
  keyFilename: path.join(process.cwd(), process.env.GCP_KEY_FILE),
});

// ===== Helper =====
async function getToken() {
  const client = await auth.getClient();
  const tokenObj = await client.getAccessToken();
  return tokenObj.token;
}

async function createDataStore(token, datastoreId) {
  console.log(`[DEBUG] Creating datastore ${datastoreId}...`);
  const url = `https://discoveryengine.googleapis.com/v1/projects/${PROJECT_ID}/locations/${LOCATION}/dataStores?dataStoreId=${datastoreId}`;
  const res = await fetch(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      displayName: `Datastore for ${datastoreId}`,
      industryVertical: "GENERIC",
      contentConfig: "CONTENT_REQUIRED",
    }),
  });
  if (!res.ok) throw new Error(await res.text());
  console.log(`[DEBUG] Datastore ${datastoreId} created`);
}

async function createSearchApp(token, searchAppId, datastoreId) {
  console.log(`[DEBUG] Creating search app ${searchAppId} linked to ${datastoreId}...`);
  const url = `https://discoveryengine.googleapis.com/v1/projects/${PROJECT_ID}/locations/${LOCATION}/collections/default_collection/engines?engineId=${searchAppId}`;
  const res = await fetch(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      displayName: `Search App for ${searchAppId}`,
      dataStoreIds: [datastoreId],
      solutionType: "SOLUTION_TYPE_SEARCH",
    }),
  });
  if (!res.ok) throw new Error(await res.text());
  console.log(`[DEBUG] Search app ${searchAppId} created`);
}

async function uploadFileToGCS(localPath, destFileName, prefix) {
  const gcsDest = `${prefix}/${destFileName}`;
  console.log(`[DEBUG] Uploading ${gcsDest} to GCS...`);
  await storage.bucket(BUCKET).upload(localPath, { destination: gcsDest });
  return `gs://${BUCKET}/${gcsDest}`;
}

async function importData(token, datastoreId, gcsPath) {
  console.log(`[DEBUG] Importing ${gcsPath} to datastore ${datastoreId}...`);
  const url = `https://discoveryengine.googleapis.com/v1/projects/${PROJECT_ID}/locations/${LOCATION}/dataStores/${datastoreId}/branches/default_branch/documents:import`;
  const res = await fetch(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ gcsSource: { inputUris: [gcsPath] }, reconciliationMode: "INCREMENTAL" }),
  });
  if (!res.ok) throw new Error(await res.text());
  console.log(`[DEBUG] Data imported to datastore ${datastoreId}`);
}

// ===== Main: process file gốc =====
const userDatastores = {}; // { userPhone: { datastoreId, searchAppId } }
const DATA_FOLDER = path.join(process.cwd(), "data");
if (!fs.existsSync(DATA_FOLDER)) fs.mkdirSync(DATA_FOLDER, { recursive: true });

async function processInputFile(inputFile) {
  const rawData = JSON.parse(fs.readFileSync(inputFile, "utf-8"));

  // Nhóm theo userPhone
  const userGroups = {};
  for (const row of rawData) {
    if (!userGroups[row.userPhone]) userGroups[row.userPhone] = [];
    userGroups[row.userPhone].push(row);
  }

  const token = await getToken();

  for (const userPhone of Object.keys(userGroups)) {
    const datastoreId = `datastore-${userPhone}-${Date.now()}`;
    const searchAppId = `searchapp-${userPhone}-${Date.now()}`;

    await createDataStore(token, datastoreId);
    await createSearchApp(token, searchAppId, datastoreId);

    userDatastores[userPhone] = { datastoreId, searchAppId };

    // Tạo JSONL cho user
    const userFilePath = path.join(os.tmpdir(), `${userPhone}-messages.jsonl`);
    const jsonlContent = userGroups[userPhone]
      .map(m => JSON.stringify({ id: m.timestamp.replace(/[^a-zA-Z0-9_-]/g,"_"), content: m.message, structData: m }))
      .join("\n") + "\n";
    fs.writeFileSync(userFilePath, jsonlContent, "utf8");

    // Upload bucket
    const gcsPath = await uploadFileToGCS(userFilePath, "messages.jsonl", userPhone);

    // Import vào Datastore
    await importData(token, datastoreId, gcsPath);

    fs.unlinkSync(userFilePath);
  }
}

// ===== API test =====
app.post("/api/process", async (req, res) => {
  try {
    await processInputFile(path.join(DATA_FOLDER, "input.json"));
    res.json({ success: true, message: "Processed all users!" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ===== Start server =====
app.listen(3002, async () => {
  console.log("[DEBUG] Server running at http://localhost:3002");
});

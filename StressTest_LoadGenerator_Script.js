/**
 * StressTest_LoadGenerator_Script.js
 * ================================================================
 * Automated load-generation script used for the server-saturation
 * ("stress test") evaluation reported in Section 2.4.4 and Table 4
 * of the main article (and Supplementary Section S10).
 *
 * Publishes batches of concurrent emergency-call payloads to the
 * ambulance-dispatch server via MQTT, in stages of 10, 25, 50, 75,
 * and 100 concurrent calls, and times every subsequent dispatch-
 * assignment message against a single reference timestamp recorded
 * at the start of each stage's batch (see Section 2.4.4 for why
 * this is a batch-relative, not a per-call, latency measurement).
 *
 * For each stage, this script computes the five quantities reported
 * in Table 4 (calls sent, publish time, and minimum/maximum/average
 * assignment-arrival time) directly in memory from live MQTT message
 * timestamps, plus a database query reporting the resulting call-
 * status breakdown. It prints this five-row, per-stage summary to
 * the console and exports it to a small CSV file; it was not
 * designed to persist a raw per-request or per-packet log of every
 * individual publish and assignment-confirmation event. Table 4 is
 * therefore the complete output of this test.
 *
 * Log-message and comment text in this script has been translated
 * to English for this submission. Database table/column names,
 * MQTT topic strings, and MQTT payload field names are left
 * unchanged, as they are literal identifiers used by the deployed
 * system (see inline comments for their English meaning); e.g.
 * panggilan/masuk = call/incoming, ambulans/tugas/+ = ambulance/task/+.
 *
 * Usage: node StressTest_LoadGenerator_Script.js
 * ================================================================
 */

const mqtt = require('mqtt');
const { Pool } = require('pg');
const fs = require('fs');
require('dotenv').config();

// ===================== CONFIGURATION =====================
const STAGES = [10, 25, 50, 75, 100];        // Number of calls per stage, in sequence
const ID_PASIEN = 45;                        // Valid patient ID in the database (change if needed)
const WAIT_PROCESSING_SEC = 35;              // Time to wait for the server to process a stage (seconds)
const WAIT_BETWEEN_STAGES_SEC = 180;         // 3-MINUTE pause between stages (seconds)
const BASE_LAT = -7.945;                     // Center coordinate for the test area
const BASE_LON = 112.615;                    // Center coordinate for the test area
const COORD_SPREAD = 0.08;                   // Coordinate spread (~4 km radius)
// ===========================================================

function randomCoord(base, spread) {
    return parseFloat((base + (Math.random() - 0.5) * spread).toFixed(7));
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function timestamp() {
    return new Date().toISOString().replace('T', ' ').substring(0, 19);
}

// Countdown timer displayed in the terminal
async function countdown(seconds, label) {
    for (let i = seconds; i > 0; i--) {
        process.stdout.write(`\r  ${label}: ${i} seconds remaining...   `);
        await sleep(1000);
    }
    process.stdout.write(`\r  ${label}: Done.                    \n`);
}

async function main() {
    console.log(`\n================================================================`);
    console.log(` STRESS TEST - SERVER SATURATION POINT TEST`);
    console.log(` Start time : ${timestamp()}`);
    console.log(` Stages     : ${STAGES.join(', ')} calls`);
    console.log(` Patient ID : ${ID_PASIEN}`);
    console.log(`================================================================\n`);

    // --- 1. MQTT connection ---
    console.log(`[SETUP] Connecting to MQTT broker...`);
    const mqttClient = mqtt.connect(process.env.MQTT_BROKER_URL);

    await new Promise((resolve, reject) => {
        mqttClient.on('connect', () => {
            console.log(`[SETUP] MQTT connected.`);
            resolve();
        });
        mqttClient.on('error', (err) => {
            console.error(`[SETUP] MQTT connection failed:`, err.message);
            reject(err);
        });
    });

    // --- 2. Database connection ---
    console.log(`[SETUP] Connecting to the Neon database...`);
    const dbPool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false },
        max: 5
    });
    await dbPool.query('SELECT 1'); // Test connection
    console.log(`[SETUP] Database connected.`);

    // --- 3. Subscribe to detect driver assignment ---
    let assignmentCount = 0;
    const assignmentTimes = [];
    let stageStartTime = 0;

    mqttClient.subscribe('ambulans/tugas/+', { qos: 1 });  // ambulans/tugas/+ = ambulance/task/+ (dispatch-assignment topic)
    mqttClient.on('message', (topic) => {
        if (topic.startsWith('ambulans/tugas/')) {
            assignmentCount++;
            assignmentTimes.push(Date.now() - stageStartTime);
        }
    });

    // --- 4. Run the test, stage by stage ---
    const allResults = [];

    for (let stage = 0; stage < STAGES.length; stage++) {
        const count = STAGES[stage];
        assignmentCount = 0;
        assignmentTimes.length = 0;

        console.log(`\n================================================================`);
        console.log(` STAGE ${stage + 1}/${STAGES.length}: ${count} CONCURRENT CALLS`);
        console.log(`================================================================`);

        // Reset server state
        console.log(`  [RESET] Returning all drivers to AVAILABLE...`);
        await dbPool.query(`
            UPDATE ambulans SET status_operasional = 'AVAILABLE'
            WHERE status_operasional IN ('BUSY', 'WAITING_FOR_DRIVER')
        `);  // ambulans = ambulance table; status_operasional = operational status column

        console.log(`  [RESET] Cancelling active calls...`);
        await dbPool.query(`
            UPDATE transaksi_panggilan SET status_panggilan = 'CANCELLED'
            WHERE status_panggilan IN ('PENDING', 'WAITING_FOR_DRIVER')
        `);  // transaksi_panggilan = call-transaction table; status_panggilan = call status column

        await sleep(3000); // Allow the server time to stabilize

        // Record the call count BEFORE the test (more accurate than relying on timestamps)
        const countBefore = (await dbPool.query(`SELECT COUNT(*)::int as total FROM transaksi_panggilan`)).rows[0].total;
        stageStartTime = Date.now();

        // Publish N messages concurrently
        console.log(`  [PUBLISH] Sending ${count} concurrent calls...`);
        const publishPromises = [];
        for (let i = 0; i < count; i++) {
            const payload = {
                id_pasien: ID_PASIEN,                                     // patient ID
                lokasi_pasien_lat: randomCoord(BASE_LAT, COORD_SPREAD),   // patient location latitude
                lokasi_pasien_lon: randomCoord(BASE_LON, COORD_SPREAD),   // patient location longitude
                jenis_layanan: "DARURAT",                                 // service type ("DARURAT" = "EMERGENCY"; literal value expected by the backend)
                kondisi: `Stress Test T${stage + 1} #${i + 1}`            // condition/notes field
            };
            publishPromises.push(new Promise((resolve) => {
                mqttClient.publish('panggilan/masuk', JSON.stringify(payload), { qos: 1 }, resolve);  // panggilan/masuk = call/incoming topic
            }));
        }
        await Promise.all(publishPromises);

        const publishDuration = Date.now() - stageStartTime;
        console.log(`  [PUBLISH] Completed in ${publishDuration} ms`);

        // Wait for the server to process the batch
        await countdown(WAIT_PROCESSING_SEC, 'Waiting for server');

        const totalDuration = Date.now() - stageStartTime;

        // Query the call count AFTER the test
        const countAfter = (await dbPool.query(`SELECT COUNT(*)::int as total FROM transaksi_panggilan`)).rows[0].total;
        const totalMasukDB = countAfter - countBefore;  // totalMasukDB = number of new calls recorded in the database

        // Query the status breakdown for the most recent calls
        const dbResult = await dbPool.query(`
            SELECT status_panggilan, COUNT(*)::int as jumlah
            FROM (
                SELECT status_panggilan FROM transaksi_panggilan
                ORDER BY id_panggilan DESC
                LIMIT $1
            ) sub
            GROUP BY status_panggilan
            ORDER BY jumlah DESC
        `, [totalMasukDB > 0 ? totalMasukDB : count]);

        const statusCounts = {};
        dbResult.rows.forEach(row => {
            statusCounts[row.status_panggilan] = row.jumlah;
        });

        const assigned = (statusCounts['WAITING_FOR_DRIVER'] || 0) + (statusCounts['ON_THE_WAY'] || 0);
        const noDrivers = (statusCounts['NO_DRIVERS'] || 0) + (statusCounts['NO_DRIVERS_AVAILABLE'] || 0);
        const apiFailed = statusCounts['API_FAILED'] || 0;
        const failed = statusCounts['FAILED'] || 0;
        const pending = statusCounts['PENDING'] || 0;
        const cancelled = statusCounts['CANCELLED'] || 0;

        const totalTerproses = totalMasukDB - pending;  // totalTerproses = number processed (not left pending)
        const tingkatProses = totalMasukDB > 0 ? ((totalTerproses / count) * 100).toFixed(1) : '0.0';  // tingkatProses = processing rate (%)
        const avgResponseMs = assignmentTimes.length > 0
            ? Math.round(assignmentTimes.reduce((a, b) => a + b, 0) / assignmentTimes.length)
            : '-';
        const minResponseMs = assignmentTimes.length > 0 ? Math.min(...assignmentTimes) : '-';
        const maxResponseMs = assignmentTimes.length > 0 ? Math.max(...assignmentTimes) : '-';

        const result = {
            tahap: stage + 1,                  // stage
            jumlahDikirim: count,               // number sent
            waktuPublishMs: publishDuration,    // publish time (ms)
            waktuTotalMs: totalDuration,        // total elapsed time (ms)
            masukDB: totalMasukDB,              // recorded in DB
            terproses: totalTerproses,          // processed
            driverDitugaskan: assignmentCount,  // drivers assigned
            noDrivers: noDrivers,
            apiFailed: apiFailed,
            gagal: failed,                      // failed
            pending: pending,
            tingkatProses: tingkatProses,       // processing rate
            avgResponseMs: avgResponseMs,
            minResponseMs: minResponseMs,
            maxResponseMs: maxResponseMs,
            statusDetail: statusCounts
        };
        allResults.push(result);

        // Print this stage's results
        console.log(`\n  ---- STAGE ${stage + 1} RESULTS ----`);
        console.log(`  Calls sent             : ${count}`);
        console.log(`  Recorded in database   : ${totalMasukDB}`);
        console.log(`  Processed              : ${totalTerproses}`);
        console.log(`  Drivers assigned       : ${assignmentCount}`);
        console.log(`  No drivers available   : ${noDrivers}`);
        console.log(`  API failed             : ${apiFailed}`);
        console.log(`  Failed / error         : ${failed}`);
        console.log(`  Still pending          : ${pending}`);
        console.log(`  Processing rate        : ${tingkatProses}%`);
        console.log(`  Publish time           : ${publishDuration} ms`);
        if (assignmentTimes.length > 0) {
            console.log(`  Fastest response       : ${minResponseMs} ms`);
            console.log(`  Slowest response       : ${maxResponseMs} ms`);
            console.log(`  Average response       : ${avgResponseMs} ms`);
        }
        console.log(`  Status detail          : ${JSON.stringify(statusCounts)}`);

        // Cleanup: reset for the next stage
        await dbPool.query(`
            UPDATE ambulans SET status_operasional = 'AVAILABLE'
            WHERE status_operasional IN ('BUSY', 'WAITING_FOR_DRIVER')
        `);

        if (stage < STAGES.length - 1) {
            console.log(``);
            await countdown(WAIT_BETWEEN_STAGES_SEC, 'Pause between stages (screenshot time)');
        }
    }

    // --- 5. Final report ---
    console.log(`\n\n================================================================`);
    console.log(` FINAL REPORT - SERVER SATURATION POINT TEST`);
    console.log(` End time: ${timestamp()}`);
    console.log(`================================================================\n`);

    // Table header
    console.log(`${''.padEnd(75, '-')}`);
    console.log(` ${'Stage'.padEnd(6)}| ${'Sent'.padStart(6)} | ${'Pub(ms)'.padStart(8)} | ${'Min(ms)'.padStart(8)} | ${'Max(ms)'.padStart(8)} | ${'Avg(ms)'.padStart(8)}`);
    console.log(`${''.padEnd(75, '-')}`);

    allResults.forEach(r => {
        const row = [
            `T${r.tahap}`.padEnd(6),
            String(r.jumlahDikirim).padStart(6),
            String(r.waktuPublishMs).padStart(8),
            String(r.minResponseMs).padStart(8),
            String(r.maxResponseMs).padStart(8),
            String(r.avgResponseMs).padStart(8),
        ];
        console.log(` ${row.join(' | ')}`);
    });
    console.log(`${''.padEnd(75, '-')}`);

    // Key findings on response time
    console.log(`\n  Notes on driver-assignment response time:`);
    allResults.forEach(r => {
        if (r.avgResponseMs !== '-') {
            console.log(`  - T${r.tahap} (${r.jumlahDikirim} calls): Avg ${r.avgResponseMs}ms, Min ${r.minResponseMs}ms, Max ${r.maxResponseMs}ms`);
        }
    });

    // --- 6. CSV export ---
    const csvHeader = 'Stage,Calls Sent,Publish Time (ms),Response Min (ms),Response Max (ms),Response Avg (ms)\n';
    const csvRows = allResults.map(r =>
        `${r.tahap},${r.jumlahDikirim},${r.waktuPublishMs},${r.minResponseMs},${r.maxResponseMs},${r.avgResponseMs}`
    ).join('\n');

    const csvFilename = `stress-test-result_${new Date().toISOString().slice(0, 10)}.csv`;
    fs.writeFileSync(csvFilename, '﻿' + csvHeader + csvRows, 'utf8');
    console.log(`\n  Results saved to: ${csvFilename}`);

    // --- 7. Automated analysis ---
    console.log(`\n================================================================`);
    console.log(` ANALYSIS`);
    console.log(`================================================================`);

    const lastFullProcess = allResults.filter(r => parseFloat(r.tingkatProses) >= 95);
    const firstDrop = allResults.find(r => parseFloat(r.tingkatProses) < 95);

    if (lastFullProcess.length > 0) {
        const best = lastFullProcess[lastFullProcess.length - 1];
        console.log(`  The server was able to process ${best.jumlahDikirim} concurrent calls`);
        console.log(`  with a success rate of ${best.tingkatProses}%.`);
    }
    if (firstDrop) {
        console.log(`  A performance drop was first detected at ${firstDrop.jumlahDikirim} calls`);
        console.log(`  (processing rate fell to ${firstDrop.tingkatProses}%).`);
    }
    if (!firstDrop) {
        console.log(`  The server processed every stage without a significant drop.`);
    }

    console.log(`\n================================================================\n`);

    // Cleanup
    mqttClient.end();
    await dbPool.end();
    process.exit(0);
}

main().catch(err => {
    console.error('\n[FATAL]', err.message);
    process.exit(1);
});

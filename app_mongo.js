//
// Add indexes in mongo
// Before start create user and indexes
/*


 db.slates.createIndex({ queue: 1, made: 1, createdat: 1 });
 db.slates.createIndex({ messageid: 1, made: 1 });
 db.slates.createIndex({ epicboxtxid: 1, made: 1 });
 db.slates.createIndex({ route: 1, epicboxtxid: 1 });
 db.slates.createIndex({ "createdat": 1 }, { expireAfterSeconds: 604800 });
 db.cancelled_slates.createIndex(
  { epicboxtxid: 1 },
  {
   unique: true,
   partialFilterExpression: { epicboxtxid: { $type: "string" } }
  }
 );
 db.cancelled_slates.createIndex({ participants: 1, cancelledat: 1 });
 db.cancelled_slates.createIndex({ receiver: 1, receiver_acknowledged: 1, cancelledat: 1 });
 db.cancelled_slates.createIndex({ "cancelledat": 1 }, { expireAfterSeconds: 604800 });
 db.createUser(
  {
    user: "epicbox",
    pwd: passwordPrompt(), // or cleartext password
    roles: [
      { role: "readWrite", db: "epicbox" }
    ]
  }
 );

*/

'use strict';

const fs = require("fs");
const { createServer } = require("http");
const https = require('https'); // moved require out of request handler
const { execFile } = require('node:child_process');
const uid = require('uid2');
const { WebSocket, WebSocketServer } = require('ws');
const { MongoClient } = require('mongodb');

const customConfig = process.argv.indexOf('--config');
// this epicbox protocol version
const protver = "3.1.0";
/**
 * @deprecated in wallet version 3.5.2
 * use dynamic challenge strings
 */
const static_challenge = "7WUDtkSaKyGRUnQ22rE3QUXChV8DmA6NnunDYP4vheTpc";
// used to reference client socket (ws) to public address (epic address) for slate passthroughs
const clients_publicaddress = {};
// Prevent a server-to-server CancelTx from looping while the original
// cancellation is still being processed.
const activeCancellations = new Set();

// Transaction-level cancellation barrier. Unlike activeCancellations, which is
// keyed by epicboxtxid:cancelrequestid and only prevents relay loops, this map
// suppresses Slate delivery for A while an authenticated participant cancel is
// being propagated. A refcount handles concurrent cancellation requests from
// both participants.
const cancellingTxids = new Map();

const beginCancellation = (epicboxtxid) => {
    cancellingTxids.set(
        epicboxtxid,
        (cancellingTxids.get(epicboxtxid) || 0) + 1
    );
};

const endCancellation = (epicboxtxid) => {
    const count = cancellingTxids.get(epicboxtxid) || 0;

    if (count <= 1) {
        cancellingTxids.delete(epicboxtxid);
    } else {
        cancellingTxids.set(epicboxtxid, count - 1);
    }
};

const cancellationInProgress = (epicboxtxid) =>
    cancellingTxids.has(epicboxtxid);
const config = {
    mongourl: "mongodb://127.0.0.1:27019",
    epicbox_domain: process.env.EPICBOX_DOMAIN || "epicbox.your-domain.com",
    epicbox_port: process.env.EPICBOX_PORT || 443,
    localepicboxserviceport: "3423",
    pathtoepicboxlib: "./epicboxlib",
    db_name: "epicbox",
    collection_name: "slates",
    cancelled_collection_name: "cancelled_slates",
    challenge_interval: 60000, // milliseconds
    debugMessage: true,
    stats: false,
    instance_id: process.env.EPICBOX_INSTANCE_ID || 0,
    // cap incoming websocket message size (bytes). Slates are small
    // and we want to defend against memory-exhaustion abuse
    ws_max_payload: 5 * 1024 * 1024
};
let mongoclient = null;
let collection = null;
// tombstones for canceled slates. allows for recovery from lost
// TransactionCancelled response. can be re-requested orre-confirmed
let cancelledCollection = null;

let statistics = {

  from: new Date(),
  connectionsInHour: 0,
  slatesReceivedInHour: 0,
  slatesRelayedInHour: 0,
  slatesSentInHour: 0,
  subscribeInHour: 0,
  activeconnections: 0,
  slatesAttempt: 0
}

//clean stats every hour
setInterval(() => {
  statistics = {
    from: new Date(),
    connectionsInHour: 0,
    slatesReceivedInHour: 0,
    slatesRelayedInHour: 0,
    slatesSentInHour: 0,
    subscribeInHour: 0,
    activeconnections: 0,
    slatesAttempt: 0
  }
}, 60 * 60 * 1000);

// cache the endpoint health checks so every page view does not
// trigger three outbound https requests
const ENDPOINT_STATUS_TTL = 60 * 1000;
let endpointStatusCache = { at: 0, results: [] };

const checkEndpoints = () => {
    return new Promise((resolve) => {
        const now = Date.now();
        if (now - endpointStatusCache.at < ENDPOINT_STATUS_TTL && endpointStatusCache.results.length > 0) {
            return resolve(endpointStatusCache.results);
        }

        // List of Epicbox endpoints to check
        const endpoints = [
            { label: "North America, South America", url: "https://epicbox.epiccash.com" },
            { label: "US East Coast", url: "https://epicbox.epicnet.us" },
            { label: "Epic Mobile Server", url: "https://epicbox.stackwallet.com" }
        ];
        const results = [];
        let checked = 0;
        const done = () => {
            if (++checked === endpoints.length) {
                endpointStatusCache = { at: Date.now(), results };
                resolve(results);
            }
        };
        endpoints.forEach((ep, idx) => {
            const req = https.get(ep.url, { timeout: 5000 }, (resp) => {
                const color = resp.statusCode === 200 ? 'green' : 'red';
                results[idx] = `<span style='font-size:2em;color:${color};'>&#9679;</span> ${ep.label} - <a href='${ep.url}' style='color:orange;'>${ep.url}</a>`;
                resp.resume();
                done();
            });
            req.on('timeout', () => req.destroy(new Error('timeout')));
            req.on('error', () => {
                results[idx] = `<span style='font-size:2em;color:red;'>&#9679;</span> ${ep.label} - <a href='${ep.url}' style='color:orange;'>${ep.url}</a>`;
                done();
            });
        });
    });
};

const requestListener = (req, res) => {
    res.writeHead(200);
    checkEndpoints().then((results) => {
        res.end(`<!DOCTYPE html>
            <html>
            <head>
            <title>Epicbox</title>
            <style>a:link { color: orange; } a:visited { color: orange; }</style>
            </head>
            <body style='background-color: #242222; color: lightgray; margin-left: 20px;'>
            <h1>Epicbox server (Instance ${config.instance_id})</h1>
            <p>Protocol version ${protver}</p>
            ${results.join('<br>')}
            <br>
            <p>More about Epic</p>
            <a href="https://epiccash.com" target="_blank">Epic Cash website</a>
            <br><br>
            Required epic-wallet.toml settings.
            <pre><code>
            [epicbox]
            epicbox_domain = '${config.epicbox_domain}'
            epicbox_port = ${config.epicbox_port}
            </code></pre>
            <p> start listen: epic-wallet listen -m epicbox</p>
            <br>
            <h1>Epicbox Statistics from ${statistics.from.toUTCString()}:</h1>
            <h3>
            connections: ${statistics.connectionsInHour}<br>
            active connections: ${wss.clients.size}<br>
            subscribes: ${statistics.subscribeInHour}<br>
            received slates: ${statistics.slatesReceivedInHour}<br>
            relayed slates: ${statistics.slatesRelayedInHour}<br>
            sending slate attempts: ${statistics.slatesAttempt}<br>
            </h3>
            </body>
            </html>`);
    });
}

/*
    webserver for port 80
*/
const server = createServer(requestListener);

/*
    epicbox websocket
*/
const wss = new WebSocketServer({
  server: server,
  // explicit payload cap (see config)
  maxPayload: config.ws_max_payload
});

wss.on('connection', (ws, req) => {

    if (config.stats) {
        statistics.connectionsInHour++;
    }

    ws.uid = uid(5);
    ws.epicboxver = null;
    ws.ip = null;
    ws.challenge = null;
    ws.epicPublicAddress = null;
    // don't send challenges or slates to busy client
    ws.process_slate = false;
    // count send attempts to client
    ws.sendslate_attempts = 0;
    ws.max_sendslate_attempts = 0;
    ws.pending_challenge = false;
    // If a TransactionCancelled notification is sent on this socket, the
    // receiver's next authenticated Subscribe acknowledges it.
    ws.pending_cancel_ack = null;
    ws.client_details = {
        wallet_version: '',
        wallet_mode: '',
        protocol_version: ''
    };


    if (req.headers['x-forwarded-for']) {
        ws.ip = req.headers['x-forwarded-for'].split(',')[0].trim();
    } else {
        ws.ip = req.socket.remoteAddress;
    }

    console.log(`[${new Date().toLocaleTimeString()}] [${ws.uid}] New connection from `, ws.ip);

    // send a Challenge to wallet or other epicbox when first time connect
    // challenges are send in interval every x seconds later
    challenge(ws);

    ws.on('close', (code, reason) => {

        removeListenerMapping(ws);
        console.log('[%s] - [%s][%s] -> [%s] code: %s, reason: %s', new Date().toLocaleTimeString(), ws.uid, ws.ip, "Close connection", code, reason.toString());
    });

    ws.on('error', (err) => {
        removeListenerMapping(ws);
        console.log('[%s] - [%s][%s] -> [%s] error: %s', new Date().toLocaleTimeString(), ws.uid, ws.ip, "Error", err);
    });

    ws.on('message', (data) => {
        let message = null;

        try {
            message = JSON.parse(data);
        } catch (err) {
            console.log("Error parsing json data from client.", err);
            removeListenerMapping(ws);
            // pass args directly so as not to leak implicit globals
            return ws.close(3000, 'Error parsing message.');
        }

        // guard against messages without a string type, to avoid throw
        if (message === null || typeof message !== 'object' || typeof message.type !== 'string') {
            removeListenerMapping(ws);
            return ws.close(3000, 'Invalid message.');
        }

        let type = message.type;

        /* TODO:
            - clients should set version via setVersion type
            - split wallet client from epicbox client
        */
        switch (type.toLowerCase()) {
            case "ping":
                ws.send("pong");
            break;
            case "pong":
                ws.send("ping");
            break;
            /**
             * @deprecated epicbox protocol version 3.0.0
             * clients should not be allowed to trigger challenge/subscribe requests
             */
            case "challenge":
                challenge(ws);
            break;
            case "subscribe":
                subscribe(ws, message);
            break;
            case "unsubscribe":
                unsubscribe(ws);
            break;
            case "postslate":
                validatePostslate(ws, message);
            break;
            // made is send after slate was successfully processed in wallet
            case "made":
                made(ws, message);
            break;
            // participant cancels every queued state by its stable epicboxtxid
            case "canceltx":
                canceltx(ws, message);
            break;
            /**
             * @deprecated epicbox protocol version 3.0.0
             */
            case "getversion":
                ws.send(JSON.stringify({type: "GetVersion", str: protver}))
            break;
            /**
             * @deprecated  epicbox protocol version 3.0.0
             */
            case "fastsend":
                ws.send(JSON.stringify({type:"Ok"}));
            break;
            case "clientdetails":
                clientdetails(ws, message);
            break;
        }
        // end switch message type

        console.log('[%s] - [%s][%s] -> [%s]', new Date().toLocaleTimeString(), ws.uid, ws.ip, type);
        config.debugMessage ? console.log("Message", message) : null;


    });
});


// shared helper, removes this socket's listener mapping
// Previously the same 4 lines were duplicated in several places and could
// delete another socket's mapping in edge cases

// now we only delete the entry if it still points at this socket.
const removeListenerMapping = (ws) => {
    if (ws.epicPublicAddress != null
        && ws.client_details.wallet_mode == 'listener'
        && clients_publicaddress[ws.epicPublicAddress] === ws) {
        delete clients_publicaddress[ws.epicPublicAddress];
    }
    ws.epicPublicAddress = null;
}

/*
    get current unix timestamp
*/
const getTimestamp = () => {
  return Math.floor(Date.now() / 1000);
}

// mongodb driver v7 returns BSON Binary (Uint8Array-backed) for
// stored Buffers. older records / driver versions may differ. Normalize to
// a utf8 string before JSON.parse instead of relying on implicit coercing

const payloadToString = (payload) => {
    if (typeof payload === 'string') return payload;
    if (payload && payload.buffer) return Buffer.from(payload.buffer).toString('utf8'); // BSON Binary
    return Buffer.from(payload).toString('utf8'); // Buffer / Uint8Array
}


const EPICBOX_ID_RE = /^[A-Za-z0-9_-]{32}$/;

const isEpicboxId = (value) => (
    typeof value === "string" && EPICBOX_ID_RE.test(value)
);

// Stable transaction IDs and transaction cancellation are protocol 3.1.0
// features. Older clients keep the legacy Slate/Ok/Error response vocabulary.
const supportsTxCancellation = (ws) =>
    ws != null && ws.epicboxver === "3.1.0";

const publicKeyFromAddress = (address) => {
    if (typeof address !== "string") return null;
    return address.split("@")[0] || null;
};

const endpointFromAddress = (address) => {
    if (typeof address !== "string") return null;

    const at = address.lastIndexOf("@");
    if (at === -1 || at === address.length - 1) return null;

    const endpoint = address.slice(at + 1);
    const colon = endpoint.lastIndexOf(":");

    if (colon > -1 && /^\d+$/.test(endpoint.slice(colon + 1))) {
        return {
            domain: endpoint.slice(0, colon),
            port: Number(endpoint.slice(colon + 1))
        };
    }

    return { domain: endpoint, port: 443 };
};

const relayIdentity = (domain, port) => `${domain}:${String(port)}`;

const currentRelayIdentity = () => relayIdentity(
    config.epicbox_domain,
    config.epicbox_port
);

const participantKeys = (records) => {
    const keys = new Set();

    for (const record of records) {
        for (const value of [
            record.queue,
            record.replyto,
            record.to,
            record.local_address,
            record.remote_address
        ]) {
            const key = publicKeyFromAddress(value);
            if (key) keys.add(key);
        }
    }

    return [...keys];
};

const recordHasParticipant = (record, publicKey) => (
    record != null && participantKeys([record]).includes(publicKey)
);

const remoteRelayEndpoints = (records) => {
    const endpoints = new Map();
    const current = currentRelayIdentity();

    const addEndpoint = (domain, port) => {
        if (typeof domain !== "string" || domain.length === 0) return;

        const normalizedPort = Number(port || 443);
        const id = relayIdentity(domain, normalizedPort);
        if (id === current) return;

        endpoints.set(id, { domain, port: normalizedPort });
    };

    for (const record of records) {
        addEndpoint(record.remote_domain, record.remote_port);

        // Fallback for records created before explicit relay routing fields.
        const replyEndpoint = endpointFromAddress(record.replyto);
        if (replyEndpoint) {
            addEndpoint(replyEndpoint.domain, replyEndpoint.port);
        }

        if (record.route === true) {
            const toEndpoint = endpointFromAddress(record.to);
            if (toEndpoint) {
                addEndpoint(toEndpoint.domain, toEndpoint.port);
            }
        }
    }

    return [...endpoints.values()];
};

const cancellationResponse = (epicboxtxid) => ({
    type: "TransactionCancelled",
    epicboxtxid
});

// Returns "pending" while a verified participant cancellation is being
// propagated, "cancelled" once the durable tombstone exists, or null when
// slate delivery is still allowed
const getCancellationState = async (epicboxtxid) => {
    if (!isEpicboxId(epicboxtxid)) return null;

    if (cancellationInProgress(epicboxtxid)) {
        return "pending";
    }

    const tombstone = await cancelledCollection.findOne(
        { epicboxtxid },
        { projection: { _id: 1, epicboxtxid: 1, participants: 1 } }
    );

    if (tombstone) {
        return "cancelled";
    }

    // findOne() yields to the event loop. Re-check the in-memory barrier in
    // case CancelTx became active while MongoDB was being queried
    if (cancellationInProgress(epicboxtxid)) {
        return "pending";
    }

    return null;
};

const sendCancellationToLocalReceiver = (
    epicboxtxid,
    receiver,
    requesterSocket
) => {
    const response = JSON.stringify(cancellationResponse(epicboxtxid));

    // Confirm the CancelTx request to its requester. This is protocol
    // confirmation only; it is not the receiver acknowledgement.
    if (requesterSocket && requesterSocket.readyState === WebSocket.OPEN) {
        requesterSocket.process_slate = false;
        requesterSocket.sendslate_attempts = 0;
        requesterSocket.send(response);
    }

    if (typeof receiver !== "string") {
        return;
    }

    const client = clients_publicaddress[receiver];

    if (
        client &&
        client !== requesterSocket &&
        client.readyState === WebSocket.OPEN &&
        supportsTxCancellation(client)
    ) {
        client.process_slate = false;
        client.sendslate_attempts = 0;
        client.send(response);

        // Do not mark the tombstone acknowledged merely because it was sent.
        // The wallet acknowledges it by successfully sending another Subscribe.
        client.pending_cancel_ack = epicboxtxid;
    }
};

const acknowledgePendingCancellation = async (ws) => {
    if (!supportsTxCancellation(ws)) {
        ws.pending_cancel_ack = null;
        return;
    }

    const epicboxtxid = ws.pending_cancel_ack;

    if (
        !isEpicboxId(epicboxtxid) ||
        typeof ws.epicPublicAddress !== "string"
    ) {
        ws.pending_cancel_ack = null;
        return;
    }

    const result = await cancelledCollection.updateOne(
        {
            epicboxtxid,
            receiver: ws.epicPublicAddress,
            receiver_acknowledged: { $ne: true }
        },
        {
            $set: {
                receiver_acknowledged: true,
                receiver_acknowledged_at: new Date()
            }
        }
    );

    if (result.modifiedCount > 0) {
        console.log(
            "Receiver acknowledged cancellation",
            epicboxtxid,
            ws.epicPublicAddress
        );
    }

    ws.pending_cancel_ack = null;
};

const sendPendingCancellation = async (ws) => {
    if (!supportsTxCancellation(ws)) {
        return false;
    }

    const tombstone = await cancelledCollection.findOne(
        {
            receiver: ws.epicPublicAddress,
            receiver_acknowledged: { $ne: true }
        },
        { sort: { cancelledat: 1 } }
    );

    if (!tombstone || !isEpicboxId(tombstone.epicboxtxid)) {
        return false;
    }

    ws.process_slate = false;
    ws.sendslate_attempts = 0;
    ws.send(JSON.stringify(cancellationResponse(tombstone.epicboxtxid)));

    // Keep the durable tombstone until this exact receiver proves it handled
    // the cancellation by successfully subscribing again.
    ws.pending_cancel_ack = tombstone.epicboxtxid;

    return true;
};

const relayCancelToRemote = (
    endpoint,
    requester,
    epicboxtxid,
    signature,
    cancelrequestid
) => new Promise((resolve) => {
    const sock = new WebSocket(
        `wss://${endpoint.domain}:${endpoint.port}`,
        {
            handshakeTimeout: 10000,
            maxPayload: config.ws_max_payload
        }
    );

    let settled = false;
    let requestSent = false;

    const finish = (ok) => {
        if (settled) return;

        settled = true;
        clearTimeout(timer);

        if (sock.readyState === WebSocket.OPEN) {
            sock.close(ok ? 1000 : 1011, ok ? "Cancel relayed." : "Cancel relay failed.");
        }

        resolve(ok);
    };

    const timer = setTimeout(() => {
        console.error(
            "Cancel relay timeout",
            endpoint.domain,
            endpoint.port,
            epicboxtxid
        );
        finish(false);
    }, 10000);

    sock.on("error", (err) => {
        console.error(
            "Cancel relay socket error",
            endpoint.domain,
            endpoint.port,
            err.message
        );
        finish(false);
    });

    sock.on("message", (data) => {
        try {
            const response = JSON.parse(data);

            if (response.type === "Challenge") {
                if (requestSent) return;
                requestSent = true;

                // The destination relay verifies the participant signature and
                // checks that this address belongs to the transaction.
                sock.send(JSON.stringify({
                    type: "CancelTx",
                    address: requester,
                    epicboxtxid,
                    signature,
                    cancelrequestid
                }));
                return;
            }

            if (response.type === "TransactionCancelled") {
                finish(response.epicboxtxid === epicboxtxid);
                return;
            }

            if (response.type === "Error") {
                console.error(
                    "Remote relay rejected cancellation",
                    endpoint.domain,
                    response.kind,
                    response.description
                );
                finish(false);
            }
        } catch (err) {
            console.error("Error parsing remote cancellation response", err);
            finish(false);
        }
    });
});

const sendNextPending = async (ws) => {
    // cancellation supersedes delivery of obsolete queued slate states.
    if (await sendPendingCancellation(ws)) return;

    if (ws.process_slate !== false) {
        ws.sendslate_attempts++;
        ws.send(JSON.stringify({ type: "Ok" }));
        return;
    }

    const records = await collection
        .find({
            queue: ws.epicPublicAddress,
            made: false,
            // route:true rows are metadata, not deliverable slates.
            route: { $ne: true }
        })
        .sort({ createdat: 1 })
        .limit(1)
        .toArray();

    if (!records || records.length === 0) {
        ws.send(JSON.stringify({ type: "Ok" }));
        return;
    }

    if (config.stats) statistics.slatesAttempt++;

    const dbslate = records[0];

    // rows created by older servers may not contain epicboxtxid. Preserve the
    // legacy delivery path for those records, but a new-protocol record whose A
    // is cancelling or cancelled must never be delivered.
    const epicboxtxid = isEpicboxId(dbslate.epicboxtxid)
        ? dbslate.epicboxtxid
        : null;

    if (epicboxtxid !== null) {
        const cancellationState = await getCancellationState(epicboxtxid);

        if (cancellationState === "cancelled") {
            // Tombstones remain authoritative even after the receiver has
            // acknowledged cancellation, so stale Slate states cannot resurrect A.
            await collection.deleteMany({ epicboxtxid });

            // Any outstanding receiver notification is handled by
            // sendPendingCancellation() at the top of this function.
            ws.process_slate = false;
            ws.sendslate_attempts = 0;
            ws.send(JSON.stringify({ type: "Ok" }));

            return;
        }

        if (cancellationState === "pending") {
            // cancellation may still fail on another relay, so do not delete
            // the slate yet. suppress delivery for this subscription cycle
            ws.send(JSON.stringify({ type: "Ok" }));
            return;
        }
    }

    const payload = JSON.parse(payloadToString(dbslate.payload));

    const slate = {
        type: "Slate",
        from: dbslate.replyto,
        str: payload.str,
        signature: payload.signature,
        challenge: payload.challenge
    };

    if (
        ws.epicboxver === "2.0.0" ||
        ws.epicboxver === "3.0.0" ||
        ws.epicboxver === "3.1.0"
    ) {
        slate.epicboxmsgid = dbslate.messageid;
        slate.ver = ws.epicboxver;

        // epicboxtxid is a 3.1.0 wire field. Keep older Slate shapes unchanged.
        if (supportsTxCancellation(ws) && epicboxtxid !== null) {
            slate.epicboxtxid = epicboxtxid;
        }
    } else {
        await collection.updateOne(
            { _id: dbslate._id },
            { $set: { made: true } }
        );
    }

    ws.send(JSON.stringify(slate));
    ws.process_slate = true;

    console.log(
        "Sent slate",
        dbslate.messageid,
        "for transaction",
        epicboxtxid || "<legacy-v3>",
        "to",
        ws.epicPublicAddress
    );

    if (config.debugMessage) console.log(slate);
};

// safe wrapper around execFile(config.pathtoepicboxlib, ...)
// The old code did (if (error) throw error) inside callback
// i.e. uncaught exception in Node

// Errors are now logged and reported back to this callback as verified=false

const epicboxlib = (args, cb) => {
    try {
        execFile(config.pathtoepicboxlib, args, (error, stdout, stderr) => {
            if (error) {
                console.error("Error execute epicboxlib", args[0], error);
                return cb(false);
            }
            cb(stdout === 'true');
        });
    } catch (err) {
        console.error("Error execute epicboxlib", err);
        cb(false);
    }
}

/*
    send challenge to client.
    the first challenge must use the old static challenge string for backward compatibility.
    older epicbox clients with  protocol version 2.0.0
    new epicbox/clients can use a dynamic challenge.
    //TODO if client blocks then this send messages are waiting in the queue
    @param {object} ws  - Client socket
*/
const challenge = (ws) => {
    // we do not know clients epicbox version on first challenge request.
    // todo. client should send its version when connect to epicbox via client_details
    let challenge = ws.epicboxver == "2.0.0" || ws.epicboxver == null ? static_challenge : uid(32);
    ws.challenge = challenge;
    ws.send(JSON.stringify({"type": "Challenge", "str": challenge}));
    ws.pending_challenge = true;
}


/*
 Information about the clients wallet version, Client command and supported epixbox protocol
 @param {object} ws  - Client socket
 @param {json} message - Client message see epic wallet
*/
const clientdetails = (ws, message) => {
    ws.client_details = message;
    ws.send(JSON.stringify({type:"Ok"}));
}


/*
 Subscribe
 validate client address and send back a pending slate
 @param {object} ws  - Client socket
 @param {json} message - Client message see epic wallet
*/
const subscribe = (ws, message) => {

    // Set the Epicbox protocol version used by this client.
    if (message.hasOwnProperty("ver")) {
        switch (message.ver) {
            case "2.0.0":
                ws.epicboxver = "2.0.0";
            break;
            case "3.1.0":
                ws.epicboxver = "3.1.0";
            break;
            default:
                // Preserve legacy behavior for 3.0.0 and unknown versions.
                ws.epicboxver = "3.0.0";
            break;
        }
    }

    epicboxlib(
        ["verifysignature", message.address, ws.challenge, message.signature],
        async (verified) => {
            if (!verified) {
                removeListenerMapping(ws);
                ws.send(JSON.stringify({
                    type: "Error",
                    kind: "InvalidSignature",
                    description: "Invalid signature."
                }));
                return;
            }

            if (config.stats) statistics.subscribeInHour++;

            ws.epicPublicAddress = message.address;

            // A TransactionCancelled notification is considered handled only
            // after the receiver successfully authenticates another Subscribe.
            // This survives socket reconnects without replaying acknowledged
            // tombstones forever.
            try {
                await acknowledgePendingCancellation(ws);
            } catch (err) {
                console.error(
                    "Error acknowledging pending cancellation",
                    err
                );
                ws.send(JSON.stringify({
                    type: "Error",
                    kind: "InvalidRequest",
                    description: "Unable to acknowledge cancellation."
                }));
                return;
            }

            if (
                clients_publicaddress[ws.epicPublicAddress] === undefined &&
                ws.client_details.wallet_mode === "listener"
            ) {
                clients_publicaddress[ws.epicPublicAddress] = ws;
            }

            ws.lastSubscriptionTime = getTimestamp();
            ws.pending_challenge = false;

            // If Made was not returned, permit redelivery after three
            // successful subscription cycles.
            if (
                ws.sendslate_attempts >= 3 &&
                ws.max_sendslate_attempts <= 3
            ) {
                ws.sendslate_attempts = 0;
                ws.max_sendslate_attempts++;
                ws.process_slate = false;
            }

            // After three reset rounds (nine attempts), remove only
            // undeliverable Slate rows. Never remove route metadata here.
            if (ws.max_sendslate_attempts >= 3) {
                collection.deleteMany({
                    queue: ws.epicPublicAddress,
                    made: false,
                    route: { $ne: true }
                }).then(() => {
                    ws.sendslate_attempts = 0;
                    ws.max_sendslate_attempts = 0;
                    ws.process_slate = false;
                    return sendNextPending(ws);
                }).catch((err) => {
                    console.error(
                        "Error cleaning or reading pending Epicbox work",
                        err
                    );
                    ws.send(JSON.stringify({ type: "Ok" }));
                });
                return;
            }

            sendNextPending(ws).catch((err) => {
                console.error("Error reading pending Epicbox work", err);
                ws.send(JSON.stringify({ type: "Ok" }));
            });
        }
    );
}


/*
 Unsubscribe and close client connection
 @param {object} ws  - Client socket
*/
const unsubscribe = (ws) => {

    if (ws.epicPublicAddress != null) {
        removeListenerMapping(ws);
        ws.close(1000, "Work complete.");
    }

}


/*
 client sends a new tx or a response to an tx
 validate address format and signature
 @param {object} ws  - Client socket
 @param {json} message - Client message see epic wallet
*/
const validatePostslate = (ws, message) => {
    try {
        const hasEpicboxTxId = Object.prototype.hasOwnProperty.call(
            message,
            "epicboxtxid"
        );
        const hasEpicboxTxIdSig = Object.prototype.hasOwnProperty.call(
            message,
            "epicboxtxidsig"
        );
        const hasStableTxId = hasEpicboxTxId && hasEpicboxTxIdSig;

        const validation = {
            from: typeof message.from,
            to: typeof message.to,
            str: typeof message.str,
            signature: typeof message.signature,
            has_epicboxtxid: hasEpicboxTxId,
            has_epicboxtxidsig: hasEpicboxTxIdSig,
            epicboxtxid: message.epicboxtxid,
            epicboxtxid_type: typeof message.epicboxtxid,
            epicboxtxid_valid: isEpicboxId(message.epicboxtxid),
            epicboxtxidsig_type: typeof message.epicboxtxidsig
        };

        // Legacy PostSlate has neither stable-ID field. Protocol 3.1.0 has both.
        // A partially-specified or malformed 3.1.0 request is always rejected.
        if (
            typeof message.from !== "string" ||
            typeof message.to !== "string" ||
            typeof message.str !== "string" ||
            typeof message.signature !== "string" ||
            hasEpicboxTxId !== hasEpicboxTxIdSig ||
            (
                hasStableTxId &&
                (
                    !isEpicboxId(message.epicboxtxid) ||
                    typeof message.epicboxtxidsig !== "string"
                )
            )
        ) {
            console.error(
                "Invalid PostSlate fields:",
                validation
            );

            return ws.send(JSON.stringify({
                type: "Error",
                kind: "InvalidRequest",
                description:
                    "PostSlate requires from, to, str, and signature. " +
                    "epicboxtxid and epicboxtxidsig must either both be absent " +
                    "(legacy) or both be valid (protocol 3.1.0)."
            }));
        }

        console.log("postslate from", message.from, "to", message.to);
        const publickey = publicKeyFromAddress(message.from);

        epicboxlib(
            ["verifyaddress", message.from, message.to],
            (addressOk) => {
                if (!addressOk) {
                    console.log(
                        "Error validate address format",
                        message.from,
                        message.to
                    );
                    ws.send(JSON.stringify({
                        type: "Error",
                        kind: "InvalidRequest",
                        description: `Wrong address format. From: ${message.from}, To: ${message.to}`
                    }));
                    return;
                }

                epicboxlib(
                    ["verifysignature", publickey, message.str, message.signature],
                    (signatureOk) => {
                        if (!signatureOk) {
                            console.log("Error postslate signature", publickey);
                            ws.send(JSON.stringify({
                                type: "Error",
                                kind: "InvalidRequest",
                                description: "Error postslate signature."
                            }));
                            return;
                        }

                        const acceptPostSlate = () => {
                            if (config.stats) statistics.slatesReceivedInHour++;

                            Promise.resolve(postSlate(ws, message)).catch((err) => {
                                console.error("Unhandled PostSlate failure", err);
                                if (ws.readyState === WebSocket.OPEN) {
                                    ws.send(JSON.stringify({
                                        type: "Error",
                                        kind: "InvalidRequest",
                                        description: "Unable to process PostSlate."
                                    }));
                                }
                            });
                        };

                        // The legacy request ends here; its original Slate
                        // signature validation remains unchanged.
                        if (!hasStableTxId) {
                            acceptPostSlate();
                            return;
                        }

                        epicboxlib(
                            [
                                "verifysignature",
                                publickey,
                                message.epicboxtxid,
                                message.epicboxtxidsig
                            ],
                            (txidSignatureOk) => {
                                if (!txidSignatureOk) {
                                    ws.send(JSON.stringify({
                                        type: "Error",
                                        kind: "InvalidSignature",
                                        description: "Invalid epicboxtxid signature."
                                    }));
                                    return;
                                }

                                // If this relay already knows A, require the
                                // posting key to be one of its participants.
                                collection.find({
                                    epicboxtxid: message.epicboxtxid
                                }).toArray().then((existingRecords) => {
                                    if (
                                        existingRecords.length > 0 &&
                                        !participantKeys(existingRecords).includes(publickey)
                                    ) {
                                        ws.send(JSON.stringify({
                                            type: "Error",
                                            kind: "InvalidRequest",
                                            description: "Posting address is not a participant of epicboxtxid."
                                        }));
                                        return;
                                    }

                                    acceptPostSlate();
                                }).catch((err) => {
                                    console.error(
                                        "Error checking epicboxtxid participant",
                                        err
                                    );
                                    ws.send(JSON.stringify({
                                        type: "Error",
                                        kind: "InvalidRequest",
                                        description: "Unable to validate epicboxtxid."
                                    }));
                                });
                            }
                        );
                    }
                );
            }
        );
    } catch (err) {
        console.error("Error postslate", err);
        ws.send(JSON.stringify({
            type: "Error",
            kind: "InvalidRequest",
            description: "Error processing PostSlate."
        }));
    }
}

/*
 client sends made response if successfully processed slate
 @param {object} ws  - Client socket
 @param {json} message - Client message see epic wallet
*/
const made = (ws, message) => {

    if (
        ws.epicPublicAddress != null &&
        message.hasOwnProperty("epicboxmsgid") &&
        message.hasOwnProperty("ver") &&
        (
            message.ver == "2.0.0" ||
            message.ver == "3.0.0" ||
            message.ver == "3.1.0"
        )
    ) {
        let args = [];
        if (message.ver == "3.0.0" || message.ver == "3.1.0") {
            args = ["verifysignature", ws.epicPublicAddress, message.epicboxmsgid, message.signature];
        } else {
            args = ["verifysignature", ws.epicPublicAddress, ws.challenge, message.signature];
        }

        epicboxlib(args, (verified) => {

            if (verified) {
                console.log("Update for ", message.epicboxmsgid);
                collection.findOne({
                    queue: ws.epicPublicAddress,
                    messageid: message.epicboxmsgid
                }).then((dbslate) => {
                    const filter = dbslate && isEpicboxId(dbslate.epicboxtxid)
                        ? {
                            queue: ws.epicPublicAddress,
                            epicboxtxid: dbslate.epicboxtxid,
                            made: false,
                            route: { $ne: true }
                        }
                        : {
                            queue: ws.epicPublicAddress,
                            messageid: message.epicboxmsgid,
                            made: false
                        };

                    return collection.updateMany(filter, { $set: {made: true}});
                }).then((updateResult) => {
                    config.debugMessage ? console.log("DB update result", updateResult) : null;
                    ws.send(JSON.stringify({type:"Ok"}));
                    ws.process_slate = false;
                    ws.sendslate_attempts = 0;
                    // if this slate was processed then send the next slate to client via challenge->subscribe
                    challenge(ws);

                }).catch((err) => {
                    console.error("Error update made flag", err);
                });
            } else {

                ws.send(JSON.stringify({type:"Error", kind:"InvalidSignature", description: `Invalid signature.`}));
            }
        });
    }
}


/*
 Cancel every queued Slate state associated with one stable epicboxtxid.

 epicboxmsgid is a per-message Made acknowledgement handle. It is not used
 for cancellation. CancelTx and TransactionCancelled use only epicboxtxid.

 The requester must be a transaction participant. Unknown transaction IDs
 and non-participants receive the same ambiguous Ok response to prevent
 relay record probing.

 @param {object} ws - Client or forwarding relay socket
 @param {json} message - {
     type: "CancelTx",
     address: "<participant public key>",
     epicboxtxid: "<32-char stable transaction id>",
     signature: "<signature over epicboxtxid>",
     cancelrequestid?: "<32-char relay loop guard>"
 }
*/
const canceltx = (ws, message) => {
    // Wallets normally prove ownership through Subscribe. A relay forwarding
    // a participant-signed cancellation supplies message.address instead.
    const requester = ws.epicPublicAddress != null
        ? ws.epicPublicAddress
        : publicKeyFromAddress(message.address);

    if (requester == null) {
        return ws.send(JSON.stringify({
            type: "Error",
            kind: "InvalidRequest",
            description: "Missing cancellation address."
        }));
    }

    const epicboxtxid = isEpicboxId(message.epicboxtxid)
        ? message.epicboxtxid
        : null;

    if (epicboxtxid === null) {
        return ws.send(JSON.stringify({
            type: "Error",
            kind: "InvalidRequest",
            description: "Invalid or missing 32-character epicboxtxid."
        }));
    }

    console.log(
        "canceltx (pre-signature-check) request received from",
        requester,
        "for tx with epicboxtxid: ",
        epicboxtxid
    );


    if (typeof message.signature !== "string") {
        return ws.send(JSON.stringify({
            type: "Error",
            kind: "InvalidRequest",
            description: "Missing signature."
        }));
    }

    epicboxlib(
        ["verifysignature", requester, epicboxtxid, message.signature],
        async (verified) => {
            if (!verified) {
                ws.send(JSON.stringify({
                    type: "Error",
                    kind: "InvalidSignature",
                    description: "Invalid signature."
                }));
                return;
            }

            try {
                const tombstone = await cancelledCollection.findOne({
                    epicboxtxid
                });

                if (tombstone) {
                    const participants = Array.isArray(tombstone.participants)
                        ? tombstone.participants
                        : participantKeys([tombstone]);

                    if (participants.includes(requester)) {
                        ws.send(JSON.stringify(cancellationResponse(epicboxtxid)));
                    } else {
                        // Do not disclose another transaction's existence.
                        ws.send(JSON.stringify({ type: "Ok" }));
                    }
                    return;
                }

                const records = await collection.find({ epicboxtxid }).toArray();

                if (!records || records.length === 0) {
                    console.log(
                        "canceltx: no matching transaction for",
                        requester,
                        epicboxtxid
                    );
                    ws.send(JSON.stringify({ type: "Ok" }));
                    return;
                } else {
                    console.log(
                        "canceltx: found matching pending transaction for",
                        requester,
                        epicboxtxid
                    );
                }

                const participants = participantKeys(records);
                if (!participants.includes(requester)) {
                    ws.send(JSON.stringify({ type: "Ok" }));
                    return;
                }

                // Epicbox transactions are two-party. Notify only the other
                // participant, not every participant socket on every reconnect.
                const receiverCandidates = participants.filter(
                    (participant) => participant !== requester
                );
                const receiver = receiverCandidates.length === 1
                    ? receiverCandidates[0]
                    : null;

                if (receiverCandidates.length !== 1) {
                    console.warn(
                        "Unable to determine a single cancellation receiver for",
                        epicboxtxid,
                        receiverCandidates
                    );
                }

                const cancelrequestid = isEpicboxId(message.cancelrequestid)
                    ? message.cancelrequestid
                    : uid(32);
                const activeKey = `${epicboxtxid}:${cancelrequestid}`;

                // A remote relay may route this request back to its source.
                // Confirm the in-flight request instead of recursing.
                if (activeCancellations.has(activeKey)) {
                    ws.send(JSON.stringify(cancellationResponse(epicboxtxid)));
                    return;
                }

                activeCancellations.add(activeKey);
                beginCancellation(epicboxtxid);

                try {
                    const remoteEndpoints = remoteRelayEndpoints(records);
                    const remoteResults = await Promise.all(
                        remoteEndpoints.map((endpoint) => relayCancelToRemote(
                            endpoint,
                            requester,
                            epicboxtxid,
                            message.signature,
                            cancelrequestid
                        ))
                    );

                    if (remoteResults.some((confirmed) => !confirmed)) {
                        ws.send(JSON.stringify({
                            type: "Error",
                            kind: "InvalidRequest",
                            description: "Cancel failed on a participant relay; try again."
                        }));
                        return;
                    }

                    // Record the transaction-wide tombstone before deleting
                    // any individual Slate states. The tombstone remains as a
                    // cancellation barrier until TTL expiry, while
                    // receiver_acknowledged controls whether it still needs to
                    // be delivered to the peer.
                    await cancelledCollection.updateOne(
                        { epicboxtxid },
                        {
                            $setOnInsert: {
                                epicboxtxid,
                                participants,
                                receiver,
                                receiver_acknowledged: receiver === null,
                                cancelledat: new Date()
                            }
                        },
                        { upsert: true }
                    );

                    const deleteResult = await collection.deleteMany({
                        epicboxtxid
                    });

                    console.log(
                        "canceltx: deleted",
                        deleteResult.deletedCount,
                        "state records for",
                        epicboxtxid
                    );

                    sendCancellationToLocalReceiver(
                        epicboxtxid,
                        receiver,
                        ws
                    );
                } finally {
                    endCancellation(epicboxtxid);
                    activeCancellations.delete(activeKey);
                }
            } catch (err) {
                console.error("Error cancelling transaction", err);
                ws.send(JSON.stringify({
                    type: "Error",
                    kind: "InvalidRequest",
                    description: "Cancel failed; try again."
                }));
            }
        }
    );
}

/*
 store tx in db or forward to foreign epicbox
 if domain does not match our epicbox domain
 @param {object} ws  - Client socket
 @param {json} message - Client message see epic wallet
*/
const postSlate = async (ws, json) => {
    let str;

    try {
        str = JSON.parse(json.str);
    } catch (err) {
        console.log("Error parsing message string", err);
        return ws.send(JSON.stringify({
            type: "Error",
            kind: "InvalidRequest",
            description: "Invalid Slate payload."
        }));
    }

    if (
        !str ||
        typeof str !== "object" ||
        !str.destination ||
        typeof str.destination.public_key !== "string" ||
        typeof str.destination.domain !== "string"
    ) {
        return ws.send(JSON.stringify({
            type: "Error",
            kind: "InvalidRequest",
            description: "Invalid slate destination."
        }));
    }

    const addressto = {
        publicKey: str.destination.public_key,
        domain: str.destination.domain,
        port: str.destination.port != null ? str.destination.port : 443
    };

    const isLocalDestination = (
        addressto.domain === config.epicbox_domain &&
        String(addressto.port) === String(config.epicbox_port)
    );
    const hasStableTxId = (
        isEpicboxId(json.epicboxtxid) &&
        typeof json.epicboxtxidsig === "string"
    );

    // A durable tombstone is terminal, and a verified cancellation currently
    // being propagated temporarily suppresses all later Slate states for A.
    const cancellationState = hasStableTxId
        ? await getCancellationState(json.epicboxtxid)
        : null;

    if (cancellationState === "cancelled") {
        console.log(
            "Rejecting PostSlate for cancelled transaction",
            json.epicboxtxid
        );
        return ws.send(JSON.stringify(cancellationResponse(json.epicboxtxid)));
    }

    if (cancellationState === "pending") {
        console.log(
            "Rejecting PostSlate while cancellation is in progress",
            json.epicboxtxid
        );
        return ws.send(JSON.stringify({
            type: "Error",
            kind: "InvalidRequest",
            description: "Transaction cancellation is in progress; PostSlate rejected."
        }));
    }

    if (isLocalDestination) {
        const signedPayload = JSON.stringify({
            str: json.str,
            challenge: "",
            signature: json.signature
        });

        // messageid is unique to this queued Slate. Protocol 3.1.0 also carries
        // a sender-generated epicboxtxid that is stable for the transaction.
        const messageid = uid(32);
        const epicboxtxid = hasStableTxId ? json.epicboxtxid : null;
        const sourceEndpoint = endpointFromAddress(json.from);

        const record = {
            queue: addressto.publicKey,
            made: false,
            route: false,
            payload: Buffer.from(signedPayload),
            replyto: json.from,
            to: json.to,
            createdat: new Date(),
            expiration: 86400000,
            messageid,
            remote_domain: sourceEndpoint?.domain,
            remote_port: sourceEndpoint?.port
        };

        // Keep legacy records free of 3.1.0-only stable-ID fields.
        if (hasStableTxId) {
            record.epicboxtxid = epicboxtxid;
            record.epicboxtxidsig = json.epicboxtxidsig;
        }

        let insertResult;

        try {
            insertResult = await collection.insertOne(record);
        } catch (err) {
            console.error("Error insert to db", err);
            return ws.send(JSON.stringify({
                type: "Error",
                kind: "InvalidRequest",
                description: "Unable to queue Slate."
            }));
        }

        // insertOne() yields to the event loop. if cancellation started or
        // completed while the row was being written, remove exactly the row we
        // just created and do not expose it to the receiver
        const postInsertCancellationState = hasStableTxId
            ? await getCancellationState(epicboxtxid)
            : null;

        if (postInsertCancellationState !== null) {
            await collection.deleteOne({ _id: insertResult.insertedId });

            if (postInsertCancellationState === "cancelled") {
                return ws.send(JSON.stringify(cancellationResponse(epicboxtxid)));
            }

            return ws.send(JSON.stringify({
                type: "Error",
                kind: "InvalidRequest",
                description: "Transaction cancellation is in progress; PostSlate rejected."
            }));
        }

        const response = hasStableTxId
            ? {
                type: "Ok",
                epicboxmsgid: messageid,
                epicboxtxid
            }
            : { type: "Ok" };
        const receiver = clients_publicaddress[addressto.publicKey];

        if (
            receiver !== undefined &&
            receiver.process_slate === false &&
            receiver.readyState === WebSocket.OPEN
        ) {
            if (config.stats) statistics.slatesAttempt++;

            const slate = {
                type: "Slate",
                from: json.from,
                str: json.str,
                signature: json.signature,
                challenge: ""
            };

            if (
                receiver.epicboxver === "2.0.0" ||
                receiver.epicboxver === "3.0.0" ||
                receiver.epicboxver === "3.1.0"
            ) {
                slate.epicboxmsgid = messageid;
                slate.ver = receiver.epicboxver;

                if (supportsTxCancellation(receiver) && hasStableTxId) {
                    slate.epicboxtxid = epicboxtxid;
                }
            } else {
                collection.updateOne(
                    { messageid },
                    { $set: { made: true } }
                ).catch((err) => console.error("Error updateOne", err));
            }

            ws.send(JSON.stringify(response));
            receiver.send(JSON.stringify(slate));
            receiver.process_slate = true;

            console.log(
                "Passthrough slate",
                messageid,
                "for transaction",
                epicboxtxid,
                "to",
                receiver.epicPublicAddress
            );

            if (config.debugMessage) console.log(slate);
        } else {
            ws.send(JSON.stringify(response));
        }

        return;
    }

    // Foreign relay passthrough. Every participating relay must preserve and
    // acknowledge the sender-generated epicboxtxid.
    const sock = new WebSocket(
        `wss://${addressto.domain}:${addressto.port}`,
        {
            handshakeTimeout: 10000,
            maxPayload: config.ws_max_payload
        }
    );

    sock.on("error", (err) => {
        console.error("Relay socket error", addressto.domain, err.message);
    });

    sock.on("open", () => {
        console.log(`Connect ${addressto.domain}:${addressto.port}`);
    });

    sock.on("message", async (data) => {
        try {
            const message = JSON.parse(data);

            if (message.type === "Challenge") {
                // re-check immediately before the cross-relay send. the
                // outbound ws handshake yields to the event loop so a
                // cancellation may have started since postSlate() began
                const relayCancellationState = hasStableTxId
                    ? await getCancellationState(json.epicboxtxid)
                    : null;

                if (relayCancellationState !== null) {
                    if (ws.readyState === WebSocket.OPEN) {
                        if (relayCancellationState === "cancelled") {
                            ws.send(JSON.stringify(
                                cancellationResponse(json.epicboxtxid)
                            ));
                        } else {
                            ws.send(JSON.stringify({
                                type: "Error",
                                kind: "InvalidRequest",
                                description: "Transaction cancellation is in progress; PostSlate rejected."
                            }));
                        }
                    }

                    sock.close(
                        1000,
                        "PostSlate suppressed by transaction cancellation."
                    );
                    return;
                }

                const relaySlate = {
                    type: "PostSlate",
                    from: json.from,
                    to: json.to,
                    str: json.str,
                    signature: json.signature
                };

                if (hasStableTxId) {
                    relaySlate.epicboxtxid = json.epicboxtxid;
                    relaySlate.epicboxtxidsig = json.epicboxtxidsig;
                }

                sock.send(JSON.stringify(relaySlate));
                return;
            }

            if (message.type === "Error") {
                console.error(
                    "Foreign relay rejected PostSlate",
                    addressto.domain,
                    message.kind,
                    message.description
                );

                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify(message));
                }
                sock.close(1002, "Remote relay rejected PostSlate.");
                return;
            }

            if (message.type !== "Ok") return;

            const remoteAckHasTxId = Object.prototype.hasOwnProperty.call(
                message,
                "epicboxtxid"
            );

            // A bare Ok is the legacy relay acknowledgement. If a relay includes
            // epicboxtxid, it must be valid and must match the ID we sent.
            if (
                remoteAckHasTxId &&
                (
                    !hasStableTxId ||
                    !isEpicboxId(message.epicboxtxid) ||
                    message.epicboxtxid !== json.epicboxtxid
                )
            ) {
                console.error(
                    "Foreign relay returned an invalid or mismatched epicboxtxid",
                    {
                        expected: hasStableTxId ? json.epicboxtxid : "<legacy>",
                        returned: message.epicboxtxid,
                        relay: `${addressto.domain}:${addressto.port}`
                    }
                );

                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({
                        type: "Error",
                        kind: "InvalidRequest",
                        description: "Foreign relay returned an invalid PostSlate acknowledgement."
                    }));
                }
                sock.close(1002, "Invalid PostSlate acknowledgement.");
                return;
            }

            if (config.stats) statistics.slatesRelayedInHour++;

            const remoteMessageId = isEpicboxId(message.epicboxmsgid)
                ? message.epicboxmsgid
                : null;
            const epicboxtxid = hasStableTxId ? json.epicboxtxid : null;

            if (hasStableTxId) {
                // Never deliver route rows as slates. They retain enough
                // information to propagate a later transaction-wide cancel. A
                // legacy remote relay may return bare Ok; keep A locally anyway.
                await collection.updateOne(
                    {
                        route: true,
                        epicboxtxid,
                        local_address: publicKeyFromAddress(json.from),
                        remote_domain: addressto.domain,
                        remote_port: Number(addressto.port)
                    },
                    {
                        $setOnInsert: {
                            route: true,
                            made: true,
                            queue: addressto.publicKey,
                            replyto: json.from,
                            to: json.to,
                            local_address: publicKeyFromAddress(json.from),
                            remote_address: addressto.publicKey,
                            remote_domain: addressto.domain,
                            remote_port: Number(addressto.port),
                            createdat: new Date(),
                            expiration: 86400000,
                            messageid: remoteMessageId || uid(32),
                            epicboxtxid
                        }
                    },
                    { upsert: true }
                );
            }

            const response = hasStableTxId
                ? {
                    type: "Ok",
                    epicboxtxid
                }
                : { type: "Ok" };

            if (hasStableTxId && remoteMessageId) {
                response.epicboxmsgid = remoteMessageId;
            }

            console.log(
                `Sent to wss://${addressto.domain}:${addressto.port}`,
                hasStableTxId
                    ? `for transaction ${epicboxtxid}`
                    : "using legacy PostSlate"
            );

            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify(response));
            }
            sock.close(1000, "Relay complete.");
        } catch (err) {
            console.error("Error forwarding Slate to foreign Epicbox", err);
            ws.send(JSON.stringify({
                type: "Error",
                kind: "InvalidRequest",
                description: `Error forwarding Slate to foreign Epicbox ${addressto.domain}:${addressto.port}.`
            }));
            sock.close(1002, "Relay error.");
        }
    });
}

/*
    send recurring challenge -> subscribe cycles to all clients
*/
const challengeInterval = () => {

    wss.clients.forEach((ws) => {

        if (ws.readyState === WebSocket.OPEN
            && ws.epicPublicAddress !== null
            // do not spam clients with challenge requests
            // do not send new challenge if old challenge request was not subscribed (when client blocks)

            // fix for lastSubscriptionTime (seconds), challenge_interval (milliseconds)
            && (ws.pending_challenge == false || (getTimestamp() - ws.lastSubscriptionTime >= config.challenge_interval / 1000))
        ) {
            try {

                challenge(ws);
            } catch (err) {
                console.log("Send Interval challenge error ", err);
            }
        }

    });

}

/*
    load config for epixbox custom settings
*/
const loadConfig = async (filePath) => {


    try {
        let jsonData = fs.readFileSync(filePath, 'utf8');
        let data = JSON.parse(jsonData);

        // priority is ENV > config file > default
        config.mongourl = process.env.MONGO_URL || data.mongo_url || config.mongourl;
        config.epicbox_domain = process.env.EPICBOX_DOMAIN || data.epicbox_domain || config.epicbox_domain;
        config.epicbox_port = process.env.EPICBOX_PORT || data.epicbox_port || config.epicbox_port;
        config.localepicboxserviceport = process.env.LOCAL_EPICBOX_SERVICE_PORT || data.local_epicbox_service_port || config.localepicboxserviceport;
        config.pathtoepicboxlib = process.env.PATH_TO_EPICBOXLIB_EXEC_FILE || data.path_to_epicboxlib_exec_file || config.pathtoepicboxlib;
        config.db_name = process.env.MONGO_DBNAME || data.mongo_dbName || config.db_name;
        config.collection_name = process.env.MONGO_COLLECTION_NAME || data.mongo_collection_name || config.collection_name;
        config.cancelled_collection_name = process.env.MONGO_CANCELLED_COLLECTION_NAME || data.mongo_cancelled_collection_name || config.cancelled_collection_name;

        // env vars are strings, convert to number for interval math
        config.challenge_interval = Number(process.env.CHALLENGE_INTERVAL || data.challenge_interval || config.challenge_interval);
        config.debugMessage = process.env.DEBUG !== undefined ? process.env.DEBUG === 'true' : (data.debug !== undefined ? data.debug : config.debugMessage);
        config.stats = process.env.STATS !== undefined ? process.env.STATS === 'true' : (data.stats !== undefined ? data.stats : config.stats);

    } catch (err) {
        console.error(err);
    }

}

const startEpicbox = async () => {

    let configPath = customConfig != -1 && process.argv[customConfig + 1] != undefined ? process.argv[customConfig + 1] : './config.json';
    console.log("Use config:", configPath);
    await loadConfig(configPath);

    // fail fast if mongo is unreachable instead of driver default hang (30 sec)
    mongoclient = new MongoClient(config.mongourl, {
        serverSelectionTimeoutMS: 10000
    });

    let db = mongoclient.db(config.db_name);
    collection = db.collection(config.collection_name);
    cancelledCollection = db.collection(config.cancelled_collection_name);

    await mongoclient.connect();

    console.log('Connected successfully to MongoDB');
    server.listen(config.localepicboxserviceport);
    setInterval(challengeInterval, config.challenge_interval);
    console.log("Epicbox ready to work.");

}


// mongoclient.close() is async in driver v5+
// await so connection tears down before exit
const handle = async (signal) => {
    console.log(`So the signal which I have Received is: ${signal}`);

    wss.clients.forEach(function each(client) {
      client.close(1001, "Server shutting down.");
    });

    try {
        if (mongoclient) {
            await mongoclient.close();
        }
    } catch (err) {
        console.error("Error closing MongoDB connection", err);
    }

    process.exit(0);
}

process.on('SIGINT', handle);
process.on('SIGBREAK', handle);
// systemd/docker send SIGTERM
process.on('SIGTERM', handle);
// SIGKILL cannot be caught
//process.on("SIGKILL", handle);

// last-resort guards against bad msgs
process.on('uncaughtException', (err) => {
    console.error('Uncaught exception:', err);
});

process.on('unhandledRejection', (reason) => {
    console.error('Unhandled rejection:', reason);
});

startEpicbox().catch((err) => {
    console.error("Fatal: failed to start Epicbox", err);
    process.exit(1);
});

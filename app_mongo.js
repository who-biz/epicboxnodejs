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
const protver = "3.0.0";
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
    // Tombstones are re-delivered after reconnect, but only once per socket.
    ws.sent_cancelled_txids = new Set();
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

const sendCancellationToLocalParticipants = (
    epicboxtxid,
    participants,
    requesterSocket
) => {
    const response = JSON.stringify(cancellationResponse(epicboxtxid));

    if (requesterSocket && requesterSocket.readyState === WebSocket.OPEN) {
        requesterSocket.process_slate = false;
        requesterSocket.sendslate_attempts = 0;
        requesterSocket.send(response);
        requesterSocket.sent_cancelled_txids?.add(epicboxtxid);
    }

    for (const participant of participants) {
        const client = clients_publicaddress[participant];

        if (
            client &&
            client !== requesterSocket &&
            client.readyState === WebSocket.OPEN
        ) {
            client.process_slate = false;
            client.sendslate_attempts = 0;
            client.send(response);
            client.sent_cancelled_txids?.add(epicboxtxid);
        }
    }
};

const sendPendingCancellation = async (ws) => {
    const tombstone = await cancelledCollection.findOne(
        {
            participants: ws.epicPublicAddress,
            epicboxtxid: { $nin: [...ws.sent_cancelled_txids] }
        },
        { sort: { cancelledat: 1 } }
    );

    if (!tombstone || !isEpicboxId(tombstone.epicboxtxid)) {
        return false;
    }

    ws.process_slate = false;
    ws.sendslate_attempts = 0;
    ws.send(JSON.stringify(cancellationResponse(tombstone.epicboxtxid)));
    ws.sent_cancelled_txids.add(tombstone.epicboxtxid);

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
    // Cancellation supersedes delivery of obsolete queued Slate states.
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
            // route:true rows are metadata, not deliverable Slates.
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
    const payload = JSON.parse(payloadToString(dbslate.payload));
    const epicboxtxid = isEpicboxId(dbslate.epicboxtxid)
        ? dbslate.epicboxtxid
        : dbslate.messageid;

    const slate = {
        type: "Slate",
        from: dbslate.replyto,
        str: payload.str,
        signature: payload.signature,
        challenge: payload.challenge
    };

    if (ws.epicboxver === "2.0.0" || ws.epicboxver === "3.0.0") {
        slate.epicboxmsgid = dbslate.messageid;
        slate.epicboxtxid = epicboxtxid;
        slate.ver = ws.epicboxver;
    } else {
        // Preserve legacy behavior for clients that did not advertise the
        // versioned protocol and may reject unknown fields.
        collection.updateOne(
            { messageid: dbslate.messageid },
            { $set: { made: true } }
        ).catch((err) => console.error("Error updateOne", err));
    }

    ws.send(JSON.stringify(slate));
    ws.process_slate = true;

    console.log(
        "Sent slate",
        dbslate.messageid,
        "for transaction",
        epicboxtxid,
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
            default:
                ws.epicboxver = "3.0.0";
            break;
        }
    }

    epicboxlib(
        ["verifysignature", message.address, ws.challenge, message.signature],
        (verified) => {
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
        if (
            typeof message.from !== "string" ||
            typeof message.to !== "string" ||
            typeof message.str !== "string" ||
            typeof message.signature !== "string"
        ) {
            return ws.send(JSON.stringify({
                type: "Error",
                kind: "InvalidRequest",
                description: "Missing fields."
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
                            postSlate(ws, message);
                        };

                        // The first state has no stable relay ID yet; the
                        // destination relay creates it.
                        if (message.epicboxtxid === undefined) {
                            acceptPostSlate();
                            return;
                        }

                        if (!isEpicboxId(message.epicboxtxid)) {
                            ws.send(JSON.stringify({
                                type: "Error",
                                kind: "InvalidRequest",
                                description: "Invalid epicboxtxid."
                            }));
                            return;
                        }

                        if (typeof message.epicboxtxidsig !== "string") {
                            ws.send(JSON.stringify({
                                type: "Error",
                                kind: "InvalidRequest",
                                description: "Missing epicboxtxid signature."
                            }));
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

    if (ws.epicPublicAddress != null && message.hasOwnProperty("epicboxmsgid") && message.hasOwnProperty("ver") && (message.ver == "2.0.0" || message.ver == "3.0.0")) {
        let args = [];
        if (message.ver == "3.0.0") {
            args = ["verifysignature", ws.epicPublicAddress, message.epicboxmsgid, message.signature];
        } else {
            args = ["verifysignature", ws.epicPublicAddress, ws.challenge, message.signature];
        }

        epicboxlib(args, (verified) => {

            if (verified) {
                console.log("Update for ", message.epicboxmsgid);
                collection.updateOne({queue: ws.epicPublicAddress, messageid: message.epicboxmsgid, made: false}, { $set: {made: true}}).then((updateResult) => {
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
                }

                const participants = participantKeys(records);
                if (!participants.includes(requester)) {
                    ws.send(JSON.stringify({ type: "Ok" }));
                    return;
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
                    // any individual Slate states.
                    await cancelledCollection.updateOne(
                        { epicboxtxid },
                        {
                            $setOnInsert: {
                                epicboxtxid,
                                participants,
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

                    sendCancellationToLocalParticipants(
                        epicboxtxid,
                        participants,
                        ws
                    );
                } finally {
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

    if (isLocalDestination) {
        const signedPayload = JSON.stringify({
            str: json.str,
            challenge: "",
            signature: json.signature
        });

        // M_n is unique to this queued Slate; A is stable for the transaction.
        const messageid = uid(32);
        const epicboxtxid = isEpicboxId(json.epicboxtxid)
            ? json.epicboxtxid
            : messageid;
        const sourceEndpoint = endpointFromAddress(json.from);

        try {
            await collection.insertOne({
                queue: addressto.publicKey,
                made: false,
                route: false,
                payload: Buffer.from(signedPayload),
                replyto: json.from,
                to: json.to,
                createdat: new Date(),
                expiration: 86400000,
                messageid,
                epicboxtxid,
                epicboxtxidsig: json.epicboxtxidsig,
                remote_domain: sourceEndpoint?.domain,
                remote_port: sourceEndpoint?.port
            });
        } catch (err) {
            console.error("Error insert to db", err);
            return ws.send(JSON.stringify({
                type: "Error",
                kind: "InvalidRequest",
                description: "Unable to queue Slate."
            }));
        }

        const response = {
            type: "Ok",
            epicboxmsgid: messageid,
            epicboxtxid
        };
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
                receiver.epicboxver === "3.0.0"
            ) {
                slate.epicboxmsgid = messageid;
                slate.epicboxtxid = epicboxtxid;
                slate.ver = receiver.epicboxver;
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

    // Foreign relay passthrough. New fields are additive; an old JavaScript
    // relay ignores them and continues handling the original PostSlate fields.
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
                const relaySlate = {
                    type: "PostSlate",
                    from: json.from,
                    to: json.to,
                    str: json.str,
                    signature: json.signature
                };

                if (isEpicboxId(json.epicboxtxid)) {
                    relaySlate.epicboxtxid = json.epicboxtxid;
                    relaySlate.epicboxtxidsig = json.epicboxtxidsig;
                }

                sock.send(JSON.stringify(relaySlate));
                return;
            }

            if (message.type !== "Ok") return;

            if (config.stats) statistics.slatesRelayedInHour++;

            const remoteMessageId = isEpicboxId(message.epicboxmsgid)
                ? message.epicboxmsgid
                : null;
            const epicboxtxid = isEpicboxId(message.epicboxtxid)
                ? message.epicboxtxid
                : (
                    isEpicboxId(json.epicboxtxid)
                        ? json.epicboxtxid
                        : remoteMessageId
                );

            if (epicboxtxid) {
                // Never deliver route rows as Slates. They retain enough
                // information to propagate a later transaction-wide cancel.
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

            const response = { type: "Ok" };
            if (remoteMessageId) response.epicboxmsgid = remoteMessageId;
            if (epicboxtxid) response.epicboxtxid = epicboxtxid;

            console.log(
                `Sent to wss://${addressto.domain}:${addressto.port}`,
                epicboxtxid ? `for transaction ${epicboxtxid}` : ""
            );

            ws.send(JSON.stringify(response));
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

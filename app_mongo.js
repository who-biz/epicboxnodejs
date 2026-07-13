//
// Add indexes in mongo
// Before start create user and indexes
/*


 db.slates.createIndex({queue:1, made:1, createdat: 1});
 db.slates.createIndex({messageid:1, made:1});
 db.slates.createIndex({ "createdat": 1 }, {expireAfterSeconds: 604800 });
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
const config = {
    mongourl: "mongodb://127.0.0.1:27019",
    epicbox_domain: process.env.EPICBOX_DOMAIN || "epicbox.your-domain.com",
    epicbox_port: process.env.EPICBOX_PORT || 443,
    localepicboxserviceport: "3423",
    pathtoepicboxlib: "./epicboxlib",
    db_name: "epicbox",
    collection_name: "slates",
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
            // participant cancels a queued tx by its slate/message id
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

    // set used epicbox protocol version
    if (message.hasOwnProperty("ver")) {
        switch (message.ver) {
            case "2.0.0":
                ws.epicboxver = "2.0.0";
            break;
            default:
                // new version is
                ws.epicboxver = "3.0.0";
            break;
        }
    }

    // verify that client is the owner of the public key
    epicboxlib(["verifysignature", message.address, ws.challenge, message.signature], (verified) => {

        // if signature is OK
        if (verified) {

            if (config.stats) {
                statistics.subscribeInHour++;
            }

            // client proved that he is the owner of the public address
            ws.epicPublicAddress = message.address;

            // add client listener for passthrough slates;
            if (clients_publicaddress[ws.epicPublicAddress] == undefined && ws.client_details.wallet_mode == 'listener') {
                clients_publicaddress[ws.epicPublicAddress] = ws;
            }

            ws.lastSubscriptionTime = getTimestamp();
            ws.pending_challenge = false;

            // if at some case a made request was not send back from client
            // we set 'process_slate' back to false after 3 successfully subscriptions
            // and let the client try to process not made slates again.
            // max resets are limited to 3 rounds.
            if (ws.sendslate_attempts >= 3 && ws.max_sendslate_attempts <= 3) {
                ws.sendslate_attempts = 0;
                ws.max_sendslate_attempts++;
                ws.process_slate = false;
            }

            // if it's not possible for client to process not made slates after 3 rounds (=9 attempts),
            // then delete all not made slates from client in db
            if (ws.max_sendslate_attempts >= 3) {
                collection.deleteMany({ queue: ws.epicPublicAddress, made: false })
                    .catch((err) => console.error("Error deleteMany", err));
                ws.sendslate_attempts = 0;
                ws.max_sendslate_attempts = 0;
                ws.process_slate = false;
            }

            // get not processed tx for client
            // prevent sending same slate multible times
            if (ws.process_slate == false) {
                collection.find({ queue: ws.epicPublicAddress, made: false }).sort({ "createdat": 1 }).limit(1).toArray().then((res) => {

                    if (res && res.length > 0) {

                        if (config.stats) {
                            statistics.slatesAttempt++;
                        }

                        let dbslate = res[0];
                        let payload = JSON.parse(payloadToString(dbslate.payload));
                        let slate = {
                            type: "Slate",
                            from: dbslate.replyto,
                            str: payload.str,
                            signature: payload.signature,
                            challenge: payload.challenge,
                        };

                        if (ws.epicboxver == "2.0.0" || ws.epicboxver == "3.0.0") {
                            slate.epicboxmsgid = dbslate.messageid;
                            slate.ver = ws.epicboxver;
                        } else {
                            collection.updateOne({ messageid: dbslate.messageid }, { $set: { made: true } })
                                .catch((err) => console.error("Error updateOne", err));
                        }

                        //TODO: check if this was already send on previous interval to client but client does block
                        // if client blocks, this will end in multible made requests
                        // we must set a flag here if the slate to client was already send but client did not process yet for any reasons.

                        ws.send(JSON.stringify(slate));
                        ws.process_slate = true;
                        console.log("Sent slate to", ws.epicPublicAddress);
                        config.debugMessage ? console.log(slate) : null;

                    } else {

                        // no slate found but subscribe was ok
                        ws.send(JSON.stringify({type:"Ok"}));

                    }
                    // end if result > 0

                }).catch((err) => {
                    console.error("Error reading pending slates", err);
                    ws.send(JSON.stringify({type:"Ok"}));
                });
            } else {
                // send back some response
                ws.sendslate_attempts++;
                ws.send(JSON.stringify({type:"Ok"}));
            }

        } else {
            // client cannot prove that he is the owner of the public address
            removeListenerMapping(ws);
            ws.send(JSON.stringify({type: "Error", kind: "InvalidSignature", description: "Invalid signature."}));
        }
    });
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
        // validate expected string fields before use
        if (typeof message.from !== 'string' || typeof message.to !== 'string' || typeof message.str !== 'string') {
            return ws.send(JSON.stringify({type: "Error", kind: "InvalidRequest", description: "Missing fields."}));
        }

        console.log("postslate from ", message.from, "to ", message.to);

        let publickey = message.from.split('@');
        publickey = publickey[0];

        // use epicboxlib to verify address format
        epicboxlib(['verifyaddress', message.from, message.to], (addressOk) => {

            if (addressOk) {

                // verify that the message we receive was signed from publickey
                epicboxlib(["verifysignature", publickey, message.str, message.signature], (signatureOk) => {

                    if (signatureOk) {

                        if (config.stats) {
                            statistics.slatesReceivedInHour++;
                        }

                        postSlate(ws, message);

                    } else {
                        console.log("Error postslate signature", publickey);
                        ws.send(JSON.stringify({type: "Error", kind: "InvalidRequest", description: "Error postslate signature."}));
                    }

                });

            } else {
                console.log("Error validate address format", message.from, message.to);

                ws.send(JSON.stringify({type:"Error", kind:"InvalidRequest", description: `Wrong address format. From: ${message.from}, To: ${message.to}`}));

            }
        });

    } catch (err) {
        console.error("Error postslate", err);
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
 participant cancels a queued tx by its slate/message id.
 The relay cannot look inside encrypted slatepack payloads, so the
 addressable handle is the epicbox message id (the same id clients receive
 as `epicboxmsgid` with every slate). Stateless with respect to slatepack
 negotiation steps: we only clear the queued slate from the DB.

 The requester must also be a participant of the tx (recipient queue or
 sender replyto); otherwise we answer a generic Ok so canceltx cannot be
 used to probe which message ids exist on the relay..

 @param {object} ws  - Client socket
 @param {json} message - { type: "canceltx", slateid: "<32-char epicboxmsgid>", signature: "<sig over id>" }
                         (epicboxmsgid is accepted as an alias for slateid;
                          wallet slate UUIDs are rejected - the relay cannot
                          resolve them)
*/
const canceltx = (ws, message) => {

    // must have proven address ownership in this session first
    if (ws.epicPublicAddress == null) {
        return ws.send(JSON.stringify({type: "Error", kind: "InvalidRequest", description: "Subscribe before canceltx."}));
    }

    // accept slateid (preferred) or epicboxmsgid for symmetry with 'made'
    const slateid = typeof message.slateid === 'string' ? message.slateid
                  : (typeof message.epicboxmsgid === 'string' ? message.epicboxmsgid : null);

    // epicbox message ids are uid(32) and exactly 32 alphanumeric chars.
    // Wallet slate UUIDs are a different identifier the relay cannot
    // resolve or track due to encryption. Other formats are rejected
    if (slateid === null || !/^[A-Za-z0-9]{32}$/.test(slateid)) {
        return ws.send(JSON.stringify({type: "Error", kind: "InvalidRequest", description: "Invalid id: expected the 32-char epicboxmsgid, not the wallet slate UUID."}));
    }

    if (typeof message.signature !== 'string') {
        return ws.send(JSON.stringify({type: "Error", kind: "InvalidRequest", description: "Missing signature."}));
    }

    // verify signature
    epicboxlib(["verifysignature", ws.epicPublicAddress, slateid, message.signature], (verified) => {

        if (!verified) {
            return ws.send(JSON.stringify({type: "Error", kind: "InvalidSignature", description: "Invalid signature."}));
        }

        // fetch first, then authorize in JS: the requester must be a
        // participant of the tx. queue holds the recipient public key,
        // replyto holds the sender address (publickey or publickey@domain).
        collection.findOne({ messageid: slateid }).then((dbslate) => {

            const isParticipant = dbslate != null && (
                dbslate.queue === ws.epicPublicAddress ||
                dbslate.replyto === ws.epicPublicAddress ||
                (typeof dbslate.replyto === 'string' && dbslate.replyto.split('@')[0] === ws.epicPublicAddress)
            );

            if (!isParticipant) {
                // identical response whether the id is unknown or belongs to
                // someone else, avoiding existence probing
                console.log("canceltx: no matching slate for", ws.epicPublicAddress, slateid);
                return ws.send(JSON.stringify({type: "Ok"}));
            }

            collection.deleteOne({ _id: dbslate._id }).then((delResult) => {
                console.log("canceltx: deleted", slateid, "for", ws.epicPublicAddress);
                config.debugMessage ? console.log("DB delete result", delResult) : null;

                // if this socket was blocked waiting on that very slate,
                // unblock it so the next challenge->subscribe cycle can
                // deliver the next queued slate
                ws.process_slate = false;
                ws.sendslate_attempts = 0;

                ws.send(JSON.stringify({type: "Ok"}));
            }).catch((err) => {
                console.error("Error canceltx delete", err);
                ws.send(JSON.stringify({type: "Error", kind: "InvalidRequest", description: "Cancel failed, try again."}));
            });

        }).catch((err) => {
            console.error("Error canceltx lookup", err);
            ws.send(JSON.stringify({type: "Error", kind: "InvalidRequest", description: "Cancel failed, try again."}));
        });
    });
}


/*
 store tx in db or forward to foreign epicbox
 if domain does not match our epicbox domain
 @param {object} ws  - Client socket
 @param {json} message - Client message see epic wallet
*/
const postSlate = (ws, json) => {

    let str = {};
    try {
        str = JSON.parse(json.str);
    } catch (err) {
        console.log("Error parsing message string", err);
        return;
    }

    // guard destination shape before access of property
    if (!str || typeof str !== 'object' || !str.destination || typeof str.destination.public_key !== 'string' || typeof str.destination.domain !== 'string') {
        return ws.send(JSON.stringify({type: "Error", kind: "InvalidRequest", description: "Invalid slate destination."}));
    }

    let addressto = {};
    addressto.publicKey = str.destination.public_key;
    addressto.domain = str.destination.domain;
    addressto.port = str.destination.port != null ? str.destination.port : 443;

    if (
            addressto.domain === config.epicbox_domain &&
            String(addressto.port) === String(config.epicbox_port)
    ) {

        // challenge is not required, we keep it for backward compatibility
        let signed_payload = JSON.stringify({str: json.str, challenge: "", signature: json.signature});
        let messageid = uid(32);
        collection.insertOne({
            queue: addressto.publicKey,
            made: false,
            payload: Buffer.from(signed_payload),
            replyto: json.from,
            createdat: new Date(),
            expiration: 86400000,
            messageid: messageid
        }).catch((err) => {
            console.error("Error insert to db", err);
        });

        let receiver = clients_publicaddress[addressto.publicKey];
        if (receiver != undefined && receiver.process_slate == false && receiver.readyState === WebSocket.OPEN) {
            if (config.stats) {
                statistics.slatesAttempt++;
            }
            let slate = {
                type: "Slate",
                from: json.from,
                str: json.str,
                signature: json.signature,
                challenge: "",
            };
            if (receiver.epicboxver == "2.0.0" || receiver.epicboxver == "3.0.0") {
                slate.epicboxmsgid = messageid;
                slate.ver = receiver.epicboxver;
            } else {
                collection.updateOne({ messageid: messageid }, { $set: { made: true } })
                    .catch((err) => console.error("Error updateOne", err));
            }
            // include the message id so the sender can canceltx later;
            // older wallets ignore unknown fields on Ok
            ws.send(JSON.stringify({type:"Ok", epicboxmsgid: messageid}));
            receiver.send(JSON.stringify(slate));
            receiver.process_slate = true;
            console.log("Passthrough slate to", receiver.epicPublicAddress);
            config.debugMessage ? console.log(slate) : null;
        } else {
            // receiver offline: slate stays queued in db. Return the
            // message id so the sender can canceltx later; older wallets
            // ignore unknown fields on Ok
            ws.send(JSON.stringify({type:"Ok", epicboxmsgid: messageid}));
        }
        return; // Do not relay externally
    }

    // Only relay to foreign epicbox domains

    // declare sock and message locally w/ handshake timeout, explicit close after relay complete
    const sock = new WebSocket("wss://" + addressto.domain + ":" + addressto.port, {
        handshakeTimeout: 10000,
        maxPayload: config.ws_max_payload
    });
    sock.on('error', (err) => {
        console.error("Relay socket error", addressto.domain, err.message);
    });
    sock.on('open', () => {
        console.log("Connect " + addressto.domain + ":" + addressto.port);
    });
    sock.on('message', (data) => {
        try {
            const message = JSON.parse(data);
            if (message.type === "Challenge") {
                let slate = {type: "PostSlate", from: json.from, to: json.to, str: json.str, signature: json.signature};
                sock.send(JSON.stringify(slate));
            }
            if (message.type === "Ok") {
                if (config.stats) {
                    statistics.slatesRelayedInHour++;
                }
                console.log("Sent to wss://" + addressto.domain + ":" + addressto.port);
                ws.send(JSON.stringify({type:"Ok"}));
                sock.close(1000, "Relay complete.");
            }
        } catch (err) {
            console.error("Error forward slate to foreign epicbox", err);
            ws.send(JSON.stringify({type: "Error", kind: "InvalidRequest", description: `Error forward slate to foreign epicbox. ToDomain: ${addressto.domain}:${addressto.port}, err: ${err}`}));
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

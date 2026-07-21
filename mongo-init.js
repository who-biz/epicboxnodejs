// Runs once, on first initialisation of an empty /data/db volume
// Executed by mongosh, so process.env is available

// password comes from .env
const pwd = process.env.EPICBOX_DB_PASSWORD;
if (!pwd) {
  throw new Error("EPICBOX_DB_PASSWORD is not set - refusing to create the epicbox user.");
}

db = db.getSiblingDB('epicbox');

db.createUser({
  user: "epicbox",
  pwd: pwd,
  roles: [
    { role: "readWrite", db: "epicbox" }
  ]
});


db.slates.createIndex({queue: 1, made: 1, createdat: 1});
db.slates.createIndex({messageid: 1, made: 1});
db.slates.createIndex({epicboxtxid: 1, made: 1});
db.slates.createIndex({route: 1, epicboxtxid: 1});
db.slates.createIndex(
    {createdat: 1},
    {expireAfterSeconds: 604800}
);

db.cancelled_slates.createIndex(
    {epicboxtxid: 1},
    {
        unique: true,
        // Existing tombstones may not have epicboxtxid yet.
        partialFilterExpression: {
            epicboxtxid: {$type: "string"}
        }
    }
);

db.cancelled_slates.createIndex({
    participants: 1,
    cancelledat: 1
});

db.cancelled_slates.createIndex(
    {cancelledat: 1},
    {expireAfterSeconds: 604800}
);


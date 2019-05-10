const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
const crypto = require('crypto');
const Bluebird = require('bluebird');

// Connection URL
const dbName = 'wwww';
const url = `mongodb://xxx:oooo@zzzz.documents.azure.com:10255/${dbName}?ssl=true&replicaSet=globaldb`;
const insert_count = 500;
const random_bytes_length = 500;

const insert = true;
const update = true;
const remove = true;

// Database Name

// Create a new MongoClient
const client = new MongoClient(url);
let db;

function insertCat(i) {
  return db.collection('cats').insertOne({ name: `Cat ${i}`, hash: crypto.randomBytes(random_bytes_length), shard_key: 'cat' });
}

function insertCats() {
  console.log('inserting cats');
  const inserts = [...Array(insert_count)].map((x, i) => () => insertCat(i));
  return Bluebird.map(inserts, (fn) => fn(), { concurrency: 10 });
}

function updateCats() {
  console.log('Updating cats');
  const updates = [...Array(insert_count)].map(() => () =>
    db.collection('cats').updateOne({ name: 'Cat 1', shard_key: 'cat' }, { $set: { hash: crypto.randomBytes(random_bytes_length) } }));
  return Bluebird.map(updates, fn => fn(), { concurrency: 10 });
}

function findCats() {
  console.log('finding cats');
  return db.collection('cats').find({ shard_key: 'cat' }).toArray().then(results => {
    console.log(results.length);
  }).catch(err => {
    console.log(err.stack);
  });
}

function removeCats() {
  return db.collection('cats').deleteMany({ shard_key: 'cat' });
}


// Use connect method to connect to the Server
client.connect(function(err) {
  assert.equal(null, err);
  console.log("Connected successfully to server");

  db = client.db(dbName);
  Promise.resolve()
    .then(() => insert ? insertCats() : null)
    .then(() => update ? updateCats() : null)
    .then(() => findCats())
    .then(() => remove ? removeCats() : null)
    .catch(err => {
      console.log(err.stack);
    })
    .then(() => process.exit());
});

const { MongoClient } = require('mongodb');

// Connection URI
const uri = 'mongodb://localhost:27017/s3-test';

// Create a new MongoClient
const client = new MongoClient(uri);

// Connect to the MongoDB cluster
async function connectDB() {
    try {
        // Connect the client to the server
        await client.connect();
        console.log(':===============MongoDB connected=================:');
        return client.db(); // Return the connected database object
    } catch (err) {
        console.error('Error connecting to MongoDB:', err);
        throw err;
    }
}

module.exports = { connectDB };
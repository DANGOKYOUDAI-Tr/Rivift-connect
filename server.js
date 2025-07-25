const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const cors = require('cors');
const { MongoClient } = require('mongodb');

const app = express();
app.use(cors());
app.use(express.json({limit: '50mb'}));
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*", methods: ["GET", "POST"] } });
const PORT = process.env.PORT || 3001;

const MONGO_URI = process.env.MONGO_URI;
let db;
let usersCollection;
let chatsCollection;

// ★★★ ここからが修正部分！ ★★★
async function connectToDatabase() {
    if (!MONGO_URI) {
        console.error("MONGO_URI not found in environment variables. Server cannot start.");
        process.exit(1); // MONGO_URIがなければサーバーを起動しない
    }
    try {
        const client = new MongoClient(MONGO_URI);
        await client.connect();
        db = client.db("rivift-connect-db");
        usersCollection = db.collection("users");
        chatsCollection = db.collection("chats");
        console.log('Successfully connected to MongoDB Atlas');
    } catch (e) {
        console.error("Failed to connect to MongoDB Atlas", e);
        process.exit(1); // 接続に失敗した場合もサーバーを起動しない
    }
}
// ★★★ ここまでが修正部分！ ★★★


let onlineUsers = {};

app.get('/', (req, res) => {
    res.send('<h1>Rivift Connect Server v4.3 Final Fix 2 is Active!</h1>');
});

// ... (ここから下のAPI部分は、前回のコードと全く同じ)
app.post('/createUser', async (req, res) => {
    try {
        const { email, displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon } = req.body;
        const existingUser = await usersCollection.findOne({ _id: email });
        if (existingUser) {
            return res.status(400).json({ error: 'User already exists' });
        }
        await usersCollection.insertOne({
            _id: email, displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon, 
            friends: [], requests: [], sentRequests: []
        });
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: 'Server error during user creation.' });
    }
});

app.post('/getUserData', async (req, res) => {
    try {
        const { email } = req.body;
        const userData = await usersCollection.findOne({ _id: email });
        res.json({ userData });
    } catch (error) {
        res.status(500).json({ error: 'Server error while fetching user data.' });
    }
});

app.post('/getUsersData', async (req, res) => {
    try {
        const { emails } = req.body;
        const usersData = {};
        if (emails && emails.length > 0) {
            const users = await usersCollection.find({ _id: { $in: emails } }).toArray();
            users.forEach(user => {
                usersData[user._id] = {
                    displayName: user.displayName,
                    icon: user.icon
                };
            });
        }
        res.json({ usersData });
    } catch (error) {
        res.status(500).json({ error: 'Server error while fetching multiple users data.' });
    }
});

app.post('/getMessageHistory', async (req, res) => {
    try {
        const { user1, user2 } = req.body;
        const chatID = [user1, user2].sort().join('__');
        let chat = await chatsCollection.findOne({ _id: chatID });
        const history = chat ? chat.messages : [];
        res.json({ history });
    } catch (error) {
        res.status(500).json({ error: 'Server error while fetching message history.' });
    }
});

io.on('connection', (socket) => {
    socket.on('login', ({ email }) => {
        onlineUsers[email] = socket.id;
        socket.email = email;
    });

    socket.on('send_friend_request', async ({ from, to }) => {
        await usersCollection.updateOne({ _id: to }, { $addToSet: { requests: from } });
        await usersCollection.updateOne({ _id: from }, { $addToSet: { sentRequests: to } });
        const recipientSocketId = onlineUsers[to];
        if (recipientSocketId) io.to(recipientSocketId).emit('db_updated_notification');
        io.to(socket.id).emit('db_updated_notification');
    });

    socket.on('accept_friend_request', async ({ from, to }) => {
        await usersCollection.updateOne({ _id: from }, { $pull: { requests: to }, $addToSet: { friends: to } });
        await usersCollection.updateOne({ _id: to }, { $pull: { sentRequests: from }, $addToSet: { friends: from } });
        const recipientSocketId = onlineUsers[to];
        if (recipientSocketId) io.to(recipientSocketId).emit('db_updated_notification');
        io.to(socket.id).emit('db_updated_notification');
    });
    
    socket.on('reject_friend_request', async ({ from, to }) => {
        await usersCollection.updateOne({ _id: from }, { $pull: { requests: to } });
        await usersCollection.updateOne({ _id: to }, { $pull: { sentRequests: from } });
        const recipientSocketId = onlineUsers[to];
        if (recipientSocketId) io.to(recipientSocketId).emit('db_updated_notification');
        io.to(socket.id).emit('db_updated_notification');
    });

    socket.on('cancel_friend_request', async ({ from, to }) => {
        await usersCollection.updateOne({ _id: from }, { $pull: { sentRequests: to } });
        await usersCollection.updateOne({ _id: to }, { $pull: { requests: from } });
        const recipientSocketId = onlineUsers[to];
        if (recipientSocketId) io.to(recipientSocketId).emit('db_updated_notification');
        io.to(socket.id).emit('db_updated_notification');
    });

    socket.on('private_message', async (payload) => {
        const chatID = [payload.from, payload.to].sort().join('__');
        await chatsCollection.updateOne(
            { _id: chatID },
            { $push: { messages: payload }, $setOnInsert: { users: [payload.from, payload.to] } },
            { upsert: true }
        );
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('private_message', payload);
    });

    socket.on('read_receipt', async (payload) => {
        const chatID = [payload.from, payload.to].sort().join('__');
        await chatsCollection.updateMany(
            { _id: chatID, "messages.to": payload.from, "messages.from": payload.to },
            { $set: { "messages.$[elem].read": true } },
            { arrayFilters: [ { "elem.to": payload.from, "elem.from": payload.to } ] }
        );
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('read_receipt', { from: payload.from });
    });

    socket.on('update_profile', async ({email, newDisplayName, newIcon}) => {
        const updateData = {};
        if (newDisplayName) updateData.displayName = newDisplayName;
        if (newIcon) updateData.icon = newIcon;
        const result = await usersCollection.updateOne({ _id: email }, { $set: updateData });
        if (result.modifiedCount > 0) {
            const user = await usersCollection.findOne({_id: email});
            const usersToNotify = [email, ...user.friends];
            usersToNotify.forEach(userEmail => {
                const userSocketId = onlineUsers[userEmail];
                if(userSocketId) io.to(userSocketId).emit('db_updated_notification');
            });
        }
    });

    socket.on('disconnect', () => {
        if (socket.email) delete onlineUsers[socket.email];
    });
});


// ★★★ ここからが修正部分！ ★★★
// サーバーを起動する前に、必ずDB接続を完了させる
connectToDatabase().then(() => {
    server.listen(PORT, () => console.log(`Server is running on port ${PORT}`));
}).catch(err => {
    console.error("Failed to start server:", err);
});
// ★★★ ここまでが修正部分！ ★★★
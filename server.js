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
let usersCollection;
let chatsCollection;

(async () => {
    if (!MONGO_URI) { console.error("MONGO_URI not found."); process.exit(1); }
    try {
        const client = new MongoClient(MONGO_URI);
        await client.connect();
        const db = client.db("rivift-connect-db");
        usersCollection = db.collection("users");
        chatsCollection = db.collection("chats");
        console.log('Connected to MongoDB Atlas');
    } catch (e) {
        console.error("Failed to connect to MongoDB Atlas", e);
        process.exit(1);
    }
})();

let onlineUsers = {};

app.get('/', (req, res) => res.send('<h1>Rivift Connect Server v5.0 is Active!</h1>'));

app.post('/createUser', async (req, res) => {
    try {
        const { email, displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon } = req.body;
        if (await usersCollection.findOne({ _id: email })) {
            return res.status(400).json({ error: 'User already exists' });
        }
        await usersCollection.insertOne({ _id: email, displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon, friends: [], requests: [], sentRequests: [] });
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
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
                    icon: user.icon,
                    publicKeyJwk: user.publicKeyJwk 
                };
            });
        }
        res.json({ usersData });
    } catch (error) {
        res.status(500).json({ error: 'Server error while fetching multiple users data.' });
    }
});

app.post('/getSidebarData', async (req, res) => {
    try {
        const { email } = req.body;
        const user = await usersCollection.findOne({ _id: email });
        if (!user) return res.status(404).json({ error: 'User not found' });
        
        const contactEmails = [...user.friends, ...user.requests, ...user.sentRequests];
        const uniqueEmails = [...new Set(contactEmails)];
        
        const contactsData = {};
        if (uniqueEmails.length > 0) {
            const users = await usersCollection.find({ _id: { $in: uniqueEmails } }).toArray();
            users.forEach(u => {
                contactsData[u._id] = { displayName: u.displayName, icon: u.icon };
            });
        }
        
        const unreadCounts = {};
        if (user.friends.length > 0) {
            for (const friendEmail of user.friends) {
                const chatID = [email, friendEmail].sort().join('__');
                const count = await chatsCollection.countDocuments({ _id: chatID, "messages.to": email, "messages.read": false });
                unreadCounts[friendEmail] = count;
            }
        }
        
        res.json({
            friends: user.friends,
            requests: user.requests,
            sentRequests: user.sentRequests,
            contactsData,
            unreadCounts
        });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/getMessageHistory', async (req, res) => {
    try {
        const { user1, user2 } = req.body;
        const chatID = [user1, user2].sort().join('__');
        const chat = await chatsCollection.findOne({ _id: chatID });
        res.json({ history: chat ? chat.messages : [] });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

io.on('connection', (socket) => {
    socket.on('login', ({ email }) => {
        onlineUsers[email] = socket.id;
        socket.email = email;
    });

    const notifyUsers = (userEmails) => {
        userEmails.forEach(email => {
            const socketId = onlineUsers[email];
            if (socketId) io.to(socketId).emit('db_updated_notification');
        });
    };

    socket.on('send_friend_request', async ({ from, to }) => {
        await usersCollection.updateOne({ _id: to }, { $addToSet: { requests: from } });
        await usersCollection.updateOne({ _id: from }, { $addToSet: { sentRequests: to } });
        notifyUsers([from, to]);
    });

    socket.on('accept_friend_request', async ({ from, to }) => {
        await usersCollection.updateOne({ _id: from }, { $pull: { requests: to }, $addToSet: { friends: to } });
        await usersCollection.updateOne({ _id: to }, { $pull: { sentRequests: from }, $addToSet: { friends: from } });
        notifyUsers([from, to]);
    });
    
    socket.on('private_message', async (payload) => {
        const chatID = [payload.from, payload.to].sort().join('__');
        await chatsCollection.updateOne({ _id: chatID }, { $push: { messages: payload } }, { upsert: true });
        
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('private_message', payload);
        
        const senderSocketId = onlineUsers[payload.from];
        if (senderSocketId) io.to(senderSocketId).emit('db_updated_notification');
    });

    socket.on('read_receipt', async (payload) => {
        const chatID = [payload.from, payload.to].sort().join('__');
        await chatsCollection.updateMany(
            { _id: chatID },
            { $set: { "messages.$[elem].read": true } },
            { arrayFilters: [ { "elem.to": payload.from, "elem.from": payload.to } ] }
        );
        notifyUsers([payload.from, payload.to]);
    });

    socket.on('delete_message', async ({ from, to, messageId }) => {
        const chatID = [from, to].sort().join('__');
        await chatsCollection.updateOne(
            { _id: chatID, "messages.id": messageId, "messages.from": from },
            { $set: { 
                "messages.$.type": "deleted",
                "messages.$.content": "メッセージが削除されました",
            }}
        );
        notifyUsers([from, to]);
    });
    
    socket.on('delete_chat', async ({ user1, user2 }) => {
        const chatID = [user1, user2].sort().join('__');
        await chatsCollection.deleteOne({ _id: chatID });
        notifyUsers([user1, user2]);
    });
    
    socket.on('update_profile', async ({email, newDisplayName, newIcon}) => {
        const updateData = {};
        if (newDisplayName) updateData.displayName = newDisplayName;
        if (newIcon) updateData.icon = newIcon;
        const result = await usersCollection.findOneAndUpdate({ _id: email }, { $set: updateData }, { returnDocument: 'after' });
        if (result.value) {
            notifyUsers([email, ...result.value.friends]);
        }
    });

    socket.on('disconnect', () => {
        if (socket.email) delete onlineUsers[socket.email];
    });
});

server.listen(PORT, () => console.log(`Server is running on port ${PORT}`));
const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const cors = require('cors');
const { MongoClient } = require('mongodb');
const axios = require('axios');
const fetch = require('node-fetch');

const app = express();
app.use(cors());
app.use(express.json({limit: '50mb'}));

const server = http.createServer(app);
const io = new Server(server, { 
    cors: { origin: "*", methods: ["GET", "POST"] },
    maxHttpBufferSize: 50e6
});
const PORT = process.env.PORT || 3001;

const MONGO_URI = process.env.MONGO_URI;
let usersCollection;
let chatsCollection;

async function connectToDatabase() {
    if (!MONGO_URI) { console.error("MONGO_URI not found."); process.exit(1); }
    try {
        const client = new MongoClient(MONGO_URI);
        await client.connect();
        const db = client.db("rivift-connect-db");
        usersCollection = db.collection("users");
        chatsCollection = db.collection("chats");
        console.log('Successfully connected to MongoDB Atlas');
    } catch (e) {
        console.error("Failed to connect to MongoDB Atlas", e);
        process.exit(1);
    }
}

let onlineUsers = {};

app.get('/', (req, res) => res.send('<h1>Rivift Connect Server v4.1 is Active!</h1>'));

app.get('/get-ice-servers', async (req, res) => {
  try {
    const response = await axios.get("https://openrelay.metered.ca/api/v1/turn/credentials?apiKey=1543912a7e4e13e008639223b79119253438");
    const iceServers = response.data;
    
    console.log("Successfully fetched ICE servers from Open Relay Project:", iceServers);
    res.json(iceServers);

  } catch (error) {
    const errorDetails = error.response ? JSON.stringify(error.response.data) : error.message;
    console.error("Failed to get ICE servers from Open Relay:", errorDetails);
    res.status(500).json([
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun1.l.google.com:19302" },
    ]);
  }
});

app.post('/createUser', async (req, res) => {
    try {
        const { email, displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon } = req.body;
        const existingUser = await usersCollection.findOne({ _id: email });
        if (existingUser) {
            return res.status(400).json({ error: 'User already exists' });
        }
        await usersCollection.insertOne({ _id: email, displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon, friends: [], requests: [], sentRequests: [] });
        res.json({ success: true });
    } catch (error) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/getUserData', async (req, res) => {
    try {
        const { email } = req.body;
        const userData = await usersCollection.findOne({ _id: email });
        res.json({ userData });
    } catch (error) { res.status(500).json({ error: 'Server error' }); }
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
                const chat = await chatsCollection.findOne({ _id: chatID });
                if (chat) {
                    const count = chat.messages.filter(msg => msg.to === email && !msg.read).length;
                    unreadCounts[friendEmail] = count;
                } else {
                    unreadCounts[friendEmail] = 0;
                }
            }
        }
        
        res.json({
            friends: user.friends,
            requests: user.requests,
            sentRequests: user.sentRequests,
            contactsData,
            unreadCounts
        });
    } catch (e) { 
        console.error("getSidebarData error:", e);
        res.status(500).json({ error: 'Server error' }); 
    }
});

app.post('/getUsersData', async (req, res) => {
    try {
        const { emails } = req.body;
        const usersData = {};
        if (emails && emails.length > 0) {
            const users = await usersCollection.find({ _id: { $in: emails } }).toArray();
            users.forEach(user => {
                usersData[user._id] = { displayName: user.displayName, icon: user.icon, publicKeyJwk: user.publicKeyJwk };
            });
        }
        res.json({ usersData });
    } catch (error) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/getMessageHistory', async (req, res) => {
    try {
        const { user1, user2 } = req.body;
        const chatID = [user1, user2].sort().join('__');
        const history = await chatsCollection
            .find({ chatID: chatID })
            .sort({ timestamp: -1 })
            .limit(50)
            .toArray();
        res.json({ history: history.reverse() });

    } catch (error) {
        console.error("getMessageHistory error:", error);
        res.status(500).json({ error: 'Server error' });
    }
});

app.post('/saveMessage', async (req, res) => {
    try {
        const { user1, user2, message } = req.body;
        const chatID = [user1, user2].sort().join('__');
        await chatsCollection.updateOne(
            { _id: chatID },
            { $push: { messages: message }, $setOnInsert: { users: [user1, user2] } },
            { upsert: true }
        );
        res.json({ success: true });
    } catch (error) { res.status(500).json({ error: 'Server error' }); }
});

app.get('/search', async (req, res) => {
    const query = req.query.q;
    if (!query) {
        return res.status(400).json({ error: 'Query parameter "q" is required.' });
    }
    try {
        const response = await axios.get('https://api.duckduckgo.com/', {
            params: {
                q: query,
                format: 'json',
                no_html: 1,
                skip_disambig: 1
            },
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        });

        const results = {
            AbstractText: response.data.AbstractText || '',
            AbstractURL: response.data.AbstractURL || '',
            Image: response.data.Image || '',
            RelatedTopics: []
        };

        if (response.data.RelatedTopics && Array.isArray(response.data.RelatedTopics)) {
            response.data.RelatedTopics.forEach(topic => {
                if (topic.Result) {
                    results.RelatedTopics.push({
                        Result: topic.Result,
                        FirstURL: topic.FirstURL,
                        Text: topic.Text
                    });
                } else if (topic.Topics && Array.isArray(topic.Topics)) {
                    topic.Topics.forEach(subTopic => {
                        if (subTopic.Result) {
                            results.RelatedTopics.push({
                                Result: subTopic.Result,
                                FirstURL: subTopic.FirstURL,
                                Text: subTopic.Text
                            });
                        }
                    });
                }
            });
        }
        
        res.json(results);

    } catch (error) {
        console.error('DuckDuckGo API Error:', error.message);
        res.status(500).json({ error: 'Failed to fetch search results.' });
    }
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

    socket.on('delete_friend', async ({ from, to }) => {
        try {
            const fromUser = await usersCollection.findOne({ _id: from });
            const toUser = await usersCollection.findOne({ _id: to });
            if (fromUser && toUser) {
                await usersCollection.updateOne({ _id: from }, { $pull: { friends: to } });
                await usersCollection.updateOne({ _id: to }, { $pull: { friends: from } });
                const chatID = [from, to].sort().join('__');
                await chatsCollection.deleteOne({ _id: chatID });
                notifyUsers([from, to]);
            }
        } catch (e) {
            console.error("delete_friend error:", e);
        }
    });
    
    socket.on('private_message', async (payload) => {
        try {
            const { from, to, timestamp } = payload;
            const chatID = [from, to].sort().join('__');
            const messageToStore = {
                chatID: chatID,
                ...payload
            };
            await chatsCollection.insertOne(messageToStore);
            const recipientSocketId = onlineUsers[to];
            if (recipientSocketId) {
                io.to(recipientSocketId).emit('private_message', payload);
            }
            const senderSocketId = onlineUsers[from];
            if (senderSocketId) {
                io.to(senderSocketId).emit('db_updated_notification');
            }
        } catch(e) {
            console.error("private_message error:", e);
        }
    });

    socket.on('read_receipt', async (payload) => {
        try { 
            const chatID = [payload.from, payload.to].sort().join('__');
            await chatsCollection.updateMany(
                { chatID: chatID, to: payload.from },
                { $set: { read: true } }
            );
            notifyUsers([payload.from, payload.to]);
        } catch (e) {
            console.error("read_receipt error:", e);
        }
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

    socket.on('webrtc-offer', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('webrtc-offer', {
                from: socket.email,
                offer: payload.offer
            });
        }
    });

    socket.on('webrtc-answer', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('webrtc-answer', {
                from: socket.email,
                answer: payload.answer
            });
        }
    });

    socket.on('webrtc-ice-candidate', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('webrtc-ice-candidate', {
                from: socket.email,
                candidate: payload.candidate
            });
        }
    });

    socket.on('webrtc-end-call', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('webrtc-end-call', { from: socket.email });
        }
    });

    socket.on('webrtc-reject-call', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('webrtc-reject-call', { from: socket.email });
        }
    });

});

connectToDatabase().then(() => {
    server.listen(PORT, () => console.log(`Server is running on port ${PORT}`));
}).catch(err => {
    console.error("Failed to start server:", err);
});
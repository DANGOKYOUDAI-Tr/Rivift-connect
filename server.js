const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const cors = require('cors');
const redis = require('redis');

const app = express();
app.use(cors());
app.use(express.json({limit: '10mb'}));

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*", methods: ["GET", "POST"] } });
const PORT = process.env.PORT || 3001;

let redisClient;
(async () => {
    if (process.env.REDIS_URL) {
        redisClient = redis.createClient({ url: process.env.REDIS_URL });
        redisClient.on('error', (err) => console.log('Redis Client Error', err));
        await redisClient.connect();
        console.log('Connected to Redis');
    }
})();

async function getDB() {
    if (!redisClient) return { users: {}, chats: {} };
    const data = await redisClient.get('db');
    return data ? JSON.parse(data) : { users: {}, chats: {} };
}

async function saveDB(db) {
    if (!redisClient) return;
    await redisClient.set('db', JSON.stringify(db));
}

let onlineUsers = {};

app.get('/', (req, res) => {
    res.send('<h1>Rivift Connect Server v4.2 Final Fix is Active!</h1>');
});

app.post('/createUser', async (req, res) => {
    const db = await getDB();
    const { email, displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon } = req.body;
    if (!db.users) db.users = {};
    if (db.users[email]) {
        return res.status(400).json({ error: 'User already exists' });
    }
    db.users[email] = { displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon, friends: [], requests: [], sentRequests: [] };
    await saveDB(db);
    res.json({ success: true });
});

app.post('/getUserData', async (req, res) => {
    const db = await getDB();
    const { email } = req.body;
    const userData = db.users?.[email];
    if (userData) {
        res.json({ userData: {
            displayName: userData.displayName,
            icon: userData.icon,
            publicKeyJwk: userData.publicKeyJwk,
            friends: userData.friends,
            requests: userData.requests,
            sentRequests: userData.sentRequests,
            encryptedPrivateKeyPayload: userData.encryptedPrivateKeyPayload
        }});
    } else {
        res.json({ userData: null });
    }
});

app.post('/getUsersData', async (req, res) => {
    const db = await getDB();
    const { emails } = req.body;
    const usersData = {};
    if (emails) {
        emails.forEach(email => {
            if (db.users?.[email]) {
                usersData[email] = {
                    displayName: db.users[email].displayName,
                    icon: db.users[email].icon
                };
            }
        });
    }
    res.json({ usersData });
});

app.post('/getMessageHistory', async (req, res) => {
    const db = await getDB();
    const { user1, user2 } = req.body;
    const chatID = [user1, user2].sort().join('__');
    const history = db.chats?.[chatID] || [];
    res.json({ history });
});


io.on('connection', (socket) => {
    socket.on('login', ({ email }) => {
        onlineUsers[email] = socket.id;
        socket.email = email;
    });
    
    // ★★★ ここから下の全ての処理で、オンラインリストを更新するロジックを追加 ★★★

    socket.on('send_friend_request', async ({ from, to }) => {
        if(!onlineUsers[from]) onlineUsers[from] = socket.id; // 送信者をリストに追加
        const db = await getDB();
        if (db.users[to] && db.users[from] && !db.users[to].requests.includes(from)) {
            db.users[to].requests.push(from);
            db.users[from].sentRequests.push(to);
            await saveDB(db);
            
            const recipientSocketId = onlineUsers[to];
            if (recipientSocketId) io.to(recipientSocketId).emit('db_updated_notification', { from });
            io.to(socket.id).emit('db_updated_notification', { from });
        }
    });

    socket.on('accept_friend_request', async ({ from, to }) => {
        if(!onlineUsers[from]) onlineUsers[from] = socket.id;
        const db = await getDB();
        if (db.users[from] && db.users[to]) {
            db.users[from].requests = db.users[from].requests.filter(e => e !== to);
            if (!db.users[from].friends.includes(to)) db.users[from].friends.push(to);
            db.users[to].sentRequests = db.users[to].sentRequests.filter(e => e !== from);
            if (!db.users[to].friends.includes(from)) db.users[to].friends.push(from);
            await saveDB(db);
            
            const recipientSocketId = onlineUsers[to];
            if (recipientSocketId) io.to(recipientSocketId).emit('db_updated_notification', { from });
            io.to(socket.id).emit('db_updated_notification', { from });
        }
    });
    
    socket.on('private_message', async (payload) => {
        if(!onlineUsers[payload.from]) onlineUsers[payload.from] = socket.id;
        const db = await getDB();
        const chatID = [payload.from, payload.to].sort().join('__');
        if (!db.chats) db.chats = {};
        if (!db.chats[chatID]) db.chats[chatID] = [];
        db.chats[chatID].push(payload);
        await saveDB(db);

        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('private_message', payload);
        }
    });

    socket.on('read_receipt', async (payload) => {
        if(!onlineUsers[payload.from]) onlineUsers[payload.from] = socket.id;
        const db = await getDB();
        const chatID = [payload.from, payload.to].sort().join('__');
        if (db.chats?.[chatID]) {
            db.chats[chatID].forEach(msg => {
                if (msg.to === payload.from && msg.from === payload.to) msg.read = true;
            });
            await saveDB(db);
        }
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('read_receipt', { from: payload.from });
        }
    });

    socket.on('update_profile', async ({email, newDisplayName, newIcon}) => {
        if(!onlineUsers[email]) onlineUsers[email] = socket.id;
        const db = await getDB();
        if (db.users[email]) {
            if (newDisplayName) db.users[email].displayName = newDisplayName;
            if (newIcon) db.users[email].icon = newIcon;
            await saveDB(db);
            
            // 自分と友達全員に通知
            const usersToNotify = [email, ...db.users[email].friends];
            usersToNotify.forEach(userEmail => {
                const userSocketId = onlineUsers[userEmail];
                if(userSocketId) io.to(userSocketId).emit('db_updated_notification', { from: email });
            });
        }
    });

    socket.on('disconnect', () => {
        if (socket.email) {
            delete onlineUsers[socket.email];
        }
    });
});

server.listen(PORT, () => console.log(`Server is running on port ${PORT}`));
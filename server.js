const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json({limit: '10mb'}));

const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

const PORT = process.env.PORT || 3001;

let db = {
    users: {},
    chats: {}
};

let onlineUsers = {};

app.get('/', (req, res) => {
    res.send('<h1>Rivift Connect Server v4.0 Final Fix is Active!</h1>');
});

app.post('/createUser', (req, res) => {
    const { email, displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon } = req.body;
    if (db.users[email]) {
        return res.status(400).json({ error: 'User already exists' });
    }
    db.users[email] = { displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon, friends: [], requests: [], sentRequests: [] };
    res.json({ success: true });
});

app.post('/getUserData', (req, res) => {
    const { email } = req.body;
    const userData = db.users[email];
    if (userData) {
        res.json({ userData: {
            displayName: userData.displayName,
            icon: userData.icon,
            publicKeyJwk: userData.publicKeyJwk,
            friends: userData.friends,
            requests: userData.requests,
            sentRequests: userData.sentRequests
        }});
    } else {
        res.status(404).json({ error: 'User not found' });
    }
});

app.post('/getUsersData', (req, res) => {
    const { emails } = req.body;
    const usersData = {};
    emails.forEach(email => {
        if (db.users[email]) {
            usersData[email] = {
                displayName: db.users[email].displayName,
                icon: db.users[email].icon
            };
        }
    });
    res.json({ usersData });
});

io.on('connection', (socket) => {
    socket.on('login', (data) => {
        onlineUsers[data.email] = socket.id;
        socket.email = data.email;
    });

    socket.on('send_friend_request', ({ from, to }) => {
        if (db.users[to] && db.users[from] && !db.users[to].requests.includes(from)) {
            db.users[to].requests.push(from);
            db.users[from].sentRequests.push(to);
            const recipientSocketId = onlineUsers[to];
            if (recipientSocketId) {
                io.to(recipientSocketId).emit('db_updated_notification', { from });
            }
            const senderSocketId = onlineUsers[from];
             if (senderSocketId) {
                io.to(senderSocketId).emit('db_updated_notification', { from });
            }
        }
    });

    socket.on('accept_friend_request', ({ from, to }) => {
        if (db.users[from] && db.users[to]) {
            db.users[from].requests = db.users[from].requests.filter(e => e !== to);
            if (!db.users[from].friends.includes(to)) db.users[from].friends.push(to);
            db.users[to].sentRequests = db.users[to].sentRequests.filter(e => e !== from);
            if (!db.users[to].friends.includes(from)) db.users[to].friends.push(from);
            
            const fromSocketId = onlineUsers[from];
            if (fromSocketId) io.to(fromSocketId).emit('db_updated_notification', { from: to });
            const toSocketId = onlineUsers[to];
            if (toSocketId) io.to(toSocketId).emit('db_updated_notification', { from });
        }
    });

    socket.on('reject_friend_request', ({ from, to }) => {
        if (db.users[from] && db.users[to]) {
            db.users[from].requests = db.users[from].requests.filter(e => e !== to);
            db.users[to].sentRequests = db.users[to].sentRequests.filter(e => e !== from);
            const fromSocketId = onlineUsers[from];
            if (fromSocketId) io.to(fromSocketId).emit('db_updated_notification', { from: to });
            const toSocketId = onlineUsers[to];
            if (toSocketId) io.to(toSocketId).emit('db_updated_notification', { from });
        }
    });

    socket.on('cancel_friend_request', ({ from, to }) => {
        if (db.users[from] && db.users[to]) {
            db.users[from].sentRequests = db.users[from].sentRequests.filter(e => e !== to);
            db.users[to].requests = db.users[to].requests.filter(e => e !== from);
            const fromSocketId = onlineUsers[from];
            if (fromSocketId) io.to(fromSocketId).emit('db_updated_notification', { from: to });
            const toSocketId = onlineUsers[to];
            if (toSocketId) io.to(toSocketId).emit('db_updated_notification', { from });
        }
    });
    
    socket.on('private_message', (payload) => {
        const chatID = [payload.from, payload.to].sort().join('__');
        if (!db.chats[chatID]) db.chats[chatID] = [];
        db.chats[chatID].push(payload);

        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('private_message', payload);
        }
    });

    socket.on('read_receipt', (payload) => {
        const chatID = [payload.from, payload.to].sort().join('__');
        if (db.chats[chatID]) {
            db.chats[chatID].forEach(msg => {
                if (msg.to === payload.from) msg.read = true;
            });
        }
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('read_receipt', { from: payload.from });
        }
    });

    socket.on('update_profile', ({email, newDisplayName, newIcon}) => {
        if (db.users[email]) {
            if (newDisplayName) db.users[email].displayName = newDisplayName;
            if (newIcon) db.users[email].icon = newIcon;
            
            db.users[email].friends.forEach(friendEmail => {
                const friendSocketId = onlineUsers[friendEmail];
                if (friendSocketId) {
                    io.to(friendSocketId).emit('db_updated_notification', { from: email });
                }
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
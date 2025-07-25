const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json({limit: '10mb'}));

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*", methods: ["GET", "POST"] } });
const PORT = process.env.PORT || 3001;

// ★★★ 修正点1：DBの構造をシンプルにし、オンラインユーザーの管理を徹底する ★★★
let db = {
    users: {}, // { email: { displayName, ... } }
    chats: {}  // { chatID: [ message, ... ] }
};
let onlineUsers = {}; // { email: socketId }

app.get('/', (req, res) => {
    res.send('<h1>Rivift Connect Server v4.1 is Active!</h1>');
});

// アカウント作成API
app.post('/createUser', (req, res) => {
    const { email, displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon } = req.body;
    if (db.users[email]) {
        return res.status(400).json({ error: 'User already exists' });
    }
    db.users[email] = { displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon, friends: [], requests: [], sentRequests: [] };
    res.json({ success: true });
});

// ユーザー情報取得API (ログイン時に使用)
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
            sentRequests: userData.sentRequests,
            encryptedPrivateKeyPayload: userData.encryptedPrivateKeyPayload
        }});
    } else {
        res.json({ userData: null });
    }
});

// 複数ユーザーの情報取得API (サイドバー表示用)
app.post('/getUsersData', (req, res) => {
    const { emails } = req.body;
    const usersData = {};
    if (emails) {
        emails.forEach(email => {
            if (db.users[email]) {
                usersData[email] = {
                    displayName: db.users[email].displayName,
                    icon: db.users[email].icon
                };
            }
        });
    }
    res.json({ usersData });
});

// チャット履歴取得API
app.post('/getMessageHistory', (req, res) => {
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
    
    // ... (ここから下のsocket.onの処理は、全てサーバー内部でDBを直接操作するように修正)

    socket.on('disconnect', () => {
        if (socket.email) {
            delete onlineUsers[socket.email];
        }
    });
});

server.listen(PORT, () => console.log(`Server is running on port ${PORT}`));
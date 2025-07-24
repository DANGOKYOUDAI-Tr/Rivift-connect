const express = require('express');
const http = require('http');
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*", methods: ["GET", "POST"] } });
const PORT = process.env.PORT || 3000;

// ★★★ サーバー内に、全ユーザー情報を記憶する「中央の電話帳」を用意する ★★★
let db = {
    users: {}, // { email: { displayName, icon, publicKeyJwk, friends:[], requests:[], sentRequests:[] } }
    sockets: {} // { email: socketId }
};

app.get('/', (req, res) => res.send('<h1>Rivift Connect Server v4.0 is Active!</h1>'));

io.on('connection', (socket) => {
    console.log('ユーザーが接続:', socket.id);

    // ログイン時に、そのユーザーのemailとsocket.idを紐付ける
    socket.on('login', (data) => {
        console.log(`ログイン: ${data.email} as ${socket.id}`);
        db.sockets[data.email] = socket.id;
        socket.email = data.email; // socket自体にもemailを持たせる
    });

    // 友達申請を中継する
    socket.on('friend_request', (payload) => {
        const recipientSocketId = db.sockets[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('friend_request_received', payload);
        }
    });

    // 申請承認を中継する
    socket.on('request_accepted', (payload) => {
        const recipientSocketId = db.sockets[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('request_accepted_notification', payload);
        }
    });
    
    // ★★★ サーバーがメッセージを中継する処理 ★★★
    socket.on('private_message', (payload) => {
        const recipientSocketId = db.sockets[payload.to];
        if (recipientSocketId) {
            // 送信者と、暗号化されたメッセージをそのまま相手に送る
            io.to(recipientSocketId).emit('private_message', {
                from: payload.from,
                encryptedBody: payload.encryptedBody,
                timestamp: payload.timestamp
            });
        }
    });

    // 既読情報を中継する
    socket.on('read_receipt', (payload) => {
        const recipientSocketId = db.sockets[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('read_receipt', { from: payload.from });
        }
    });

    // 接続が切れたら、電話帳から削除
    socket.on('disconnect', () => {
        console.log('ユーザーが切断:', socket.id);
        if (socket.email) {
            delete db.sockets[socket.email];
        }
    });
});

server.listen(PORT, () => console.log(`Server is running on port ${PORT}`));
// server.js

// 必要な部品をインポートする
const express = require('express');
const http = require('http');
const { Server } = require("socket.io");

// Expressアプリを作成
const app = express();
const server = http.createServer(app);

// Socket.IOサーバーを作成
const io = new Server(server, {
    cors: {
        origin: "*", // どのWebページからの接続も許可する（テスト用）
        methods: ["GET", "POST"]
    }
});

// サーバーが接続を待ち受ける「ポート」番号を決める
// Renderみたいなサービスは自動でポートを決めてくれるので、それに従う
const PORT = process.env.PORT || 3000;

// 「/」にアクセスがあったら、「Hello, Rivift Connect Server!」と表示する
// これで、サーバーがちゃんと動いてるか確認できる
app.get('/', (req, res) => {
    res.send('<h1>Hello, Rivift Connect Server!</h1>');
});

// 誰かがサーバーに接続してきた時の処理
io.on('connection', (socket) => {
    console.log('a user connected:', socket.id);

    // クライアント（君のHTML）から 'private_message' という伝言が来たら
    socket.on('private_message', (payload) => {
        // そのメッセージを、指定された相手にだけ送り返す
        // to: 相手のsocket.id, payload: メッセージ本体
        io.to(payload.to).emit('private_message', payload);
    });

    // 誰かが接続を切った時の処理
    socket.on('disconnect', () => {
        console.log('user disconnected:', socket.id);
    });
});

// サーバーを起動する
server.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
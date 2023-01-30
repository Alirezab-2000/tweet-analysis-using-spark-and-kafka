var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);
var kafka = require('kafka-node');
var port = 8081;

let client = new kafka.KafkaClient("localhost:9092");

consumer = new kafka.Consumer(
<<<<<<< HEAD
    client, [{ topic: 't1', partitions: 0 }, { topic: 'emotion', partitions: 1 }, { topic: 'classification', partitions: 2 }], { autoCommit: false });
=======
    client, [{ topic: 't1', partitions: 0 }, { topic: 'emotion', partitions: 1 }], { autoCommit: false });
>>>>>>> d2d5765 (clean code)

// classification_consumer = new kafka.Consumer(
//     client, [{ topic: 'classification', partition: 0 }], { groupId: 'group2' }, { autoCommit: false });

// emotion_consumer = new kafka.Consumer(
//     client, [{ topic: 'emotion', partition: 0 }], { groupId: 'group3' }, { autoCommit: false });

app.get('/', function (req, res) {
    res.sendfile('index.html');
});

io = io.on('connection', function (socket) {
    console.log('a user connected');
    socket.on('disconnect', function () {
        console.log('user disconnected');
    });
});


consumer.on('message', function (message) {

    split_str = message.value.split("@@")
    first_part = split_str[0]
    info = split_str[1]

    if(!info) return

    info = info.slice(0, -1)
    info = info.replaceAll("'", "\"")

    if (first_part == "\"emotion_result") {
        console.log(111, first_part , info)
        io.emit('emotion', info)
    } else if(first_part == "\"hashtag_result") {
        console.log(222, first_part , info)
        io.emit('hashtag', info)
<<<<<<< HEAD
    }else if(first_part == "\"classification_result") {
        console.log(222, first_part , info)
        io.emit('classification', info)
=======
>>>>>>> d2d5765 (clean code)
    }

});



http.listen(port, function () {
    console.log("Running on port " + port)
});
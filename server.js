/**
 * This page contains server script using express and socket.io
 * Log using winston. 
 * 
 * @project name: Adres
 * @module: bidding
 * @created on: July 26, 2021
 * @author: Gautam
 */

require('dotenv').config();
global.config = {
    SOCKET_PORT: process.env.SOCKET_PORT,
    DOMAIN_BASE: process.env.BASE_IP,
    SITE_BASE: process.env.PROTOCOL + process.env.DOMAIN,
    EMAIL_FROM: process.env.SITE_NAME + '<' + process.env.EMAIL_FROM + '>',
    EMAIL_HOST: process.env.EMAIL_HOST,
    EMAIL_PORT: process.env.EMAIL_PORT,
    EMAIL_USERNAME: process.env.EMAIL_USERNAME,
    EMAIL_PASSWORD: process.env.EMAIL_PASSWORD,
    DB_HOST: process.env.DATABASE_HOST,
    DB_NAME: process.env.DATABASE_NAME,
    DB_USER: process.env.DATABASE_USERNAME,
    DB_PASS: process.env.DATABASE_PASSWORD,
    TIMEZONE: process.env.TIMEZONE,
    DEFAULT_TIMEZONE: process.env.DEFAULT_TIMEZONE,
    API_URL: process.env.API_URL,
    API_TOKEN: process.env.API_TOKEN,
    API_PORT: process.env.API_PORT,
    API_PROTOCOL: process.env.API_PROTOCOL,
    AUTH_TOKEN: process.env.AUTH_TOKEN
};

const fs = require('fs'); 
const http = require('http'); 
const https = require('https');
const url = require('url');
const redis = require('socket.io-redis');
const cluster = require('cluster');
const numCPUs = require('os').cpus().length;
const bid = require('./app/bid');
const insider = require('./app/insider');
const notifications = require('./app/notifications');
const chat = require('./app/chat');
const logger = require('./app/logger');
const decryptUserId = require('./app/common');
var app = require('express')();

app.get('/', function(req, res) {
   res.sendfile('index.html');
});

/**
 * Setup server culester 
 */
if (cluster.isMaster) {
	console.log('Master Process ' + process.pid + ' is running.');
    for (let i = 0; i < numCPUs; i++) {
        cluster.fork({ IS_PRIMARY_WORKER: i === 0 ? 'true' : 'false' });
    }
    cluster.on('online', (worker) => {
        console.log('Worker Process ' + worker.process.pid + ' is online and listening.');
    });
    cluster.on('exit', (worker, code, signal) => {
        if (signal) {
            console.log('Worker Process ' + worker.process.pid + ' was killed by signal: ' + signal + '.');
            cluster.fork({ IS_PRIMARY_WORKER: 'false' });
        } else if (code) {
            console.log('Worker Process ' + worker.process.pid + ' has died with code: ' + code + '.');
            cluster.fork({ IS_PRIMARY_WORKER: 'false' });
        }
    });

}
else{
	var server = new https.createServer(
        {
		'key' : fs.readFileSync( process.env.CA_KEY_FILE_PATH ),
		'cert': fs.readFileSync( process.env.CA_CERT_FILE_PATH ),
		'requestCert': false,
		'rejectUnauthorized': false
		},
        function (request, response) {
        const parsedUrl = url.parse(request.url, true);

        // Handle /connection route
        if (parsedUrl.pathname === '/is-running' && request.method === 'GET') {
            console.log("Received connection request");

            // Send a simple response
            response.writeHead(200, { 'Content-Type': 'application/json' });
            response.end(JSON.stringify({ message: 'Server is running successfully!' }));

        } else {
            // If the route is not /connection, send 404
            response.writeHead(404, { 'Content-Type': 'text/plain' });
            response.end('Not Found');
        }
		console.log("server is running at "+global.config.SOCKET_PORT)
    }).listen(global.config.SOCKET_PORT);

     // ✅ Graceful Shutdown
     function shutdown() {
        const pool = require('../connection');
        logger.info('Gracefully shutting down...');
        server.close(() => {
        logger.info('HTTP server closed');
        pool.end((err) => {
            if (err) logger.error('Error closing DB pool:', err);
            else logger.info('DB pool closed successfully');
            process.exit(0);
        });
        });
    }
    
    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

    process.on('uncaughtException', (err) => {
        logger.error('Uncaught Exception Server.js:', err.stack || err);
      });
      
      process.on('unhandledRejection', (reason, promise) => {
        logger.error('Unhandled Rejection Server.js:', reason);
      });
    }


    if (process.env.IS_PRIMARY_WORKER === 'true') {
        setInterval(() => {
            try {
                bid.checkAuction(null, { domain_id: 3 });
            } catch (err) {
                logger.error('Error in bid.checkAuction Server.js :', err);
            }
        }, 15000)
        
        setInterval(() => {
            try {
                bid.auctionStartMail(null, {domain_id:3});
            } catch (err) {
                logger.error('Error in bid.checkAuction Server.js :', err);
            }
        }, 900000)
    }

    function wrapSocketHandler(socket, handler) {
        return (data) => {
            if (data?.user_id) {
                const user_id = decryptUserId(data.user_id);
                data = { ...data, user_id: parseInt(user_id) };
            }
            handler(socket, data);
        };
        
}

const io = require('socket.io')(server, {transports: ["websocket", "xhr-polling", "htmlfile", "jsonp-polling"]});
/** configure redis with socket */
io.adapter(redis({host: process.env.REDIS_HOST, port: process.env.REDIS_PORT}));

global.io = io;

/** Request handler  */
var watcher = 0;
var property_id = '';

io.use((socket, next) => {
    const token = socket.handshake.auth.token;
    if (token === global.config.AUTH_TOKEN || token == socket.handshake.query.token) {
        return next(); // allow connection
    }
    return next(new Error("Authentication error"));
});

io.on('connection', function(socket){
    watcher++;
    console.log("Socket connected")
    // -------Automatic create room for notifications time of connection-------
    const user_id = socket.handshake.query.user_id;
    if (user_id) {
        socket.join("notification_" + user_id);
        socket.join("user_" + user_id);
    }

    // -------Create room for notifications-------
    socket.on("joinRoom", (user_id) => {
        socket.join("notification_" + user_id);
        socket.join("user_" + user_id);
    });

    socket.emit('newconnection',{ description: 'Hey, welcome!'});

    socket.on('checkAuctionStatus', function (data) {
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        bid.checkAuctionStatus(socket,data);
    });
    socket.on('checkAuctionEnd', function (data) {
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        bid.checkAuctionEnd(socket,data);
    });

    socket.on('loadsession', function () {
        logger.log("info", "Session started successfully.");
        io.sockets.emit('initNodes', "");
    });

    /**
     * Check bid amount at user end
     */
    socket.on('re-sync',function(data){
        if ('user_id' in data) {
            delete data.user_id;
        }
        io.sockets.emit('re-sync', data);
    })
    
    /**
     * Check bid amount at user end
     */
    socket.on('checkBid', wrapSocketHandler(socket, bid.checkBid));

    socket.on('choose-highest-bid', wrapSocketHandler(socket, bid.chooseHighestBid));

    socket.on('deleteCurrentBid',function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        bid.deleteCurrentBid(socket,data);
        //console.log("---->Check Bid");
    })


    /**
     * Check auction status based on time and mark sold or ended etc..
     */
    socket.on('checkMyBid',function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        bid.checkMyBid(socket,data);
    })


    /**
     * Check auction dashboard
     */
    socket.on('checkAuctionDashboard',function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        bid.checkAuctionDashboard(socket,data);
    })

    /**
     * Check insider auction dashboard
     */
    socket.on('checkInsiderAuctionDashboard',function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        bid.checkInsiderAuctionDashboard(socket,data);
    })

    /**
     * New bidding..
     */
    socket.on('addNewBid', wrapSocketHandler(socket, bid.addNewBid));

    /**
     * Set autobid amount.
     */
    socket.on('setAutoBid', wrapSocketHandler(socket, bid.setAutoBid));

    /**
     * Check bid amount at user end
     */
    socket.on('addBid',function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        bid.addBid(socket,data);
        console.log("---->Add Bid");
        console.log(data);
    })

    /**
     * Check bid amount at user end
     */
    socket.on('bidHistory',function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        bid.bidHistory(socket,data);
        // console.log("---->History Bid");
    })

    /**
     * Check auction status based on time and mark sold or ended etc..
     */
    socket.on('checkAuction', wrapSocketHandler(socket, bid.checkAuction));


    /**
    * Check bid amount at user end
    */
    socket.on('sync',function({data, emitterName}){
        if(Array.isArray(data)){
            data = data.map((currItem) => {
                let user_id = decryptUserId(currItem.user_id);
                return {
                    ...currItem,
                    user_id: parseInt(user_id),
                };
            });
        }else if(typeof data == "object"){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        bid.sync(socket, data, emitterName);
    })
    
    /**
     * Check auction data, .
     */
    socket.on('checkAuctionData',function(data){
        if(Array.isArray(data)){
            data = data.map((currItem) => {
                let user_id = decryptUserId(currItem.user_id);
                return {
                    ...currItem,
                    user_id: parseInt(user_id),
                };
            });
        }else if(typeof data == "object"){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        bid.checkAuctionData(socket, data);
    })

    socket.on('test',function(data){
        // if("user_id" in data && data.user_id != undefined && data.user_id != "" && typeof data == "object" && Array.isArray(data)){
        if(typeof data == "object" && Array.isArray(data)){
            data = data.map((currItem) => {
                let user_id = decryptUserId(currItem.user_id);
                return {
                    ...currItem,
                    user_id: parseInt(user_id),
                };
            });
        }
        bid.test(socket, data);
    })

    socket.on('watcher',function(){
        bid.watcher(socket);
    })


    socket.on('loadChatRooms',function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        chat.loadChatRooms(socket, data);
    })

    socket.on('loadChatRoomConversation',function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        chat.loadChatRoomConversation(socket, data);
    })

    socket.on('sendMessageToUser', function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        chat.sendMessageToUser(socket, data)

    })

    socket.on('userMessageCount', function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        chat.userMessageCount(socket, data)

    })

    //-------------Insider Hybrid Auction---------
    socket.on('dutchAuction', function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        insider.dutchAuction(socket, data)
    })

    //-------------Insider Hybrid Rate Decrease---------
    socket.on('dutchAuctionRateDecrease', function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        insider.dutchAuctionRateDecrease(socket, data)
    })

    //-------------Dutch ended property check---------
    socket.on('dutchAuctionEnded', function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        insider.dutchAuctionEnded(socket, data)
    })

    //-------------Insider Hybrid Sealed Bid Auction---------
    socket.on('sealedAuction', function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        insider.sealedAuction(socket, data)
    })

    //-------------Insider Hybrid Sealed Bid Ended---------
    socket.on('sealedAuctionEnded', function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        insider.sealedAuctionEnded(socket, data)
    })


    //-------------English Auction---------
    socket.on('englishAuction', function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        insider.englishAuction(socket, data)
    })


    //-------------English Auction Ended---------
    socket.on('englishAuctionEnded', function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        insider.englishAuctionEnded(socket, data)
    })

    //-------------Insider User Dashboard-------
    socket.on('insiderUserDashboard',function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        insider.insiderUserDashboard(socket,data);
    })

    // -----------Notifications-----------
    socket.on('getNotifications',function(data){
       if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        notifications.getNotifications(socket,data);
    })

    /**
      * toggle autoBid status.
      */
    socket.on('toggleAutoBidStatus',function(data){
        if("user_id" in data && data.user_id != undefined && data.user_id != ""){
            let user_id = decryptUserId(data.user_id);
            data = {...data, user_id : parseInt(user_id)};
        }
        bid.toggleAutoBidStatus(socket,data);
    })

    //socket.emit('newclientconnect',{ description: 'Hey, welcome!'});
    //socket.broadcast.emit('newclientconnect',{ description: 'clients connected!'})
    try{
        socket.emit('newclientconnect',{ description: 'Hey, welcome!!'});
        socket.broadcast.emit('newclientconnect',{ description: 'Testing'})
    }catch(err){
        console.log(err.message)
    }

    socket.on('disconnect', function () {
        console.log('Socket disconnected');
    });
  
});








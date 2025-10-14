/**
 * This module contains all notifications
 * update at user end. 
 * Date: Feb 12, 2025
 * Author: Gautam
 */ 
const pool = require('../connection');
const logger = require('../app/logger');

module.exports = {
    //-------------Notifications---------
    getNotifications: function getNotifications(socket, data) {
		try{
            let userId = data?.user_id || null,
                sqlQuery = "SELECT (SELECT COUNT(id) FROM event_notification WHERE user_id = " + userId + " AND is_read = false) AS notification_cnt, ";
            sqlQuery += "(SELECT title FROM event_notification WHERE user_id = "+ userId + " AND is_read = false ORDER BY id DESC LIMIT 1) AS notification_title, ";
            sqlQuery += "(SELECT content FROM event_notification WHERE user_id = "+ userId + " AND is_read = false ORDER BY id DESC LIMIT 1) AS notification_content";
            pool.query(sqlQuery, function (err, result) {
                if(err){
                    //check logger wokring
                    logger.log("error", 'ERROR QUERY : ' + sqlQuery);
                    socket.emit("getNotifications", {"msg": err,"error": 1, "status": 400, "type": 5,"res_msg": "notification query error", "user_id": userId});
                    return 0;
                }
                else{
                    if(result.rowCount > 0){
                        //---------send notification count to user--------
                        global.io.to("notification_" + userId).emit("getNotifications", {
                            "data": result.rows[0],
                            "msg": "Fetch data",
                            "status": 201,
                            "error": 0,
                            "user_id": userId
                        });
                    }
                }
            });
			
		}catch(err){
          logger.log("msg", 'ERROR : ' + err.message);
          return 0;
        }
	},
}
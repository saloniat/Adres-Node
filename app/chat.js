/**
 * This module contains all action related to bid which need a realtime
 * update at user end. 
 * Date: March 05, 2025
 * Author: Gautam
 */ 
const pool = require('../connection');
const logger = require('./logger');

module.exports = {

	loadChatRooms: function loadChatRooms(socket, data){
		/**
		 * This is listner function to load chat rooms between buyer/seller
		 * against listing. 
		 * requrest parameter:
		 * 			domain_id: as integer
		 * 			user_id: as integer
		 * 			last_msg_id: as integer
		 * 			msg_type: as string default 'pre_msg'
		 * 			filter_data: as string
		 * 			user_type: as string
		 * response:
		 * 		 db object as json			
		 */	
		try {
            if(parseInt(data.user_id) > 0 && data.user_type){
				user_id = parseInt(data.user_id);
				if(data.user_type != 'agent' && data.user_type != 'buyer' && data.user_type != 'broker' && data.user_type != 'admin'){
					logger.log("error", 'ERROR : Missing required parameter');
					global.io.to("user_" + user_id).emit("loadChatRooms",{"msg":"Forbidden.","status":400, "error": 1});
					return 0;
				}


				let chatRoomQ1 = "SELECT "+
					"mc.*, "+
					"ch.id as child_id, "+
					"ch.added_on as child_added_on, "+
					"us.site_id as seller_site_id, "+
					"ch.sender_id, "+
					"ch.is_read , "+
					"CASE "+
						"WHEN mc.seller_id = " + user_id + " THEN upb.doc_file_name "+
						"ELSE ups.doc_file_name "+
					"END AS doc_file_name, "+
					"CASE "+
						"WHEN mc.seller_id = " + user_id + " THEN upb.bucket_name "+
						"ELSE ups.bucket_name "+
					"END AS bucket_name, "+
					"CASE "+
						"WHEN mc.seller_id = " + user_id + " THEN CONCAT(ub.first_name,' ', ub.last_name) "+
						"ELSE CONCAT(us.first_name,' ', us.last_name) "+
					"END AS name, "+
					"CASE "+
						"WHEN mc.seller_id = " + user_id + " THEN ub.is_logged_in "+
						"ELSE us.is_logged_in "+
					"END AS is_logged_in, "+
					"CASE "+
						"WHEN mc.seller_id = " + user_id + " THEN ub.email "+
						"ELSE us.email "+
					"END AS email, "+
					"CASE "+
						"WHEN mc.seller_id = " + user_id + " THEN ub.phone_no "+
						"ELSE us.phone_no "+
					"END AS phone_no, "+
					"CASE  "+
						"WHEN mc.property_id is not null  THEN CONCAT(pl.address_one, ', ', pl.city, ',' , ls.state_name,', ', pl.postal_code ) "+
						"ELSE '' "+
					"END as property_name, "+
					"(select count(*) from chat where master_id=mc.id and receiver_id = " + user_id + " and status_id=1 and is_read=false) as unread_msg_cnt, "+
					"ch.message,"+
					"cd.document_id as chat_document_id,"+
					"upc.doc_file_name as chat_document_file_name,"+
					"upc.bucket_name as chat_document_bucket_name, "+
					"pl.property_name as prop_prperty_name, "+
					"pl.community as prop_community, "+
					"ls.state_name as prop_state_name, "+
					"us.first_name as first_name, "+
					"nd.domain_name as domain_name " +
				"from master_chat mc "+
				"JOIN chat ch on ch.master_id = mc.id "+
				"JOIN users us on us.id = mc.seller_id "+
				"JOIN users ub on ub.id = mc.buyer_id "+
				"LEFT JOIN "+
					"("+
					"SELECT    MAX(id) max_id, chat_id "+
					"FROM      chat_documents "+
					"GROUP BY  chat_id "+
					") cd_max ON (ch.id = cd_max.chat_id) "+
				"LEFT JOIN chat_documents cd on cd.id = cd_max.max_id "+
				"LEFT JOIN  user_uploads upc on upc.id = cd.document_id "+
				"LEFT JOIN property_listing pl on pl.id = mc.property_id "+
				"LEFT JOIN network_domain nd on nd.id = pl.domain_id " +
				"LEFT JOIN lookup_state ls on ls.id = pl.state_id "+
				"LEFT JOIN user_uploads ups on ups.id = NULLIF(us.profile_image, '')::int "+
				"LEFT JOIN user_uploads upb on upb.id = NULLIF(ub.profile_image, '')::int "+
				"LEFT OUTER JOIN chat ch2 ON (mc.id = ch2.master_id "+
											"AND (ch.added_on < ch2.added_on "+
											"OR (ch.added_on = ch2.added_on AND ch.id < ch2.id))) "+
				"WHERE ch2.id IS NULL and mc.status_id = 1 and us.status_id = 1 and ub.status_id = 1"

				
				// if buyer
				if (data.user_type == "buyer"){
					chatRoomQ1 += " and mc.buyer_id=" + data.user_id + " and mc.domain_id=" +  data.domain_id
				}
				// if admin
				// if(data.user_type == 'broker' || data.user_type == 'agent' ){
				if(data.user_type == 'broker'){
					chatRoomQ1 += " and mc.domain_id=" +  data.domain_id
				}

				//  if agent
				if(data.user_type == 'agent'){
					chatRoomQ1 += " and mc.seller_id="+ data.user_id
				}

				// check if last message
				last_msg_id = (data.last_msg_id) ? parseInt(data.last_msg_id):0
				msg_type = (data.msg_type) ? data.msg_type:''
				if (last_msg_id){
					if (msg_type == 'pre_msg')
						chatRoomQ1 += " and ch.id < " + last_msg_id
					else
						chatRoomQ1 += " and ch.id > " + last_msg_id
				}

				// apply filter
				if(data.filter_data){
					filter_data = data.filter_data.toString().toLowerCase();
					if(data.user_type == 'broker'){ // for broker dash
						if(filter_data == 'buyer')
							chatRoomQ1 += " and mc.seller_id=" + data.user_id 
						else if(filter_data == 'agent')
							chatRoomQ1 += " and mc.seller_id <> " + data.user_id

					} else if(data.user_type == 'agent'){ // for agent dash

						if(filter_data == 'buyer')
							chatRoomQ1 += " and mc.seller_id=" + data.user_id 
						else if(filter_data == 'broker')
							chatRoomQ1 += " and mc.buyer_id=" + data.user_id

					} else if(data.user_type == 'buyer'){
						if(filter_data == 'agent')
							chatRoomQ1 += " and (us.site_id!=" + data.domain_id + " or us.site_id is null) "
						else if(filter_data == 'broker')
							chatRoomQ1 += " and us.site_id=" + data.domain_id
					} else if (data.user_type == 'admin'){
						if(filter_data == 'buyer'){
							chatRoomQ1 += " and mc.seller_id != ch.sender_id" 
						} else if (filter_data == 'agent'){
							chatRoomQ1 += " and mc.seller_id = ch.sender_id and (us.site_id != mc.domain_id  or us.site_id is null)" 
						} else if(filter_data == 'broker'){
							chatRoomQ1 += " and mc.seller_id = ch.sender_id and us.site_id = mc.domain_id" 
						}
					}

				}

				// order by last message
				chatRoomQ1 += " order by ch.id desc LIMIT 15";	

				pool.query(chatRoomQ1, function (err, chatRoomRes) {
					if(err){
						logger.log("error", 'ERROR QUERY : ' + chatRoomQ1);
						global.io.to("user_" + user_id).emit("loadChatRooms", {"msg": err, 'status': 400, "error": 1});
						return 0;
					}
					else{
					if(chatRoomRes.rowCount){
						global.io.to("user_" + user_id).emit("loadChatRooms", {"data": chatRoomRes.rows, 'msg_type': msg_type, "status": 201, "error": 0, "user_type": data.user_type});
					}
					else{
						global.io.to("user_" + user_id).emit("loadChatRooms", {"msg":"No chat room found", data: [], 'msg_type': msg_type  ,"status": 201, "error": 0, "user_type": data.user_type});
					}
					return 0;
					}
				});
			}
			else{
				//required parameter not found
				logger.log("error", 'ERROR : Missing required parameter');
				socket.emit("loadChatRooms",{"msg":"Forbidden.","status":400, "error": 1});
				return 0;
			}
		}catch(err){
			//trapped error
			logger.log("error", 'ERROR : ' + err.message);
			socket.emit("loadChatRooms",{"msg":err.message,"status":400, "error": 1});
			return 0;
        }
	
	},

	loadChatRoomConversation: function loadChatRoomConversation(socket, data){
		/**
		 * This is listner function to load chat between buyer/seller
		 * against listing. 
		 * requrest parameter:
		 * 			domain_id: as integer
		 * 			user_id: as integer
		 * 			last_msg_id: as integer
		 * 			msg_type: as string default 'pre_msg'
		 *          master_id: as integer
		 * 			user_type: as string
		 * response:
		 * 		 db object as json			
		 */	
		try {
            if(parseInt(data.user_id) > 0 && data.master_id && data.user_type){
				user_id = parseInt(data.user_id)

				if(data.user_type == 'broker' || data.user_type == 'agent'){
					disabledChatCase = "CASE WHEN mc.seller_id = " + user_id + " THEN false ELSE true END as disable_chat, "
				} else if(data.user_type == 'buyer'){
					disabledChatCase = "CASE WHEN mc.buyer_id = " + user_id + " THEN false ELSE true END as disable_chat, "
				} else {
					disabledChatCase = "true as disable_chat, "
				}

				let chatRoomConversation = "SELECT " +
						"mc.buyer_id as buyer_id, " +
						"mc.seller_id as seller_id, " +
						"mc.domain_id as master_site_id, " +
						"useller.site_id as seller_site_id, " +
						"ch.*, " +
						"ups.doc_file_name AS image_name, " +
						"ups.bucket_name AS bucket_name, " +
						"CONCAT(us.first_name,' ', us.last_name) AS name, " +
						"us.email AS email, " +
						"us.phone_no as phone_no, " +
						disabledChatCase +
						"cd.document_id as chat_document_id, "+
						"upc.doc_file_name as chat_document_file_name, "+
						"upc.bucket_name as chat_document_bucket_name "+
					"from chat ch " +
					"LEFT JOIN chat_documents cd on cd.chat_id = ch.id "+
  					"LEFT JOIN  user_uploads upc on upc.id = cd.document_id "+
					"JOIN master_chat mc on mc.id = ch.master_id " +
					"JOIN users us on us.id = ch.sender_id " +
					"JOIN users useller on useller.id = mc.seller_id " +
					"LEFT JOIN user_uploads ups on ups.id = NULLIF(us.profile_image, '')::int " +
					"WHERE mc.status_id = 1 and ch.master_id=" +
					data.master_id + " and ch.status_id=1 and us.status_id = 1 and useller.status_id = 1 ";
				

				// check if last message
				last_msg_id = (data.last_msg_id) ? parseInt(data.last_msg_id):0
				msg_type = (data.msg_type) ? data.msg_type:''
				if (last_msg_id){
					if (msg_type == 'pre_msg')
						chatRoomConversation += " and ch.id < " + last_msg_id
					else
						chatRoomConversation += " and ch.id > " + last_msg_id
				}

				// order by last message
				chatRoomConversation += " order by ch.id desc LIMIT 15";

				pool.query(chatRoomConversation, function (err, chatRoomRes) {
					if(err){
						logger.log("error", 'ERROR QUERY : ' + chatRoomConversation);
						global.io.to("user_" + data.user_id).emit("loadChatRoomConversation", {"msg": err, 'status': 400, "error": 1});
						return 0;
					}
					else{
					if(chatRoomRes.rowCount){
						if(chatRoomRes.rows[0].disable_chat == false){
							// mark message as read
							try {
								let updateIsRead = "UPDATE chat SET is_read=true WHERE is_read=false and master_id = " + data.master_id + " and receiver_id=" + user_id;
								pool.query(updateIsRead,function (err) {
									if(err){
										logger.log("error", 'ERROR QUERY : ' + updateIsRead);
									}
								});	
							} catch (error) {
								logger.log("error", 'ERROR QUERY : ' + updateIsRead);
							}
						}
						global.io.to("user_" + data.user_id).emit("loadChatRoomConversation", {"data": chatRoomRes.rows, 'msg_type': msg_type, "status": 201, "error": 0, "user_type": data.user_type});
					}
					else{
						global.io.to("user_" + data.user_id).emit("loadChatRoomConversation", {"msg":"No message found in room", data: [], 'msg_type': msg_type  ,"status": 201, "error": 0, "user_type": data.user_type});
					}
					return 0;
					}
				});
			}
			else{
			//required parameter not found
			logger.log("error", 'ERROR : Missing required parameter');
			socket.emit("loadChatRoomConversation",{"msg":"Forbidden.","status":400, "error": 1});
			return 0;
			}
		}catch(err){
			//trapped error
			logger.log("error", 'ERROR : ' + err.message);
			socket.emit("loadChatRoomConversation",{"msg":err.message,"status":400, "error": 1});
			return 0;
        }
	
	},


	sendMessageToUser__OLD: function sendMessageToUser(socket, data){
		/**
		 * This is listner function to send chat between buyer/seller
		 * against listing. 
		 * requrest parameter:
		 * 			domain_id: as integer
		 * 			user_id: as integer
		 *          master_id: as integer
		 * 			message: as string
		 * response:
		 * 		 db object as json			
		 */	
		try {
            if(parseInt(data.user_id) > 0 && data.master_id && data.domain_id && (data.message || data.chat_doc_ids.length > 0)){
				user_id = parseInt(data.user_id)

				checkAuthorization = 'select '+ 
						'CASE '+ 
						'WHEN mc.buyer_id = ' + user_id + '  THEN mc.seller_id '+
						'ELSE mc.buyer_id '+
					'END as receiver_id  '+
					'from users us '+
					'join master_chat mc on mc.buyer_id=us.id or mc.seller_id=us.id '+
					'left outer join network_user nu on nu.domain_id = mc.domain_id and nu.user_id = us.id '+
					'where '+
					'mc.id=' + data.master_id + ' and '+
					'mc.domain_id=' + data.domain_id + ' and '+
					'mc.status_id=1 and '+
					'us.id = ' + user_id + ' and '+
					'us.status_id=1 and ' +
					'(mc.seller_id=' + user_id + ' or mc.buyer_id=' + user_id + ') and '+
					'((us.user_type_id=2 and us.site_id=' + data.domain_id + ') or '+
					'nu.status_id =1)'

				pool.query(checkAuthorization, function (err, authRes) {
					if(err){
						logger.log("error", 'ERROR QUERY : ' + checkAuthorization);
						socket.emit("sendMessageToUser", {"msg": err, 'status': 400, "error": 1});
						return 0;
					}
					else{
						if(authRes.rows[0] && authRes.rows[0].receiver_id){
							if(data.message != ''){
								message = data.message
							} else{
								message = ''
							}
							let saveChatResponse = "INSERT into chat (added_on, updated_on, master_id, message, sender_id, receiver_id, status_id, is_read) " +
									"VALUES(now(), now(), " + data.master_id + ",'" + message + "'," + user_id + ", " + authRes.rows[0].receiver_id + ", 1, false) returning *";
						pool.query(saveChatResponse, function (err, saveChatRes) {
							if(err){
								console.log(err)
								logger.log("error", 'ERROR QUERY : ' + saveChatResponse);
								socket.emit("sendMessageToUser", {"msg": err, 'status': 400, "error": 1});
								return 0;
							}
							else{
								if(saveChatRes.rowCount){
									if(saveChatRes.rows[0].id && data.chat_doc_ids){
										// insert data documents
										for(const val of data.chat_doc_ids) {
											bidAddQl = "INSERT into chat_documents (chat_id, document_id, added_on, updated_on)";
											bidAddQl += "VALUES("+ saveChatRes.rows[0].id  +", "+ val +", now(), now())";
											executeQuery(bidAddQl);
										}
									}
									socket.emit("sendMessageToUser", {"msg": 'Message sent successfully', 'msg_type': msg_type, "status": 201, "error": 0});
								}
								return 0;
							}
						});
						}
					}
				});
			}
			else{
				//required parameter not found
				logger.log("error", 'ERROR : Missing required parameter');
				socket.emit("sendMessageToUser",{"msg":"Forbidden.","status":400, "error": 1});
				return 0;
			}
		}catch(err){
			//trapped error
			logger.log("error", 'ERROR : ' + err.message);
			socket.emit("sendMessageToUser",{"msg":err.message,"status":400, "error": 1});
			return 0;
        }
	
	},

	userMessageCount: function userMessageCount(socket, data){
		/**
		 * This is listner function to send chat between buyer/seller
		 * against listing. 
		 * requrest parameter:
		 * 			domain_id: as integer
		 * 			user_id: as integer
		 *          user_type: as string
		 * response:
		 * 		 db object as json			
		 */	
		try {
            if(parseInt(data.user_id) > 0 && data.domain_id && data.user_type){
				user_id = parseInt(data.user_id)

				userMessageCnt = 'select count(*) from chat as c '+
					'join master_chat mc on mc.id = c.master_id '+
					'where c.is_read=false '+
					'and mc.status_id=1 '+
					'and c.status_id=1 ';
				
				// include only domain specific count for users
				if (data.user_type == 'broker' || data.user_type == 'agent' || data.user_type == 'buyer' ){
					userMessageCnt += ' and mc.domain_id = ' + data.domain_id
				}

				//filter for agents under brokers
				if(data.user_type == 'agent' ){
					userMessageCnt += ' and mc.seller_id=' + user_id + ' and c.receiver_id=' + user_id
				}

				if(data.user_type == 'broker'){
					userMessageCnt += ' and c.receiver_id=' + user_id
				}

				// filter for buyer under agents/broker
				if(data.user_type == 'buyer'){
					userMessageCnt += ' and mc.buyer_id=' + user_id + ' and c.receiver_id=' + user_id
				}


				pool.query(userMessageCnt, function (err, countRes) {
					if(err){
						logger.log("error", 'ERROR QUERY : ' + userMessageCnt);
						socket.emit("userMessageCount", {"msg": err, 'status': 400, "error": 1});
						return 0;
					}
					else{
						socket.emit("userMessageCount", {"data": countRes.rows, 'user_type': data.user_type, "status": 201, "error": 0});
					}
				});
			}
			else{
				//required parameter not found
				logger.log("error", 'ERROR : Missing required parameter');
				socket.emit("userMessageCount",{"msg":"Forbidden.","status":400, "error": 1});
				return 0;
			}
		}catch(err){
			//trapped error
			logger.log("error", 'ERROR : ' + err.message);
			socket.emit("userMessageCount",{"msg":err.message,"status":400, "error": 1});
			return 0;
        }
	
	},

	sendMessageToUser: function sendMessageToUser(socket, data){
		/**
		 * This is listner function to send chat between buyer/seller
		 * against listing. 
		 * requrest parameter:
		 * 			domain_id: as integer
		 * 			user_id: as integer
		 *          master_id: as integer
		 * 			message: as string
		 * response:
		 * 		 db object as json			
		 */	
		try {
			if (!("master_id" in data) && parseInt(data.user_id) > 0 && parseInt(data.seller_id) > 0 && parseInt(data.property_id) > 0 && data.domain_id && (data.message || data.chat_doc_ids.length > 0)) {
				masrerQry = "INSERT INTO master_chat (domain_id, property_id, buyer_id, seller_id, added_by_id, status_id, added_on, updated_on) "+
				"VALUES("+data.domain_id+", "+data.property_id+", "+data.user_id+", "+data.seller_id+", "+data.user_id+", 1, now(), now()) returning *";
				pool.query(masrerQry, function (err, qryRes) {
					if(err){
						logger.log("error", 'ERROR QUERY : ' + masrerQry);
						socket.emit("sendMessageToUser", {"msg": err, 'status': 400, "error": 1});
						return 0;
					}else{
						if(qryRes.rowCount){
							if(qryRes.rows[0].id){
								var master_id = qryRes.rows[0].id;
								var user_id = parseInt(data.user_id);
								let saveChatResponse = "INSERT into chat (added_on, updated_on, master_id, message, sender_id, receiver_id, status_id, is_read) " +
									"VALUES(now(), now(), " + master_id + ",'" + data.message + "'," + user_id + ", " + data.seller_id + ", 1, false) returning *";
								pool.query(saveChatResponse, function (err, saveChatRes) {
									if(err){
										console.log(err)
										logger.log("error", 'ERROR QUERY : ' + saveChatResponse);
										socket.emit("sendMessageToUser", {"msg": err, 'status': 400, "error": 1});
										return 0;
									}
									else{
										if(saveChatRes.rowCount){
											if(saveChatRes.rows[0].id && data.chat_doc_ids){
												// insert data documents
												for(const val of data.chat_doc_ids) {
													bidAddQl = "INSERT into chat_documents (chat_id, document_id, added_on, updated_on)";
													bidAddQl += "VALUES("+ saveChatRes.rows[0].id  +", "+ val +", now(), now())";
													executeQuery(bidAddQl);
												}
											}
											socket.emit("sendMessageToUser", {"msg": 'Message sent successfully', 'user_message': data.message, 'msg_type': msg_type, "status": 201, 'master_id': master_id, "error": 0});
											createSendMessage(parseInt(data.domain_id), parseInt(master_id), parseInt(data.user_id), parseInt(data.seller_id));

											// --------This section for emit loadChatRooms and loadChatRoomConversation socket-------
											// Send socket events to both users
											let loadPayloadOne;
											let loadPayloadTwo;
											let loadPayloadThree;
											let loadPayloadFour;
											if(data.user_type == 'buyer'){
												loadPayloadOne = {
													user_id: data.user_id,
													master_id: parseInt(master_id),
													user_type: "buyer",
													domain_id: parseInt(data.domain_id),
													last_msg_id: ""
												};
												// loadPayloadFour = {
												// 	user_id: data.seller_id,
												// 	master_id: parseInt(master_id),
												// 	user_type: "buyer",
												// 	domain_id: parseInt(data.domain_id),
												// 	last_msg_id: ""
												// };
												loadPayloadTwo = {
													user_id: data.seller_id,
													master_id: parseInt(master_id),
													user_type: "broker",
													domain_id: parseInt(data.domain_id),
													last_msg_id: ""
												};
												loadPayloadThree = {
													user_id: data.seller_id,
													master_id: parseInt(master_id),
													user_type: "agent",
													domain_id: parseInt(data.domain_id),
													last_msg_id: ""
												};
											}else{
												loadPayloadOne = {
													user_id: data.user_id,
													master_id: parseInt(master_id),
													user_type: data.user_type,
													domain_id: parseInt(data.domain_id),
													last_msg_id: ""
												};
												loadPayloadTwo = {
													user_id: data.seller_id,
													master_id: parseInt(master_id),
													user_type: (data.user_type == 'agent')? "broker": "agent",
													domain_id: parseInt(data.domain_id),
													last_msg_id: ""
												};
												loadPayloadThree = {
													user_id: data.seller_id,
													master_id: parseInt(master_id),
													user_type: "buyer",
													domain_id: parseInt(data.domain_id),
													last_msg_id: ""
												};
											}
											module.exports.loadChatRoomConversation(socket, loadPayloadOne);
											module.exports.loadChatRoomConversation(socket, loadPayloadTwo);
											// if(data.user_type != "broker"){
											// 	module.exports.loadChatRoomConversation(socket, loadPayloadThree);
											// }
											module.exports.loadChatRoomConversation(socket, loadPayloadThree);
											// if(loadPayloadFour){
											// 	module.exports.loadChatRoomConversation(socket, loadPayloadFour);
											// }
											
											let payloadOne;
											let payloadTwo;
											let payloadThree;
											let payloadFour;
											if(data.user_type == 'buyer'){
												payloadOne = {
													user_id: data.user_id,
													filter_data: "",
													user_type: "buyer",
													domain_id: parseInt(data.domain_id),
													last_msg_id: 0
												};

												payloadFour = {
													user_id: data.seller_id,
													filter_data: "",
													user_type: "buyer",
													domain_id: parseInt(data.domain_id),
													last_msg_id: 0
												};

												payloadTwo = {
													user_id: data.seller_id,
													filter_data: "",
													user_type: "broker",
													domain_id: parseInt(data.domain_id),
													last_msg_id: 0
												};
												payloadThree = {
													user_id: data.seller_id,
													filter_data: "",
													user_type: "agent",
													domain_id: parseInt(data.domain_id),
													last_msg_id: 0
												};
											}else{
												payloadOne = {
													user_id: data.user_id,
													filter_data: "",
													user_type: data.user_type,
													domain_id: parseInt(data.domain_id),
													last_msg_id: 0
												};
												payloadTwo = {
													user_id: data.seller_id,
													filter_data: "",
													user_type: (data.user_type == 'agent')? "broker": "agent",
													domain_id: parseInt(data.domain_id),
													last_msg_id: 0
												};
												payloadThree = {
													user_id: data.seller_id,
													filter_data: "",
													user_type: "buyer",
													domain_id: parseInt(data.domain_id),
													last_msg_id: 0
												};
											}
											module.exports.loadChatRooms(socket, payloadOne);
											setTimeout(function(){
												module.exports.loadChatRooms(socket, payloadTwo);
											}, 300);
											setTimeout(function(){
												// if(data.user_type != "broker"){
												// 	module.exports.loadChatRooms(socket, payloadThree);
												// }
												module.exports.loadChatRooms(socket, payloadThree);
											}, 300);
											// setTimeout(function(){
											// 	if(payloadFour){
											// 		module.exports.loadChatRooms(socket, payloadFour);
											// 	}
											// }, 300);
										}
										return 0;
									}
								});
							}
						}
					}
				});
			}else if(parseInt(data.user_id) > 0 && data.master_id && data.domain_id && (data.message || data.chat_doc_ids.length > 0)){
				user_id = parseInt(data.user_id)

				checkAuthorization = 'select '+ 
						'CASE '+ 
						'WHEN mc.buyer_id = ' + user_id + '  THEN mc.seller_id '+
						'ELSE mc.buyer_id '+
					'END as receiver_id  '+
					'from users us '+
					'join master_chat mc on mc.buyer_id=us.id or mc.seller_id=us.id '+
					'left outer join network_user nu on nu.domain_id = mc.domain_id and nu.user_id = us.id '+
					'where '+
					'mc.id=' + data.master_id + ' and '+
					'mc.domain_id=' + data.domain_id + ' and '+
					'mc.status_id=1 and '+
					'us.id = ' + user_id + ' and '+
					'us.status_id=1 and ' +
					'(mc.seller_id=' + user_id + ' or mc.buyer_id=' + user_id + ') and '+
					'((us.user_type_id=2 and us.site_id=' + data.domain_id + ') or '+
					'nu.status_id =1)'

				pool.query(checkAuthorization, function (err, authRes) {
					if(err){
						logger.log("error", 'ERROR QUERY : ' + checkAuthorization);
						socket.emit("sendMessageToUser", {"msg": err, 'status': 400, "error": 1});
						return 0;
					}
					else{
						if(authRes.rows[0] && authRes.rows[0].receiver_id){
							if(data.message != ''){
								message = data.message
							} else{
								message = ''
							}
							let saveChatResponse = "INSERT into chat (added_on, updated_on, master_id, message, sender_id, receiver_id, status_id, is_read) " +
									"VALUES(now(), now(), " + data.master_id + ",'" + message + "'," + user_id + ", " + authRes.rows[0].receiver_id + ", 1, false) returning *";
						pool.query(saveChatResponse, function (err, saveChatRes) {
							if(err){
								console.log(err)
								logger.log("error", 'ERROR QUERY : ' + saveChatResponse);
								socket.emit("sendMessageToUser", {"msg": err, 'status': 400, "error": 1});
								return 0;
							}
							else{
								if(saveChatRes.rowCount){
									if(saveChatRes.rows[0].id && data.chat_doc_ids){
										// insert data documents
										for(const val of data.chat_doc_ids) {
											bidAddQl = "INSERT into chat_documents (chat_id, document_id, added_on, updated_on)";
											bidAddQl += "VALUES("+ saveChatRes.rows[0].id  +", "+ val +", now(), now())";
											executeQuery(bidAddQl);
										}
									}
									socket.emit("sendMessageToUser", {"msg": 'Message sent successfully', 'user_message': data.message, 'msg_type': msg_type, "status": 201, "error": 0});
									createSendMessage(parseInt(data.domain_id), parseInt(data.master_id), parseInt(data.user_id), parseInt(authRes.rows[0].receiver_id));
									
									// --------This section for emit loadChatRooms and loadChatRoomConversation socket-------
									// Send socket events to both users
									let loadPayloadOne;
									let loadPayloadTwo;
									let loadPayloadThree;
									let loadPayloadFour;
									console.log("user type="+data.user_type+"=====");
									if(data.user_type == 'buyer'){
										loadPayloadOne = {
											user_id: data.user_id,
											master_id: data.master_id,
											user_type: "buyer",
											domain_id: parseInt(data.domain_id),
											last_msg_id: ""
										};

										// loadPayloadFour = {
										// 	user_id: authRes.rows[0].receiver_id,
										// 	master_id: data.master_id,
										// 	user_type: "buyer",
										// 	domain_id: parseInt(data.domain_id),
										// 	last_msg_id: ""
										// };

										loadPayloadTwo = {
											user_id: authRes.rows[0].receiver_id,
											master_id: data.master_id,
											user_type: "broker",
											domain_id: parseInt(data.domain_id),
											last_msg_id: ""
										};
										loadPayloadThree = {
											user_id: authRes.rows[0].receiver_id,
											master_id: data.master_id,
											user_type: "agent",
											domain_id: parseInt(data.domain_id),
											last_msg_id: ""
										};
									}else{
										loadPayloadOne = {
											user_id: data.user_id,
											master_id: data.master_id,
											user_type: data.user_type,
											domain_id: parseInt(data.domain_id),
											last_msg_id: ""
										};
										loadPayloadTwo = {
											user_id: authRes.rows[0].receiver_id,
											master_id: data.master_id,
											user_type: (data.user_type == 'agent')? "broker": "agent",
											domain_id: parseInt(data.domain_id),
											last_msg_id: ""
										};
										loadPayloadThree = {
											user_id: authRes.rows[0].receiver_id,
											master_id: data.master_id,
											user_type: "buyer",
											domain_id: parseInt(data.domain_id),
											last_msg_id: ""
										};
									}

									module.exports.loadChatRoomConversation(socket, loadPayloadOne);
									module.exports.loadChatRoomConversation(socket, loadPayloadTwo);
									module.exports.loadChatRoomConversation(socket, loadPayloadThree);
									// if(data.user_type != "broker"){
									// 	// module.exports.loadChatRoomConversation(socket, loadPayloadThree);
									// }
									
									// if (loadPayloadFour){
									// 	module.exports.loadChatRoomConversation(socket, loadPayloadFour);
									// }

									let payloadOne;
									let payloadTwo;
									let payloadThree;
									let payloadFour;
									if(data.user_type == 'buyer'){
										payloadOne = {
											user_id: data.user_id,
											filter_data: "",
											user_type: "buyer",
											domain_id: parseInt(data.domain_id),
											last_msg_id: 0
										};

										// payloadFour = {
										// 	user_id: authRes.rows[0].receiver_id,
										// 	filter_data: "",
										// 	user_type: "buyer",
										// 	domain_id: parseInt(data.domain_id),
										// 	last_msg_id: 0
										// };

										payloadTwo = {
											user_id: authRes.rows[0].receiver_id,
											filter_data: "",
											user_type: "broker",
											domain_id: parseInt(data.domain_id),
											last_msg_id: 0
										};
										payloadThree = {
											user_id: authRes.rows[0].receiver_id,
											filter_data: "",
											user_type: "agent",
											domain_id: parseInt(data.domain_id),
											last_msg_id: 0
										};
									}else{
										payloadOne = {
											user_id: data.user_id,
											filter_data: "",
											user_type: data.user_type,
											domain_id: parseInt(data.domain_id),
											last_msg_id: 0
										};
										payloadTwo = {
											user_id: authRes.rows[0].receiver_id,
											filter_data: "",
											user_type: (data.user_type == 'agent')? "broker": "agent",
											domain_id: parseInt(data.domain_id),
											last_msg_id: 0
										};
										payloadThree = {
											user_id: authRes.rows[0].receiver_id,
											filter_data: "",
											user_type: "buyer",
											domain_id: parseInt(data.domain_id),
											last_msg_id: 0
										};
									}
									module.exports.loadChatRooms(socket, payloadOne);
									setTimeout(function(){
										module.exports.loadChatRooms(socket, payloadTwo);
									}, 500);
									setTimeout(function(){
										// if(data.user_type != "broker"){
										// 	module.exports.loadChatRooms(socket, payloadThree);
										// }
										module.exports.loadChatRooms(socket, payloadThree);
									}, 500);
									// setTimeout(function(){
									// 	if(payloadFour){
									// 		module.exports.loadChatRooms(socket, payloadFour);
									// 	}
									// }, 500);
									
								}
								return 0;
							}
						});
						}
					}
				});
			}
			else{
				//required parameter not found
				logger.log("error", 'ERROR : Missing required parameter');
				socket.emit("sendMessageToUser",{"msg":"Forbidden.","status":400, "error": 1});
				return 0;
			}
		}catch(err){
			//trapped error
			logger.log("error", 'ERROR : ' + err.message);
			socket.emit("sendMessageToUser",{"msg":err.message,"status":400, "error": 1});
			return 0;
        }
	
	},

}

function createSendMessage(domain_id, master_id, sender_id, receiver_id){
	returnBody = JSON.stringify(
			{
				"domain_id": domain_id,
				"master_id": master_id,
				"sender_id": sender_id,
				"receiver_id": receiver_id
				}
	);
	//console.log(returnBody);
	msg = messageAPICall(returnBody);
	return ;
}

function messageAPICall(postData){
	const https = require(global.config.API_PROTOCOL);
	const data = postData;
	const options = {
		  hostname: global.config.API_URL,
		  port: global.config.API_PORT,
		  path: '/api-contact/send-chat-email/',
		  method: 'POST',
		  rejectUnauthorized: false,
		  headers: {
			'Content-Type': 'application/json;',
			'Connection': 'keep-alive',
			'Content-Length': data.length,
			'Authorization': global.config.API_TOKEN
		  }
	};

	let reqPost = https.request(options, function(res){
		if(res.statusCode !== 200){
			// to do
			logger.log("error", 'Email Error : Code '+ res.statusMessage +"; Message :" + res.statusMessage);
		}

		res.on('data', (d) => {
			process.stdout.write(d);
		})

		res.on('end', () => {
			global.io.emit("syncNotifications")
		});
	});


	/** Log error*/
	reqPost.on('error', function(e) {
		console.error(e.code);
		console.error(e);
		logger.log("error", 'ERROR MESSAGE : ' + e);
	});

	reqPost.write(data);
	//gracely close connection	
	reqPost.end();
}

function call_socket(receiverSocket, loadData){
	receiverSocket.emit("loadChatRoomConversation", loadData);
}

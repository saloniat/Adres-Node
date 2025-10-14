/**
 * This module contains all action related to bid which need a realtime
 * update at user end.
 * Date: July 26, 2021
 * Author: Gautam
 */
const pool = require('../connection');
const logger = require('../app/logger');
const Holidays = require('date-holidays');

module.exports = {
	sync: async function sync(socket, data, emitterName = "sync") {
		try {
			if (!data?.length) return;

			const data_arr = [];
			const queryPromises = data?.map(async (auctionData) => {
				let flshQuery = `
			  SELECT 
			  	plbn.added_on offerer_offer_date,
				plbn.buy_now_amount offerer_offer_amount, 
				plbn.buy_now_status offerer_offer_status, 
				plbn.user_id offerer_user_id,
				pl.id AS property_id, 
				a.bid_increments, 
				a.start_date, 
				a.end_date, 
				a.start_price, 
				a.id AS auction_id, 
				bcv.total AS bid_count, 
				a.status_id AS auction_status, 
				a.reserve_amount, 
				pl.status_id AS listing_status_id, 
				pls.status_name AS listing_status_name, 
				ls.status_name AS auction_status_name, 
				pl.closing_status_id, 
				plc.status_name AS closing_statuss_name,
				EXTRACT(EPOCH FROM (a.end_date::timestamp - NOW()::timestamp))::INTEGER AS time_left_hr,
				EXTRACT(EPOCH FROM (a.start_date::timestamp - NOW()::timestamp))::INTEGER AS start_time_left_hr,
				(SELECT bid.user_id FROM bid INNER JOIN bid_registration br ON br.user_id = bid.user_id AND br.property_id = bid.property_id WHERE bid.is_retracted = false AND bid.auction_id = $3 AND br.purchase_forefit_status != 2 ORDER BY bid.bid_amount DESC limit 1) as max_bidder_user_id,
			  (SELECT bid_amount from bid where user_id = $1  and auction_id = $3 and bid.is_retracted = false ORDER BY bid_amount desc limit 1) as my_max_bid_val,
			  (SELECT is_retracted from bid where user_id = $1  and auction_id = $3 and bid.is_retracted = true ORDER BY bid_amount desc limit 1) is_bid_retracted,
			  (SELECT MAX(bid.bid_amount) FROM bid INNER JOIN bid_registration br ON br.user_id = bid.user_id AND br.property_id = bid.property_id WHERE bid.is_retracted= false AND bid.property_id = $2 AND br.purchase_forefit_status != 2 limit 1) as high_bid_amt,
			  (SELECT count(*) from property_listing_buy_now where property_id = $2) as offers_count 
			  FROM property_listing pl
			  LEFT JOIN property_auction a ON pl.id = a.property_id
			  LEFT JOIN lookup_status ls ON ls.id = a.status_id
			  LEFT JOIN lookup_status pls ON pls.id = pl.status_id
			  LEFT JOIN lookup_status plc ON plc.id = pl.closing_status_id
			  LEFT JOIN bid_count_view bcv ON pl.id = bcv.property_id
			  LEFT JOIN property_listing_buy_now plbn on  plbn.property_id = $2 and plbn.buy_now_status = 1
			  WHERE pl.id = $2 AND a.id = $3 AND pl.domain_id = $4
			  ORDER BY pl.id DESC LIMIT 1
			`;

			// Use parseInt and fallback to null if invalid
			const userId = parseInt(auctionData?.user_id);
			const propertyId = parseInt(auctionData?.property_id);
			const auctionId = parseInt(auctionData?.auction_id);
			const domainId = parseInt(auctionData?.domain_id);
	  
			const result = await pool.query(flshQuery, [
			  isNaN(userId) ? null : userId,
			  isNaN(propertyId) ? null : propertyId,
			  isNaN(auctionId) ? null : auctionId,
			  isNaN(domainId) ? null : domainId,
			]);
			return result.rows[0];
			});

			// Wait for all queries to finish concurrently
			const results = await Promise.all(queryPromises);
			data_arr.push(...results);

			// Emit data to the socket
			socket.emit(emitterName, {
				data: data_arr,
				status: 201,
				error: 0,
				type: 4,
				res_msg: "not loggedin"
			});
			// console.log("sync");
			(data[0]?.property_id && data[0]?.user_id) &&
			socket.in(`${data[0].property_id}_${data[0].user_id}`).emit(emitterName, {
				data: data_arr,
				status: 201,
				error: 0,
				type: 4,
				res_msg: "not loggedin"
			});

		} catch (err) {
			console.log(err);
			logger.log("error", 'ERROR : ' + err.message);
			return 0;
		}
	},

	chooseHighestBid: async function chooseHighestBid(socket, data, broadcast = false) {
		//first we check if user id , property id, auction id and domain id exist in parameter
		try {
			if (broadcast) socket = global.io;
			let userId = data?.user_id || null
			let propertyId = data?.property_id || null
			let domainId = data?.domain_id || null
			let auctionId = data?.auction_id || null

			if (!propertyId || !domainId || !auctionId) {
				const errorMsg = `Missing required params: property_id=${propertyId}, domain_id=${domainId}, auction_id=${auctionId}`;
				logger.log("warn", "CHOOSE_HIGHEST_BID_1: " + errorMsg);
				socket.emit("checkBid", {
					msg: "Missing required parameters",
					error: 1,
					status: 400,
					type: 5,
					res_msg: "Required: property_id, domain_id, auction_id",
					user_id: userId,
				});
				return;
			}

			const chkQry = `
							SELECT b.id, b.bid_amount
							FROM bid b
							JOIN bid_registration br 
								ON b.registration_id = br.id 
								AND br.user_id = b.user_id 
							WHERE b.property_id = $1
								AND b.is_canceled = false
								AND b.is_retracted = false
								AND b.domain_id = $2
								AND br.purchase_forefit_status != 2
							ORDER BY b.id DESC
							LIMIT 1;
							`;
			pool.query(chkQry,[parseInt(propertyId), parseInt(domainId)], function (err, checkRes) {
				if (err) {
					//if error in sql let log query
					logger.log("error", 'CHOOSE_HIGHEST_BID_2 ERROR QUERY : ' + chkQry);
					socket.emit("choose-highest-bid", { "msg": err, "error": 1, "status": 400, "type": 5, "res_msg": "bid reg query error", "data": {} });
					return 0;
				} else {
					if (checkRes?.rowCount > 0) {
						const bidId = checkRes?.rows[0]?.id;
						const bid_amount = checkRes?.rows[0]?.bid_amount;
						const updateQry = `UPDATE bid SET selected_highest_bid =  $1 WHERE id =  $2`;
						pool.query(updateQry,[true, bidId], async function (err, result) {
							if (err) {
								logger.log("error", 'CHOOSE_HIGHEST_BID_3 ERROR QUERY : ' + updateQry);
								socket.emit("choose-highest-bid", {
								msg: err,
								error: 1,
								status: 400,
								type: 5,
								res_msg: "Bid update error",
								data: {}
								});
							} else {
								const checkPaymentStatusQuery = `
									update property_listing set closing_status_id = 35
									WHERE id = ${propertyId} `;

								const paymentStatus = await pool.query(checkPaymentStatusQuery);
								socket.emit("choose-highest-bid", {
									msg: "Bid updated succesfully",
									error: 0,
									status: 200,
									type: 5,
									res_msg: "Success",
									data: {bid_amount}
								  });
								  global.io.emit("re-sync", {data})
							}
						});
					} else {
						socket.emit("choose-highest-bid", {
							msg: "No active bid found",
							error: 0,
							status: 204,
							type: 5,
							res_msg: "No data",
							data: {}
						  });
					}
				}
			});		
		} catch (err) {
			logger.log("error", `CHOOSE_HIGHEST_BID_4 ERROR: ${err.stack}`);
			return 0;
		}
	},
	checkBid: function checkBid(socket, data, broadcast = false) {
		//first we check if user id , property id, auction id and domain id exist in parameter
		try {
			if (broadcast) socket = global.io;
			let userId = data?.user_id || null
			let propertyId = data?.property_id || null
			let domainId = data?.domain_id || null
			let auctionId = data?.auction_id || null

			if (!propertyId || !domainId || !auctionId) {
				const errorMsg = `Missing required params: property_id=${propertyId}, domain_id=${domainId}, auction_id=${auctionId}`;
				logger.log("warn", "CHECKBID_MISSING_PARAMS: " + errorMsg);
				socket.emit("checkBid", {
					msg: "Missing required parameters",
					error: 1,
					status: 400,
					type: 5,
					res_msg: "Required: property_id, domain_id, auction_id",
					user_id: userId,
				});
				return;
			}

			if (userId) {
				let chkQry = "select id from bid_registration where property_id = " + propertyId + " and status_id = 1 and user_id =" + userId + " and domain_id=" + domainId;

				pool.query(chkQry, function (err, checkRes) {
					if (err) {
						//if error in sql let log query
						logger.log("error", 'CHECKBID_1 ERROR QUERY : ' + chkQry);
						socket.emit("checkBid", { "msg": err, "error": 1, "status": 400, "type": 5, "res_msg": "bid reg query error", "data": {} });
						return 0;
					} else {
						if (checkRes?.rowCount > 0) {
							let sqlQuery = "select plbn.added_on offerer_offer_date, plbn.buy_now_amount offerer_offer_amount, plbn.buy_now_status offerer_offer_status, plbn.user_id offerer_user_id, b.property_id as  property_id, b.user_id  as user_id, total as bid_count, br.is_reviewed as is_reviewed, br.is_approved as is_approved, a.id as auction_id, a.auction_id as auction_type_id,a.start_price,a.bid_increments,a.start_date,a.status_id as auction_status,a.reserve_amount as reserve_amount,ps.show_reverse_not_met as show_reserve_not_met,ps.is_log_time_extension,ps.time_flash,ps.log_time_extension,ps.bid_limit,ls.status_name as auction_status_name,ps.remain_time_to_add_extension, pl.status_id as listing_status_id, pls.status_name as listing_status_name,pl.closing_status_id,plc.status_name as closing_statuss_name,a.dutch_end_time,a.dutch_pause_time, a.dutch_time, a.english_start_time, a.english_time, a.insider_price_decrease,a.sealed_end_time, a.sealed_pause_time, a.sealed_start_time, a.sealed_time, a.insider_decreased_price, ";
							sqlQuery += "(SELECT bid.user_id FROM bid INNER JOIN bid_registration br ON br.user_id = bid.user_id AND br.property_id = bid.property_id WHERE bid.is_retracted = false and bid.auction_id = " + auctionId + " AND br.purchase_forefit_status != 2 ORDER BY bid.bid_amount DESC limit 1) as max_bidder_user_id,";
							sqlQuery += "(SELECT MAX(bid.bid_amount) FROM bid INNER JOIN bid_registration br ON br.user_id = bid.user_id AND br.property_id = bid.property_id WHERE bid.is_retracted = false and bid.property_id = " + propertyId + " AND bid.domain_id=" + domainId + " AND br.purchase_forefit_status != 2 limit 1) as high_bid_amt,";
							sqlQuery += "(SELECT MAX(bid.bid_amount) FROM bid INNER JOIN bid_registration br ON br.user_id = bid.user_id AND br.property_id = bid.property_id WHERE bid.is_retracted = false and bid.property_id = " + propertyId + " AND bid.domain_id=" + domainId + " AND bid.is_canceled = false AND bid.user_id=" + userId + " limit 1) as my_high_bid_amt,";
							// sqlQuery += `(select CONCAT(u.first_name, ' ', u.last_name) FROM users u WHERE u.id =(SELECT b.user_id FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true)) AS bidder_name,`;
							sqlQuery += `(SELECT CONCAT(u.first_name, ' ', u.last_name) FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id INNER JOIN users u ON u.id = b.user_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true LIMIT 1) AS bidder_name,`;
							sqlQuery += "(select show_reverse_not_met from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_reserve_not_met,";
							sqlQuery += "(select is_log_time_extension from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_is_log_time_extension,";
							sqlQuery += "(select time_flash from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_time_flash,";
							sqlQuery += "(select log_time_extension from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_log_time_extension,";
							sqlQuery += "(select remain_time_to_add_extension from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_remain_time_to_add_extension,";
							sqlQuery += "(select bid_limit from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_bid_limit,";
							sqlQuery += "(select autobid from property_settings where domain_id=" + domainId + " and is_broker=true and is_agent=false and status_id=1 order by id desc limit 1)as auto_bid,";
							sqlQuery += "(select autobid_setup from property_settings where domain_id=" + domainId + " and is_broker=true and is_agent=false and status_id=1 order by id desc limit 1)as auto_bid_setup,";
							sqlQuery += "(select auto_bid_amount from auto_bid_amount where user_id=" + userId + " and property_id=" + propertyId + " order by id desc limit 1)as user_auto_bid_amount,";
							sqlQuery += "(select status_id from auto_bid_amount where user_id=" + userId + " and property_id=" + propertyId + " order by id desc limit 1)as user_auto_bid_status,";
							sqlQuery += "(select CASE WHEN is_retracted = true THEN 1 ELSE 0 END from bid where user_id=" + userId + " and property_id=" + propertyId + " order by id desc limit 1)as is_last_bid_retracted,";
							sqlQuery += "(select amount from insider_auction_step_winner where domain_id=" + domainId + " and property_id=" + propertyId + " and status_id=1 and insider_auction_step=1 limit 1)as dutch_winning_amount,";
							sqlQuery += "(select show_reverse_not_met from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_reserve_not_met,";
							sqlQuery += "(select is_log_time_extension from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_is_log_time_extension,";
							sqlQuery += "(select time_flash from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_time_flash,";
							sqlQuery += "(select log_time_extension from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_log_time_extension,";
							sqlQuery += "(select remain_time_to_add_extension from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_remain_time_to_add_extension,";
							sqlQuery += "(select bid_limit from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_bid_limit,";
							sqlQuery += "(select is_retracted from bid where bid.is_retracted = true and user_id = " + userId + " and auction_id = " + auctionId + " ORDER BY bid_amount desc limit 1) as is_bid_retracted,";
							sqlQuery += "(SELECT count(*) from property_listing_buy_now where property_id = " + propertyId + " ) as offers_count, ";
							// sqlQuery += `(SELECT b.user_id FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId}) AS selected_highest_bid_user_id, `;
							// sqlQuery += "(SELECT bid.bid_amount FROM bid INNER JOIN bid_registration br ON br.user_id = bid.user_id AND br.property_id = bid.property_id WHERE bid.is_retracted = false and bid.auction_id = " + auctionId + " AND br.purchase_forefit_status != 2 AND selected_highest_bid = true ORDER BY bid.bid_amount DESC limit 1) as selected_highest_bid,";
							// sqlQuery += `(SELECT EXISTS (SELECT 1 FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId})) AS is_selected_highest_bid, `;
							sqlQuery += `(SELECT CONCAT(u.first_name, ' ', u.last_name) FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id INNER JOIN users u ON u.id = b.user_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true LIMIT 1) AS bidder_name, (SELECT b.user_id FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId} LIMIT 1) AS selected_highest_bid_user_id, `;
							sqlQuery += `(SELECT b.bid_amount FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true ORDER BY b.bid_amount DESC LIMIT 1) AS selected_highest_bid,`;
							sqlQuery += `(SELECT EXISTS (SELECT 1 FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId})) AS is_selected_highest_bid, `;
							sqlQuery += "(select bid_amount from bid where bid.is_retracted = false and user_id = " + userId + " and auction_id = " + auctionId + " ORDER BY bid_amount desc limit 1) as my_max_bid_val, ((extract (epoch from (a.end_date::timestamp - now()::timestamp))::integer)) as time_left_hr, ((extract (epoch from (a.start_date::timestamp - now()::timestamp))::integer)) as start_time_left_hr, a.ending_soon_threshold,a.end_date";
							sqlQuery += " from bid b left join property_auction a on a.id = b.auction_id left Join bid_count_view bv on b.property_id = bv.property_id left join property_listing as pl ON pl.id=a.property_id ";
							sqlQuery += " left join bid_registration br on  b.property_id = br.property_id ";
							sqlQuery += " left join lookup_status ls on  ls.id = a.status_id ";
							sqlQuery += " left join lookup_status pls on  pls.id = pl.status_id ";
							sqlQuery += " left join lookup_status plc on  plc.id = pl.closing_status_id ";
							sqlQuery += " left join property_settings ps on  ps.property_id = a.property_id ";
							sqlQuery += " left join property_listing_buy_now plbn on  plbn.property_id = " + propertyId + " and plbn.buy_now_status = 1 ";
							sqlQuery += " where br.status_id=1 and br.user_id=" + userId;
							sqlQuery += " and bid_type IN('2','3')  and b.auction_id = " + auctionId + " and b.property_id = " + propertyId + " and b.domain_id=" + domainId;
							sqlQuery += " order by  b.bid_amount DESC , bid_date desc limit 1";
							pool.query(sqlQuery, function (err, result) {
								if (err) {
									//check logger wokring
									logger.log("error", 'CHECKBID_2 ERROR QUERY : ' + sqlQuery);
									socket.emit("checkBid", { "msg": err, "error": 1, "status": 400, "type": 5, "res_msg": "bidding started query error", "user_id": userId });
									return 0;
								}
								else {
									if (result?.rowCount > 0) {
										socket.emit("checkBid", { "data": result?.rows[0], "msg": "Fetch data", "status": 201, "error": 0, "type": 1, "user_id": userId });
										socket.in(propertyId + '_' + userId).emit("checkBid", { "data": result?.rows[0], "msg": "Fetch data", "status": 201, "error": 0, "type": 1, "user_id": userId });
									}
									else {
										//when bidding not started yet but user has registered for bid
										let flshQuery = "Select plbn.added_on offerer_offer_date, plbn.buy_now_amount offerer_offer_amount, plbn.buy_now_status offerer_offer_status, plbn.user_id offerer_user_id, pl.id as property_id, a.bid_increments as bid_increments, r.is_approved as is_approved, r.is_reviewed as is_reviewed, a.start_date as start_date, a.end_date as end_date,a.auction_id as auction_type_id,a.start_price,r.user_id,a.status_id as auction_status,a.reserve_amount as reserve_amount,ps.show_reverse_not_met as show_reserve_not_met,ps.is_log_time_extension,ps.time_flash,ps.log_time_extension,ps.bid_limit,ps.remain_time_to_add_extension,pl.status_id as listing_status_id, pls.status_name as listing_status_name,ls.status_name as auction_status_name,pl.closing_status_id,plc.status_name as closing_statuss_name,a.dutch_end_time,a.dutch_pause_time, a.dutch_time, a.english_start_time, a.english_time, a.insider_price_decrease,a.sealed_end_time, a.sealed_pause_time, a.sealed_start_time, a.sealed_time, a.insider_decreased_price,";
										flshQuery += " ((extract (epoch from (a.end_date::timestamp - now()::timestamp))::integer)) as time_left_hr, ((extract (epoch from (a.start_date::timestamp - now()::timestamp))::integer)) as start_time_left_hr, a.ending_soon_threshold, ";
										flshQuery += "(select show_reverse_not_met from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_reserve_not_met,";
										flshQuery += "(select is_log_time_extension from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_is_log_time_extension,";
										flshQuery += "(select time_flash from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_time_flash,";
										flshQuery += "(select log_time_extension from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_log_time_extension,";
										flshQuery += "(select remain_time_to_add_extension from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_remain_time_to_add_extension,";
										flshQuery += "(select bid_limit from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_bid_limit,";
										flshQuery += "(select autobid from property_settings where domain_id=" + domainId + " and is_broker=true and is_agent=false and status_id=1 order by id desc limit 1)as auto_bid,";
										flshQuery += "(select autobid_setup from property_settings where domain_id=" + domainId + " and is_broker=true and is_agent=false and status_id=1 order by id desc limit 1)as auto_bid_setup,";
										flshQuery += "(select auto_bid_amount from auto_bid_amount where user_id=" + userId + " and property_id=" + propertyId + " order by id desc limit 1)as user_auto_bid_amount,";
										flshQuery += "(select status_id from auto_bid_amount where user_id=" + userId + " and property_id=" + propertyId + " order by id desc limit 1)as user_auto_bid_status,";
										flshQuery += "(select CASE WHEN is_retracted = true THEN 1 ELSE 0 END from bid where user_id=" + userId + " and property_id=" + propertyId + " order by id desc limit 1)as is_last_bid_retracted,";
										flshQuery += "(select amount from insider_auction_step_winner where domain_id=" + domainId + " and property_id=" + propertyId + " and status_id=1 and insider_auction_step=1 limit 1)as dutch_winning_amount,";
										flshQuery += "(select show_reverse_not_met from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_reserve_not_met,";
										flshQuery += "(select is_log_time_extension from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_is_log_time_extension,";
										flshQuery += "(select time_flash from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_time_flash,";
										flshQuery += "(select log_time_extension from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_log_time_extension,";
										flshQuery += "(select remain_time_to_add_extension from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_remain_time_to_add_extension,";
										flshQuery += "(SELECT count(*) from property_listing_buy_now where property_id = " + propertyId + " ) as offers_count, ";
										// flshQuery += `(SELECT b.user_id FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId}) AS selected_highest_bid_user_id, `;
										// flshQuery += "(SELECT bid.bid_amount FROM bid INNER JOIN bid_registration br ON br.user_id = bid.user_id AND br.property_id = bid.property_id WHERE bid.is_retracted = false and bid.auction_id = " + auctionId + " AND br.purchase_forefit_status != 2 AND selected_highest_bid = true ORDER BY bid.bid_amount DESC limit 1) as selected_highest_bid,";
										// flshQuery += `(SELECT EXISTS (SELECT 1 FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId})) AS is_selected_highest_bid, `;
										flshQuery += `(SELECT CONCAT(u.first_name, ' ', u.last_name) FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id INNER JOIN users u ON u.id = b.user_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true LIMIT 1) AS bidder_name, (SELECT b.user_id FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId} LIMIT 1) AS selected_highest_bid_user_id, `;
										flshQuery += `(SELECT b.bid_amount FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true ORDER BY b.bid_amount DESC LIMIT 1) AS selected_highest_bid,`;
										flshQuery += `(SELECT EXISTS (SELECT 1 FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId})) AS is_selected_highest_bid, `;
										flshQuery += "(select bid_limit from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_bid_limit ";
										flshQuery += "from property_listing pl join property_auction a on pl.id = a.property_id ";
										flshQuery += " left join lookup_status ls on  ls.id = a.status_id ";
										flshQuery += " left join lookup_status pls on  pls.id = pl.status_id ";
										flshQuery += " left join lookup_status plc on  plc.id = pl.closing_status_id ";
										flshQuery += "left join bid_registration r on pl.id = r.property_id ";
										flshQuery += " left join property_listing_buy_now plbn on  plbn.property_id = " + propertyId + " and plbn.buy_now_status = 1 ";
										flshQuery += " left join property_settings ps on  ps.property_id = a.property_id ";
										flshQuery += "where pl.id=" + propertyId + " and r.status_id=1 and a.id=" + auctionId + " and r.user_id=" + userId + " and pl.domain_id=" + domainId;
										flshQuery += " order by pl.id desc limit 1";
										pool.query(flshQuery, function (err, checkRes) {
											if (err) {
												logger.log("error", 'CHECKBID_3 ERROR QUERY : ' + flshQuery);
												socket.emit("checkBid", { "msg": err, "error": 1, "status": 400, "type": 5, "res_msg": "not started query error", "data": {}, "user_id": userId });
												return 0;
											}
											else {
												if (checkRes?.rowCount > 0) {
													socket.emit("checkBid", { "data": checkRes?.rows[0], "status": 201, "error": 0, "type": 2, "res_msg": "not started", "user_id": userId });
													socket.in(propertyId + '_' + userId).emit("checkBid", { "data": checkRes?.rows[0], "status": 201, "error": 0, "type": 2, "res_msg": "not started", "user_id": userId });
													return 0;
												}
												else {
													socket.emit("checkBid", { "msg": err, "error": 1, "status": 400, "type": 5, "res_msg": "not started invalid request", "user_id": userId });
												}
											}
										});
									}
								}
							});
						} else {
							//user not registered for bid
							let flshQuery = "Select plbn.added_on offerer_offer_date, plbn.buy_now_amount offerer_offer_amount, plbn.buy_now_status offerer_offer_status, plbn.user_id offerer_user_id, pl.id as property_id, a.bid_increments as bid_increments,a.start_date as start_date, a.end_date as end_date,a.start_price, a.id as auction_id, a.auction_id as auction_type_id,bcv.total as bid_count,a.status_id as auction_status,a.reserve_amount as reserve_amount,ps.show_reverse_not_met as show_reserve_not_met,ps.is_log_time_extension,ps.time_flash,ps.log_time_extension,ps.bid_limit,ps.remain_time_to_add_extension,pl.status_id as listing_status_id,pls.status_name as listing_status_name,ls.status_name as auction_status_name,pl.closing_status_id,plc.status_name as closing_statuss_name,a.dutch_end_time,a.dutch_pause_time, a.dutch_time, a.english_start_time, a.english_time, a.insider_price_decrease,a.sealed_end_time, a.sealed_pause_time, a.sealed_start_time, a.sealed_time, a.insider_decreased_price,";
							flshQuery += " ((extract (epoch from (a.end_date::timestamp - now()::timestamp))::integer)) as time_left_hr,";
							flshQuery += "(select show_reverse_not_met from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_reserve_not_met,";
							flshQuery += "(select is_log_time_extension from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_is_log_time_extension,";
							flshQuery += "(select time_flash from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_time_flash,";
							flshQuery += "(select log_time_extension from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_log_time_extension,";
							flshQuery += "(select remain_time_to_add_extension from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_remain_time_to_add_extension,";
							flshQuery += "(select bid_limit from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_bid_limit,";
							flshQuery += "(select autobid from property_settings where domain_id=" + domainId + " and is_broker=true and is_agent=false and status_id=1 order by id desc limit 1)as auto_bid,";
							flshQuery += "(select autobid_setup from property_settings where domain_id=" + domainId + " and is_broker=true and is_agent=false and status_id=1 order by id desc limit 1)as auto_bid_setup,";
							flshQuery += "(select auto_bid_amount from auto_bid_amount where user_id=" + userId + " and property_id=" + propertyId + " order by id desc limit 1)as user_auto_bid_amount,";
							flshQuery += "(select status_id from auto_bid_amount where user_id=" + userId + " and property_id=" + propertyId + " order by id desc limit 1)as user_auto_bid_status,";
							flshQuery += "(select CASE WHEN is_retracted = true THEN 1 ELSE 0 END from bid where user_id=" + userId + " and property_id=" + propertyId + " order by id desc limit 1)as is_last_bid_retracted,";
							flshQuery += "(select show_reverse_not_met from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_reserve_not_met,";
							flshQuery += "(select is_log_time_extension from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_is_log_time_extension,";
							flshQuery += "(select time_flash from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_time_flash,";
							flshQuery += "(select log_time_extension from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_log_time_extension,";
							flshQuery += "(select remain_time_to_add_extension from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_remain_time_to_add_extension,";
							flshQuery += "(select bid_limit from property_settings where domain_id=" + domainId + " and user_id = " + userId + " and is_agent=true limit 1)as agent_bid_limit,";
							flshQuery += "(select amount from insider_auction_step_winner where domain_id=" + domainId + " and property_id=" + propertyId + " and status_id=1 and insider_auction_step=1 limit 1)as dutch_winning_amount,";
							flshQuery += " ((extract (epoch from (a.start_date::timestamp - now()::timestamp))::integer)) as start_time_left_hr,a.ending_soon_threshold,";
							flshQuery += "(SELECT bid.user_id FROM bid INNER JOIN bid_registration br ON br.user_id = bid.user_id AND br.property_id = bid.property_id WHERE bid.is_retracted = false and bid.auction_id = " + auctionId + " AND br.purchase_forefit_status != 2 ORDER BY bid.bid_amount DESC limit 1) as max_bidder_user_id,";
							// flshQuery += `(select CONCAT(u.first_name, ' ', u.last_name) FROM users u WHERE u.id =(SELECT b.user_id FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true)) AS bidder_name,`;
							flshQuery += `(SELECT CONCAT(u.first_name, ' ', u.last_name) FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id INNER JOIN users u ON u.id = b.user_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true LIMIT 1) AS bidder_name,`;
							flshQuery += "(select is_retracted from bid where bid.is_retracted = true and user_id = " + userId + " and auction_id = " + auctionId + " ORDER BY bid_amount desc limit 1) as is_bid_retracted,";
							flshQuery += " (select bid_amount from bid where bid.is_retracted = false and user_id = " + userId + " and auction_id =" + auctionId + "  ORDER BY bid_amount desc limit 1) as my_max_bid_val,";
							flshQuery += "(SELECT count(*) from property_listing_buy_now where property_id = " + propertyId + " ) as offers_count, ";
							// flshQuery += `(SELECT b.user_id FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId}) AS selected_highest_bid_user_id, `;
							// flshQuery += "(SELECT bid.bid_amount FROM bid INNER JOIN bid_registration br ON br.user_id = bid.user_id AND br.property_id = bid.property_id WHERE bid.is_retracted = false and bid.auction_id = " + auctionId + " AND br.purchase_forefit_status != 2 AND selected_highest_bid = true ORDER BY bid.bid_amount DESC limit 1) as selected_highest_bid,";
							// flshQuery += `(SELECT EXISTS (SELECT 1 FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId})) AS is_selected_highest_bid, `;
							flshQuery += `(SELECT CONCAT(u.first_name, ' ', u.last_name) FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id INNER JOIN users u ON u.id = b.user_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true LIMIT 1) AS bidder_name, (SELECT b.user_id FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId} LIMIT 1) AS selected_highest_bid_user_id, `;
							flshQuery += `(SELECT b.bid_amount FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true ORDER BY b.bid_amount DESC LIMIT 1) AS selected_highest_bid,`;
							flshQuery += `(SELECT EXISTS (SELECT 1 FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId})) AS is_selected_highest_bid, `;
							flshQuery += "(SELECT MAX(bid.bid_amount) FROM bid INNER JOIN bid_registration br ON br.user_id = bid.user_id AND br.property_id = bid.property_id WHERE bid.is_retracted = false and bid.property_id = " + propertyId + " AND bid.domain_id=" + domainId + " AND br.purchase_forefit_status != 2 limit 1) as high_bid_amt, ";
							flshQuery += "(SELECT MAX(bid.bid_amount) FROM bid INNER JOIN bid_registration br ON br.user_id = bid.user_id AND br.property_id = bid.property_id WHERE bid.is_retracted = false and bid.property_id = " + propertyId + " AND bid.domain_id=" + domainId + " AND bid.user_id =" + userId + " limit 1) as my_high_bid_amt ";
							flshQuery += " from property_listing pl left join property_auction a on pl.id = a.property_id";
							flshQuery += " left join lookup_status ls on  ls.id = a.status_id ";
							flshQuery += " left join lookup_status pls on  pls.id = pl.status_id ";
							flshQuery += " left join lookup_status plc on  plc.id = pl.closing_status_id ";
							flshQuery += " left join bid_count_view bcv on pl.id = bcv.property_id";
							flshQuery += " left join property_settings ps on  ps.property_id = a.property_id";
							flshQuery += " left join property_listing_buy_now plbn on  plbn.property_id = " + propertyId + " and plbn.buy_now_status = 1 ";
							flshQuery += " where pl.id=" + propertyId + " and a.id=" + auctionId + " and pl.domain_id=" + domainId + " order by pl.id desc limit 1";

							pool.query(flshQuery, function (err, checkRes) {

								if (err) {
									logger.log("error", 'CHECKBID_4 ERROR QUERY : ' + flshQuery);
									socket.emit("checkBid", { "msg": err, "error": 1, "status": 400, "type": 5, "res_msg": "query error", "user_id": userId });
									return 0;
								}
								else {
									if (checkRes?.rowCount > 0) {
										socket.emit("checkBid", { "data": checkRes?.rows[0], "status": 201, "error": 0, "type": 3, "errorcode": 0, "res_msg": "not register", "user_id": userId });
										socket.in(propertyId + '_' + userId).emit("checkBid", { "data": checkRes?.rows[0], "status": 201, "error": 0, "type": 3, "errorcode": 0, "res_msg": "not register", "user_id": userId });
										return 0;
									}
									else {
										socket.emit("checkBid", { "msg": "Invalid request", "error": 1, "status": 403, "type": 5, "res_msg": "not register", "user_id": userId });
									}
								}
							});
						}
					}
				});
			} else {
				//when user is not loggedin
				let flshQuery = "Select plbn.added_on offerer_offer_date, plbn.buy_now_amount offerer_offer_amount, plbn.buy_now_status offerer_offer_status, plbn.user_id offerer_user_id, pl.id as property_id, a.bid_increments as bid_increments,a.start_date as start_date, a.end_date as end_date,a.start_price, a.id as auction_id, a.auction_id as auction_type_id,bcv.total as bid_count,a.status_id as auction_status,a.reserve_amount as reserve_amount,ps.show_reverse_not_met as show_reserve_not_met,ps.is_log_time_extension,ps.time_flash,ps.log_time_extension,ps.bid_limit,ps.remain_time_to_add_extension,pl.status_id as listing_status_id,pls.status_name as listing_status_name,ls.status_name as auction_status_name,pl.closing_status_id,plc.status_name as closing_statuss_name, a.dutch_time, a.english_start_time, a.english_time, a.insider_price_decrease,a.sealed_end_time, a.sealed_pause_time, a.sealed_start_time, a.sealed_time, a.insider_decreased_price,";
				flshQuery += " ((extract (epoch from (a.end_date::timestamp - now()::timestamp))::integer)) as time_left_hr,";
				flshQuery += " ((extract (epoch from (a.start_date::timestamp - now()::timestamp))::integer)) as start_time_left_hr,a.ending_soon_threshold,";
				// flshQuery += `(select CONCAT(u.first_name, ' ', u.last_name) FROM users u WHERE u.id =(SELECT b.user_id FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true)) AS bidder_name,`;
				flshQuery += `(SELECT CONCAT(u.first_name, ' ', u.last_name) FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id INNER JOIN users u ON u.id = b.user_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true LIMIT 1) AS bidder_name,`;
				flshQuery += "(SELECT bid.user_id FROM bid INNER JOIN bid_registration br ON br.user_id = bid.user_id AND br.property_id = bid.property_id WHERE bid.is_retracted = false and bid.auction_id = " + auctionId + " AND br.purchase_forefit_status != 2 ORDER BY bid.bid_amount DESC limit 1) as max_bidder_user_id,";
				flshQuery += "(select show_reverse_not_met from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_reserve_not_met,";
				flshQuery += "(select is_log_time_extension from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_is_log_time_extension,";
				flshQuery += "(select time_flash from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_time_flash,";
				flshQuery += "(select log_time_extension from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_log_time_extension,";
				flshQuery += "(select remain_time_to_add_extension from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_remain_time_to_add_extension,";
				flshQuery += "(select bid_limit from property_settings where domain_id=" + domainId + " and is_broker=true limit 1)as global_bid_limit,";
				flshQuery += "(select autobid from property_settings where domain_id=" + domainId + " and is_broker=true and is_agent=false and status_id=1 order by id desc limit 1)as auto_bid,";
				flshQuery += "(select autobid_setup from property_settings where domain_id=" + domainId + " and is_broker=true and is_agent=false and status_id=1 order by id desc limit 1)as auto_bid_setup,";
				flshQuery += "(select show_reverse_not_met from property_settings where domain_id=" + domainId + " and is_agent=true limit 1)as agent_reserve_not_met,";
				flshQuery += "(select is_log_time_extension from property_settings where domain_id=" + domainId + " and is_agent=true limit 1)as agent_is_log_time_extension,";
				flshQuery += "(select time_flash from property_settings where domain_id=" + domainId + " and is_agent=true limit 1)as agent_time_flash,";
				flshQuery += "(select log_time_extension from property_settings where domain_id=" + domainId + " and is_agent=true limit 1)as agent_log_time_extension,";
				flshQuery += "(select remain_time_to_add_extension from property_settings where domain_id=" + domainId + " and is_agent=true limit 1)as agent_remain_time_to_add_extension,";
				flshQuery += "(select bid_limit from property_settings where domain_id=" + domainId + " and is_agent=true limit 1)as agent_bid_limit,";
				flshQuery += "(select CASE WHEN is_retracted = true THEN 1 ELSE 0 END from bid where user_id=" + userId + " and property_id=" + propertyId + " order by id desc limit 1)as is_last_bid_retracted,";
				flshQuery += "(select amount from insider_auction_step_winner where domain_id=" + domainId + " and property_id=" + propertyId + " and status_id=1 and insider_auction_step=1 limit 1)as dutch_winning_amount,";
				flshQuery += " (select max(bid_amount) from bid where bid.is_retracted = false and user_id = " + userId + " and auction_id =" + auctionId + ") as my_max_bid_val,";
				flshQuery += "(select is_retracted from bid where bid.is_retracted = true and user_id = " + userId + " and auction_id = " + auctionId + ") as is_bid_retracted,";
				flshQuery += "(SELECT count(*) from property_listing_buy_now where property_id = " + propertyId + " ) as offers_count, ";
				// flshQuery += `(SELECT b.user_id FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId}) AS selected_highest_bid_user_id, `;
				// flshQuery += "(SELECT bid.bid_amount FROM bid INNER JOIN bid_registration br ON br.user_id = bid.user_id AND br.property_id = bid.property_id WHERE bid.is_retracted = false and bid.auction_id = " + auctionId + " AND br.purchase_forefit_status != 2 AND selected_highest_bid = true ORDER BY bid.bid_amount DESC limit 1) as selected_highest_bid,";
				// flshQuery += `(SELECT EXISTS (SELECT 1 FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId})) AS is_selected_highest_bid, `;
				flshQuery += `(SELECT CONCAT(u.first_name, ' ', u.last_name) FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id INNER JOIN users u ON u.id = b.user_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true LIMIT 1) AS bidder_name, (SELECT b.user_id FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId} LIMIT 1) AS selected_highest_bid_user_id, `;
				flshQuery += `(SELECT b.bid_amount FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true ORDER BY b.bid_amount DESC LIMIT 1) AS selected_highest_bid,`;
				flshQuery += `(SELECT EXISTS (SELECT 1 FROM bid b INNER JOIN bid_registration br ON br.user_id = b.user_id AND br.property_id = b.property_id WHERE b.is_retracted = false AND b.auction_id = ${auctionId} AND br.purchase_forefit_status != 2 AND b.selected_highest_bid = true AND b.property_id = ${propertyId})) AS is_selected_highest_bid, `;
				flshQuery += "(SELECT MAX(bid.bid_amount) FROM bid INNER JOIN bid_registration br ON br.user_id = bid.user_id AND br.property_id = bid.property_id WHERE bid.is_retracted = false and bid.property_id = " + propertyId + " AND bid.domain_id=" + domainId + " AND br.purchase_forefit_status != 2 limit 1) as high_bid_amt, ";
				flshQuery += "0 as my_high_bid_amt ";
				flshQuery += " from property_listing pl left join property_auction a on pl.id = a.property_id";
				flshQuery += " left join lookup_status ls on  ls.id = a.status_id ";
				flshQuery += " left join lookup_status pls on  pls.id = pl.status_id ";
				flshQuery += " left join lookup_status plc on  plc.id = pl.closing_status_id ";
				flshQuery += " left join bid_count_view bcv on pl.id = bcv.property_id";
				flshQuery += " left join property_settings ps on  ps.property_id = a.property_id ";
				flshQuery += " left join property_listing_buy_now plbn on  plbn.property_id = " + propertyId + " and plbn.buy_now_status = 1 ";
				flshQuery += " where pl.id=" + propertyId + " and a.id=" + auctionId + " and pl.domain_id=" + domainId + " order by pl.id desc limit 1";
				pool.query(flshQuery, function (err, checkRes) {
					if (err) {
						//if error in sql let log query
						logger.log("error", 'CHECKBID_5 ERROR QUERY : ' + flshQuery);
						socket.emit("checkBid", { "msg": err, "error": 1, "status": 400, "type": 5, "res_msg": "query error", "user_id": userId });
						return 0;
					}
					else {

						if (checkRes?.rowCount > 0) {
							socket.emit("checkBid", { "data": checkRes?.rows[0], "status": 201, "error": 0, "type": 4, "res_msg": "not loggedin", "user_id": userId });
							socket.in(propertyId + '_' + userId).emit("checkBid", { "data": checkRes?.rows[0], "status": 201, "error": 0, "type": 4, "res_msg": "not loggedin", "user_id": userId });
							return 0;
						} else {
							socket.in(propertyId + '_' + userId).emit("checkBid", { "data": {}, "status": 201, "error": 1, "type": 4, "res_msg": "not loggedin", "user_id": userId });
						}
					}
				});
			}
		} catch (err) {
			logger.log("error", `CHECKBID_6 ERROR: ${err.stack}`);
			return 0;
		}
	},

	checkMyBid: function checkMyBid(socket, data) {
		//first we check if user id , property id, auction id and domain id exist in parameter
		try {
			if (data.property_id > 0 && data.user_id > 0 && data.auction_id > 0 && data.domain_id > 0) {
				let chkQry = "select id from property_listing where id = " + data.property_id + " and domain_id=" + parseInt(data.domain_id);
				pool.query(chkQry, function (err, checkRes) {
					if (err) {
						//if error in sql let log query
						logger.log("error", 'ERROR QUERY : ' + chkQry);
						socket.emit("checkMyBid", { "msg": err, "status": 400, "error": 1 });
						return 0;
					}
					else {
						if (checkRes.rowCount > 0) {
							const userId = data?.user_id?.trim?.() === '' ? null : data?.user_id;

							//This will check bid records and keep bid value in sync it also take time remain and threshold value
							let sqlQuery = "select b.property_id as  property_id, b.user_id  as user_id, total as bid_count, br.is_reviewed as is_reviewed, br.is_approved as is_approved, a.bid_increments as bid_increments, a.start_date as start_date, a.end_date as end_date, a.start_price as start_price, a.status_id as auction_status, pl.winner_id as winner_id, pl.status_id as property_status, ls.status_name as property_status_name, pl.sale_by_type_id as sale_by_type_id, pl.closing_status_id as closing_status, plc.status_name as closing_status_name, ";
							sqlQuery += "(SELECT MAX(bid.bid_amount) FROM bid INNER JOIN bid_registration br ON br.user_id = bid.user_id AND br.property_id = bid.property_id WHERE bid.is_retracted = false and bid.property_id = " + data.property_id + " AND bid.domain_id=" + data.domain_id + " AND br.purchase_forefit_status != 2 limit 1) as high_bid_amt,";
							flshQuery += "(select is_retracted from bid where bid.is_retracted = true and user_id = " + userId + " and auction_id = " + data.auction_id + " ORDER BY bid_amount desc limit 1) as is_bid_retracted,";
							sqlQuery += "(select max(bid_amount) from bid where bid.is_retracted = false and user_id = " + userId + " and auction_id = " + data.auction_id + ") as my_max_bid_val, ((extract (epoch from (a.end_date::timestamp - now()::timestamp))::integer)) as time_left_hr, ((extract (epoch from (a.start_date::timestamp - now()::timestamp))::integer)) as start_time_left_hr, a.ending_soon_threshold,end_date";
							sqlQuery += " from bid b join property_auction a on a.id = b.auction_id Join bid_count_view bv on b.property_id = bv.property_id";
							sqlQuery += " join bid_registration br on  b.property_id = br.property_id ";
							sqlQuery += " join property_listing pl on  b.property_id = pl.id ";
							sqlQuery += " join lookup_status ls on pl.status_id = ls.id ";
							sqlQuery += " left join lookup_status plc on  plc.id = pl.closing_status_id ";
							sqlQuery += " where a.end_date >= now() and br.status_id=1 and br.user_id=" + data.user_id;
							sqlQuery += " and bid_type IN('2','3')  and b.auction_id = " + data.auction_id + " and b.property_id = " + data.property_id + " and b.domain_id=" + data.domain_id;
							sqlQuery += " order by  b.bid_amount DESC , bid_date desc limit 1";
							pool.query(sqlQuery, function (err, result) {
								if (err) {
									//check logger wokring
									logger.log("error", 'ERROR QUERY : ' + sqlQuery);
									socket.emit("checkMyBid", { "msg": err, "error": 1, "status": 400 });
									return 0;
								}
								else {

									if (result.rowCount > 0) {
										// console.log(result.rows) //result value
										socket.emit("checkMyBid", { "data": result.rows[0], "msg": "Fetch data", "status": 201, "error": 0 });
										socket.in(data.property_id + '_' + data.user_id).emit("checkMyBid", { "data": result.rows[0], "msg": "Fetch data", "status": 201, "error": 0 });
									}
									else {
										const userId = data?.user_id?.trim?.() === '' ? null : data?.user_id;
										let flshQuery = "Select pl.id as property_id, a.bid_increments as bid_increments, r.is_approved as is_approved, r.is_reviewed as is_reviewed, a.start_date as start_date, a.end_date as end_date, a.start_price as start_price, total as bid_count, a.status_id as auction_status, pl.winner_id as winner_id, pl.status_id as property_status, ls.status_name as property_status_name, pl.closing_status_id as closing_status, cls.status_name as closing_status_name, pl.sale_by_type_id as sale_by_type_id, ";
										flshQuery += "(select is_retracted from bid where bid.is_retracted = true and user_id = " + userId + " and auction_id = " + data.auction_id + " ORDER BY bid_amount desc limit 1) as is_bid_retracted,";
										flshQuery += " (select max(bid_amount) from bid where bid.is_retracted = false and user_id = " + userId + " and auction_id = " + data.auction_id + ") as my_max_bid_val, ";
										flshQuery += " ((extract (epoch from (a.end_date::timestamp - now()::timestamp))::integer)) as time_left_hr, ((extract (epoch from (a.start_date::timestamp - now()::timestamp))::integer)) as start_time_left_hr, a.ending_soon_threshold ";
										flshQuery += "from property_listing pl join property_auction a on pl.id = a.property_id ";
										flshQuery += "left join bid_registration r on pl.id = r.property_id ";
										flshQuery += "left join lookup_status ls on pl.status_id = ls.id ";
										flshQuery += " left join lookup_status cls on pl.closing_status_id = cls.id ";
										flshQuery += "left join bid_count_view bv on pl.id = bv.property_id "
										flshQuery += "where pl.id=" + data.property_id + " and a.id=" + data.auction_id + " and r.status_id=1 and r.user_id=" + data.user_id + " and pl.domain_id=" + data.domain_id;
										flshQuery += " order by pl.id desc limit 1;";
										pool.query(flshQuery, function (err, checkRes) {
											if (err) {
												logger.log("error", 'ERROR QUERY : ' + flshQuery);
												socket.emit("checkMyBid", { "msg": err, "status": 400, "error": 1 });
												return 0;

											}
											else {
												if (checkRes.rowCount > 0) {
													socket.emit("checkMyBid", { "data": checkRes.rows[0], "status": 201, "error": 0 });
													socket.in(data.property_id + '_' + data.user_id).emit("checkMyBid", { "data": checkRes.rows[0], "status": 201, "error": 0 });
													//socket.broadcast.emit("checkMyBid", {"data": checkRes.rows[0], "status": 201, "error": 0});
													return 0;
												}
												else {
													socket.emit("checkMyBid", { 'msg': 'Invalid request.', 'status': 400, "error": 1 });
												}
											}
										});
									}
								}
							});

						}
						else {
							socket.emit("checkMyBid", { 'msg': 'Invalid request.', 'status': 400, "error": 1 });
						}
					}
				});	//Auth query end
			}
			else {
				socket.emit("checkMyBid", { 'msg': 'Invalid request.', 'status': 403, "error": 1 });
			}
		} catch (err) {
			logger.log("msg", 'ERROR : ' + err.message);
			return 0;
		}
	},

	addBid: async function addBid(socket, data) {
		if (data.property_id && data.user_id && data.auction_id && data.bid_amount && data.domain_id) {
			try {
				// lets check if bid is already started
				let startDateQuery = "select id from property_auction where property_id = " + data.property_id + " and domain_id = " + data.domain_id + " and extract(epoch from ( start_date::timestamp -  now()::timestamp)) > 0;";
				pool.query(startDateQuery, function (err, startDateRes) {
					if (err) {
						logger.log("error", 'ERROR QUERY : ' + startDateQuery);
						socket.emit("addBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
						return 0;
					}
					else {
						if (startDateRes.rowCount > 0) {
							socket.emit("addBid", { "status": 201, "msg": "Auction not started.", "error": 1 });
							return 0;
						}
					}
				});

				// lets check if bid is ended
				startDateQuery = "select id from property_auction where property_id = " + data.property_id + " and domain_id = " + data.domain_id + " and extract(epoch from ( end_date::timestamp -  now()::timestamp)) < 0;";
				pool.query(startDateQuery, function (err, startDateRes) {
					if (err) {
						logger.log("error", 'ERROR QUERY here : ' + startDateQuery);
						socket.emit("addBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
						return 0;
					}
					else {
						if (startDateRes.rowCount > 0) {
							socket.emit("addBid", { "status": 201, "msg": "Auction ended.", "error": 1 });
							return 0;
						}
					}
				});

				//Now authenticate user against bid registration
				let chkQry = "select id from bid_registration where property_id = " + data.property_id + " and status_id = 1 and user_id =" + data.user_id;
				// console.log(chkQry);
				pool.query(chkQry, function (err, checkRes) {

					if (err) {
						logger.log("error", 'ERROR QUERY : ' + chkQry);
						socket.emit("addBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
						return 0;
					}
					else {
						if (checkRes.rowCount > 0) {
							//user found lets proceed
							const minBidAmount = parseInt(data.min_bid_amount);
							const newBidAmount = parseInt(data.bid_amount);
							const bidIncrement = parseInt(data.bid_increment);
							let newBidderId = parseInt(data.user_id);
							let exitingHighBidderId = 0;
							let existingHighBid = 0; //let get this value from db
							let existingProxyAmount = 0; // let get this too from db
							let startingBid = 0; // let get this too from db
							let reservePrice = 0; // let get this too from db
							let currentHighBid = 0;
							let messageStatus = 0;
							let maxBidAmount = 0;
							let curDate = new Date().toLocaleDateString();
							let curTime = new Date();
							let sndTime = curTime.toLocaleString('en-US', { hour: 'numeric', minute: 'numeric', hour12: true });

							//Lets check if bid amount is correct
							if (minBidAmount > newBidAmount) {
								socket.emit("addBid", { "status": 400, "msg": "Please increase your bid.", "error": 1 });
								return 0;
							}
							// bid amount must be divisible by bid increment value
							if (newBidAmount % bidIncrement > 0) {
								socket.emit("addBid", { "status": 400, "msg": "Bid amount must be divisible by bid increment value.", "error": 1 });
								return 0;
							}

							//Lets get bid extension threshold and period from settings.
							// Lets check if bid is still live , time remains and amount is ok and not initial bid
							let bidQl = "select b.bid_amount,a.status_id,a.reserve_amount,a.end_date,vma.max  as max_amount, b.user_id  as max_bider_id, ";
							bidQl += "(select user_id from bid_max_amount am where am.auction_id = b.auction_id ORDER BY max_amount DESC, id ASC limit 1)as user_max_id,a.bid_extension_time_period ,a.bid_extension_amount,(extract (epoch from (end_date::timestamp - now()::timestamp))::integer)/(60) as time_left_min, agent_id as owner_id, ceil(((extract (epoch from (a.end_date::timestamp - now()::timestamp))))/(60*60*24)) as days_left ";
							bidQl += " from bid b LEFT JOIN property_auction a on  b.auction_id = a.id LEFT JOIN view_max_bid_amount vma ON vma.auction_id = b.auction_id ";
							bidQl += " JOIN property_listing l on a.property_id = l.id ";
							bidQl += " Where a.end_date > now() AND a.status_id = 1 AND b.bid_type in('2','3') AND b.property_id = " + data.property_id + " AND b.auction_id = " + data.auction_id + " order by b.bid_amount desc,  b.id desc limit 1"
							pool.query(bidQl, function (err, checkBidRec) {
								if (err) {
									//if error in sql let log query
									// pool.end(() => { });
									logger.log("error", 'ERROR QUERY : ' + bidQl);
									socket.emit("addBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
									return 0;
								}
								else {
									// bid found in table and bid status and time remains authentication only.
									if (checkBidRec.rowCount > 0) {
										let bidRes = checkBidRec.rows;
										existingHighBid = parseInt(bidRes[0].bid_amount);
										existingProxyAmount = (bidRes[0].max_amount) ? parseInt(bidRes[0].max_amount) : 0;
										reservePrice = parseInt(bidRes[0].reserve_amount);
										/* if no proxy set then max bidder id remain high bidder id */
										exitingHighBidderId = (bidRes[0].user_max_id) ? parseInt(bidRes[0].user_max_id) : bidRes[0].max_bider_id;

										/** Lets check if time is about to expire lets update time **/
										let timeLeft = parseInt(bidRes[0].time_left_min);
										let extensionPeriod = parseInt(bidRes[0].bid_extension_time_period);
										let extensionAmt = parseInt(bidRes[0].bid_extension_amount);
										let ownerId = parseInt(bidRes[0].owner_id);
										let daysLeft = parseInt(bidRes[0].days_left);
										let maxBidderId = parseInt(bidRes[0].max_bider_id);


										if (timeLeft <= extensionPeriod && extensionAmt > 0 && parseInt(data.auction_id) > 0) {
											let extensionQry = "UPDATE property_auction SET end_date = (end_date::timestamp + (" + extensionAmt + " ||' minutes')::interval) WHERE id = " + data.auction_id;
											executeQuery(extensionQry);
										}
										/* time extension query end here  */
										/** New bid amount must be greater than existing high bid**/
										if (existingHighBid < newBidAmount) {
											if (newBidAmount < reservePrice) { // You are max bidder without reserve mat
												//new bid is less than reserve lets use full amount no min bid
												bidAmount = newBidAmount;
												maxBidAmount = 0;
												messageStatus = 1;
											}
											else if (newBidAmount >= reservePrice && existingHighBid < reservePrice) { // You are max bidder with reserve mat
												// This could be possible first bid met reserve
												bidAmount = reservePrice;	// set bid upto reserve
												maxBidAmount = newBidAmount; // set bid amount to increse proxy
												messageStatus = 2; // reserve met high bidder
											}
											else {  // Already max bidder set
												// bidding and proxy logic after reserve met
												if (existingHighBid === existingProxyAmount && exitingHighBidderId != newBidderId) {
													bidAmount = existingHighBid + bidIncrement;
													maxBidAmount = newBidAmount;
													messageStatus = 2; // reserve met high bidder
												}
												else {
													/** Let log high proxy amount as newBidAmount */
													let maxAmtQry = "INSERT INTO bid_max_amount(max_amount, date_added, auction_id, user_id) ";
													maxAmtQry += " VALUES(" + newBidAmount + ", now(), " + data.auction_id + ", " + data.user_id + ") ";
													executeQuery(maxAmtQry);
													/** User is high bidder and reserve already met then no need to bid for him only increase proxy*/
													if (exitingHighBidderId === newBidderId) {
														socket.emit("addBid", { "msg": "You are the high bidder.", "status": 201, "error": 0 });
														//As no bid placed in but only incresed proxy no email send
														return 0;
													}

													if (newBidAmount === existingProxyAmount && exitingHighBidderId != newBidderId) { // New bid and max bid will same here
														// if proxy already set for bid amount New bid amount is audit only bid perhaps
														if (existingProxyAmount - existingHighBid === bidIncrement) {
															//if one time higher
															bidAmount = existingProxyAmount;
															maxBidAmount = 0;
															messageStatus = 3; //audit only no high bidder
															// Proxy bid with proxy amount audit only
															let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
															bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + exitingHighBidderId + ", '1', '0', " + data.domain_id + ")";

															executeQuery(bidAddQl);
															profileId = bidRes[0].user_max_id;

															//Proxy bid high bid
															bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
															bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + profileId + ", '2', '0', " + data.domain_id + ")";
															executeQuery(bidAddQl);
														}
														else if ((existingProxyAmount - existingHighBid) / bidIncrement === 2) {

															//if two increment higher bid value here we make two entry
															/** this bid for new user who lost high bid manual */
															bidAmount = existingHighBid + bidIncrement;
															messageStatus = 4; //no high bidder by proxy
															maxBidAmount = 0;
															// auto bid status 3
															let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
															bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + data.user_id + ", '3', '0', " + data.domain_id + ")";

															executeQuery(bidAddQl);

															/** add another bid for proxy user as proxy*/
															bidAmount = existingProxyAmount;
															bidAddQl = "";
															// manual bid status 2
															bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
															bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + exitingHighBidderId + ", '2', '0', " + data.domain_id + ")";
															executeQuery(bidAddQl);
														}
														else {
															/** Bid amount more than 3 increment */
															/** this bid for new user who start it */
															bidAmount = existingHighBid + bidIncrement;
															messageStatus = 4; //no high bidder by proxy
															maxBidAmount = 0;
															// auto bid status 3
															let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
															bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + data.user_id + ", '3', '0', " + data.domain_id + ")";

															executeQuery(bidAddQl);

															/** this bid for new user who lost war */
															bidAmount = existingProxyAmount - bidIncrement;
															// auto bid status 3
															bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
															bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + data.user_id + ", '3', '0', " + data.domain_id + ")";

															executeQuery(bidAddQl);

															/** add another bid for proxy user as proxy who won it */
															bidAmount = existingProxyAmount;
															bidAddQl = "";
															// manual bid status 2
															bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
															bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + exitingHighBidderId + ", '2', '0', " + data.domain_id + ")";
															executeQuery(bidAddQl);
														}
													}
													else if (newBidAmount < existingProxyAmount && exitingHighBidderId != newBidderId) {
														bidAmount = existingHighBid + bidIncrement;
														messageStatus = 4; //no high bidder by proxy
														maxBidAmount = 0;
														// auto bid status 3
														let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
														bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + data.user_id + ", '2', '0', " + data.domain_id + ")";

														executeQuery(bidAddQl);

														if ((newBidAmount - existingHighBid) / bidIncrement > 1) {
															bidAmount = newBidAmount;
															bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
															bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + data.user_id + ", '3', '0', " + data.domain_id + ")";

															executeQuery(bidAddQl);
														}

														bidAmount = newBidAmount + bidIncrement;
														bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
														bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + exitingHighBidderId + ", '3', '0', " + data.domain_id + ")";

														executeQuery(bidAddQl);
													}
													else if (newBidAmount > existingProxyAmount && exitingHighBidderId != newBidderId) {
														/** High bidder case as his proxy is more than any other user*/
														messageStatus = 5; // high bidder by proxy
														if ((existingProxyAmount - existingHighBid) === bidIncrement) {
															/** if new bid amount more than proxy and proxy is one increment of high bid */
															bidAmount = existingProxyAmount;
															let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
															bidAddQl += "VALUES(now()," + bidAmount + ",'1','1', '0'," + data.property_id + "," + data.auction_id + "," + exitingHighBidderId + ", '3', '0', " + data.domain_id + ")";

															executeQuery(bidAddQl);

															/** add existing proxy + 1 increment */
															bidAmount = existingProxyAmount + bidIncrement;
															bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
															bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + data.user_id + ", '2', '0', " + data.domain_id + ")";

															executeQuery(bidAddQl);
														}
														else if ((existingProxyAmount - existingHighBid) / bidIncrement >= 2) {
															/** first bid for new user manual bid*/
															messageStatus = 5; // high bidder by proxy
															bidAmount = existingHighBid + bidIncrement;
															let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
															bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + data.user_id + ", '2', '0', " + data.domain_id + ")";
															executeQuery(bidAddQl);
															
															/** second bid for existing max proxy auto*/
															bidAmount = existingProxyAmount;
															bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
															bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + exitingHighBidderId + ", '3', '0', " + data.domain_id + ")";
															executeQuery(bidAddQl);
															/** Lets notify user he is outbid by proxy */
															/** last bid for new user max proxy */
															bidAmount = existingProxyAmount + bidIncrement;
															bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
															bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + data.user_id + ", '3', '0', " + data.domain_id + ")";
															executeQuery(bidAddQl);
														}
														else if ((newBidAmount - existingHighBid) / bidIncrement >= 1) {
															/* if proxy yet to set and new bid amount == highbid + increment or more
															 Place only one bid, as max bid already set lets not set it again
															*/
															bidAmount = existingHighBid + bidIncrement;
															maxBidAmount = 0;
															messageStatus = 2; // reserve met high bidder
														}
													}
												}
											}

											/** Query for bidding need single query */
											if (messageStatus === 1 || messageStatus === 2) {
												bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
												bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + data.user_id + ", '2', '0', " + data.domain_id + ")";
												executeQuery(bidAddQl);

												// If new proxy need to set lets do the same
												if (parseInt(maxBidAmount) > 0) {
													maxAmtQry = "INSERT INTO bid_max_amount(max_amount, date_added, auction_id, user_id) ";
													maxAmtQry += " VALUES(" + maxBidAmount + ", now(), " + data.auction_id + ", " + data.user_id + ") ";
													executeQuery(maxAmtQry);
												}

												// All is well lets emmit message , send notification if any ...
												if (messageStatus == 1) {
													socket.emit("addBid", { "msg": "You are the high bidder but the reserve has not yet been met.", "status": 201, "error": 0 });
													//message to owner on receive a bid below reserve
													createSendMessage(29, ownerId, data.listingId, bidAmount, daysLeft);
												}
												else if (messageStatus == 2) {
													socket.emit("addBid", { "msg": "You are the high bidder.", "status": 201, "error": 0 });
													// Notify seller on first bid after reserve met
													createSendMessage(41, ownerId, data.listingId, daysLeft);
												}

												// if old max bid amount user is not same then notify outbid
												if (maxBidderId != data.user_id) {
													createSendMessage(3, maxBidderId, data.listingId);
												}

												//Notify buyer
												createSendMessage(6, data.profileId, data.listingId, bidAmount, curDate, sndTime);
												return true;
											}

											if (messageStatus === 4 || messageStatus === 3) {
												socket.emit("addBid", { "msg": "Your bid was successfully placed but you are not the current high bidder.", "status": 201, "error": 0 });
												//Bidder outbid by another user
												createSendMessage(3, data.profileId, data.listingId);
											}
											else if (messageStatus === 5) {
												socket.emit("addBid", { "msg": "You are the high bidder.", "status": 201, "error": 0 });
												/* if old user id is not same lets notify user for outbid */
												if (parseInt(data.user_id) != exitingHighBidderId) {
													createSendMessage(3, exitingHighBidderId, data.listingId);
												}

												//Nofity bidder on bid
												createSendMessage(6, data.profileId, data.listingId, bidAmount, curDate, sndTime);
											}
											return 0;
										}
										else {
											//return error similar bid amount already placed
											socket.emit("addBid", { "msg": "Another bidder has already placed the same bid just before you. Please increase your bid.", "status": 400, "error": 1 });
										}
									}
									else {
										/** no bid found lets check if there are no bid exist in table and date and status is ok*/
										let noBidSql = "select a.status_id AS auction_status_id,a.reserve_amount AS reserve_prie,a.end_date,b.bid_amount, agent_id as owner_id,";
										noBidSql += "ceil(((extract (epoch from (a.end_date::timestamp - now()::timestamp))))/(60*60*24)) as days_left ";
										noBidSql += " from property_auction a LEFT JOIN bid b on  b.auction_id = a.id ";
										noBidSql += " JOIN property_listing l on a.property_id = l.id ";
										noBidSql += " Where a.end_date > now() ";
										noBidSql += " AND a.status_id = 1 AND a.property_id = " + data.property_id + "  AND a.id =" + data.auction_id
										noBidSql += " order by b.bid_amount desc limit 1";

										pool.query(noBidSql, function (err, noBidRes) {
											if (err) {
												logger.log("error", 'ERROR QUERY : ' + noBidSql);
												socket.emit("addBid", { "message": "An error occurred that prevented your bid from being placed. Please try again.", "status": 500 });
												return 0;
											}
											else {
												maxBidAmount = 0;
												if (noBidRes.rowCount > 0) {
													let noBidResRec = noBidRes.rows;
													let bidAmount = parseInt(data.bid_amount);
													reservePrice = parseInt(noBidResRec[0].reserve_prie)
													maxBidAmount = parseInt(noBidResRec[0].bid_amount)
													//lets place first bid
													//if bid amount is more than reserve bid for only reserve amount
													if (bidAmount > reservePrice) {
														maxBidAmount = bidAmount;
														bidAmount = reservePrice;
													}

													let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id)";
													bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + data.user_id + ", '2', '0', " + data.domain_id + ")";

													pool.query(bidAddQl, function (err, addBidRes) {
														if (err) {
															logger.log("error", 'ERROR QUERY : ' + bidAddQl);
															socket.emit("addBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
															return 0;
														}
													});
													//lets check if bid amount is more than reserve set proxy
													if (maxBidAmount > 0) {
														let maxAmtQry = "INSERT INTO bid_max_amount(max_amount, date_added, auction_id, user_id) ";
														maxAmtQry += " VALUES(" + maxBidAmount + ", now(), " + data.auction_id + ", " + data.user_id + ") ";
														//set max bid amount
														pool.query(maxAmtQry, function (err, maxBidRes) {
															if (err) {
																logger.log("error", 'ERROR QUERY : ' + maxAmtQry);
																socket.emit("addBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																return 0;
															}
														});
													}

													let ownerId = parseInt(noBidResRec[0].owner_id);
													let daysLeft = parseInt(noBidResRec[0].days_left);
													//As this is first bid emmit high bidder message if reserve not met
													if (bidAmount < reservePrice) {
														// bid placed but reserve not met
														socket.emit("addBid", { "msg": "You are the high bidder but the reserve has not yet been met.", "status": 201, "error": 0 });
														//message to owner on receive a bid below reserve
														createSendMessage(29, ownerId, data.listingId, bidAmount, daysLeft);
													}
													else {
														socket.emit("addBid", { "msg": "Your $" + convertToDisplayFormat(bidAmount) + " bid was successfully placed, You are the high bidder.", "status": 201, "error": 0 });
														// Seller email first bid at/over reserve met  template id  41
														createSendMessage(41, ownerId, data.listingId, daysLeft);
													}

													// buyer placed a bid successfully
													createSendMessage(6, data.profileId, data.listingId, bidAmount, curDate, sndTime);
												}
												else {
													//either bid is not in active status or data is over lets emmit error
													socket.emit("addBid", { "msg": "The listing has already ended because either the listing end date has been reached or another user has won the auction.", "status": 400, "error": 1 });
												}
											}
										});
									}
								}
							});
						}//user validation
					}					
				});
			}
			catch (err) {
				//trapped error
				logger.log("error", 'ERROR : ' + err.message);
				socket.emit("bidHistory", { "message": err.message, "status": 500 });
				return 0;
			}
		}
		else {
			socket.emit("addBid", { 'message': 'Invalid request.', 'status': 403 });
		}
	},

	addNewBid: async function addBid(socket, data) {
		try {
			let propertyId = data?.property_id || null;
			let userId = data?.user_id || null;
			let auctionId = data?.auction_id || null;
			let bidAmount = data?.bid_amount || null;
			let domainId = data?.domain_id || null;
			if (propertyId && userId && auctionId && bidAmount && domainId) {
				// lets check if property already sold
				let propertyQuery = "select id from property_listing where id = " + propertyId + " and domain_id = " + domainId + " and status_id=1;";
				pool.query(propertyQuery, function (err, startDateQueryRes) {
					if (err) {
						logger.log("error", 'ADDNEW_BID_1 ERROR QUERY : ' + propertyQuery);
						socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
						return 0;
					}
					else {
						if (startDateQueryRes?.rowCount < 1) {
							socket.emit("addNewBid", { "status": 400, "msg": "Property sold.", "error": 1 });
							return 0;
						}
					}
				});

				// lets check if bid is already started
				let startDateQuery = "select id from property_auction where property_id = " + propertyId + " and domain_id = " + domainId + " and status_id=1 and extract(epoch from ( start_date::timestamp -  now()::timestamp)) > 0;";
				pool.query(startDateQuery, function (err, startDateRes) {
					if (err) {
						logger.log("error", 'ADDNEW_BID_2 ERROR QUERY : ' + startDateQuery);
						socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
						return 0;
					}
					else {
						if (startDateRes?.rowCount > 0) {
							socket.emit("addNewBid", { "status": 201, "msg": "Auction not started.", "error": 1 });
							return 0;
						}
					}
				});
				// lets check if bid is ended
				startDateQuery = "select id from property_auction where property_id = " + propertyId + " and domain_id = " + domainId + " and extract(epoch from ( end_date::timestamp -  now()::timestamp)) < 0;";
				pool.query(startDateQuery, function (err, startDateRes) {
					if (err) {
						logger.log("error", 'ADDNEW_BID_3 ERROR QUERY here : ' + startDateQuery);
						socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
						return 0;
					}
					else {
						if (startDateRes?.rowCount > 0) {
							socket.emit("addNewBid", { "status": 201, "msg": "Auction ended.", "error": 1 });
							return 0;
						}
					}
				});

				const checkPaymentStatusQuery = `
					SELECT bt.status_id 
					FROM bid_registration br 
					LEFT JOIN bid_transaction bt ON br.transaction_id = bt.id 
					WHERE br.property_id = ${propertyId} 
						AND br.user_id = ${userId} 
						AND bt.status_id = 34 
						AND bt.gateway_status = 'APPROVED'
					`;

				const paymentStatus = await pool.query(checkPaymentStatusQuery);
			
				if (paymentStatus?.rowCount > 0) {
					const updateQuery = `
					  UPDATE bid_registration 
					  SET is_approved = 2 
					  WHERE property_id = ${propertyId} AND user_id = ${userId}
					`;
			  
					await pool.query(updateQuery);
				}

				//Now authenticate user against bid registration
				let chkQry = "select br.id as id, approval_limit ";
				chkQry += "from bid_limit bl join bid_registration br on bl.registration_id = br.id ";
				chkQry += "where br.user_id = " + userId + " and br.property_id = " + propertyId + " and br.domain_id = " + domainId + " and br.status_id=1 and br.is_approved=2 and br.is_reviewed=true and bl.is_approved=2 and bl.status_id=1 "
				chkQry += "order by bl.id desc limit 1;";
				pool.query(chkQry, function (err, checkRes) {
					if (err) {
						logger.log("error", 'ADDNEW_BID_4 ERROR QUERY : ' + chkQry);
						socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
						return 0;
					}
					else {
						if (checkRes?.rowCount > 0) {
							var registration_detail = checkRes?.rows[0];
							if (parseInt(registration_detail?.approval_limit) < parseInt(bidAmount)) {
								socket.emit("addNewBid", { "msg": "Your approval limit has been reached.<br/> Please contact to admin.", "status": 400, "error": 1 });
								return 0;
							}
							let startDateQuery = "select id, start_price, bid_increments, reserve_amount, bid_extension_time_period, bid_extension_amount, CEIL((extract (epoch from (end_date::timestamp - now()::timestamp))::float)/(60)) as time_left_min, end_date, ";
							startDateQuery += "(select agent_id from property_listing where id=" + propertyId + " order by id desc limit 1) as listing_agent_id ";
							startDateQuery += "from property_auction where property_id = " + propertyId + " order by id desc limit 1";

							pool.query(startDateQuery, function (err, propertyAuction) {
								if (err) {
									logger.log("error", 'ADDNEW_BID_5 ERROR QUERY : ' + startDateQuery);
									socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
									return 0;
								}
								else {
									if (propertyAuction?.rowCount > 0) {
										var auction_data = propertyAuction?.rows[0];
										const listing_agent_id = parseInt(auction_data?.listing_agent_id);
										const minBidAmount = parseInt(auction_data?.start_price);
										const newBidAmount = parseInt(data?.bid_amount);
										const bidAmount = parseInt(data?.bid_amount);
										const bidIncrement = parseInt(auction_data?.bid_increments);
										const auction_id = parseInt(auction_data?.id);
										const reserve_amount = parseInt(auction_data?.reserve_amount);
										const timeLeft = auction_data?.time_left_min;
										const extensionPeriod = auction_data?.bid_extension_time_period;
										const extensionAmt = auction_data?.bid_extension_amount;
										const ip_address = data?.ip_address;
										let newBidderId = parseInt(userId);
										let curDate = new Date().toLocaleDateString();
										let curTime = new Date();
										let sndTime = curTime.toLocaleString('en-US', { hour: 'numeric', minute: 'numeric', hour12: true });
										//Lets check if bid amount is correct
										if (minBidAmount > newBidAmount) {
											socket.emit("addNewBid", { "status": 400, "msg": "Please increase your bid.", "error": 1 });
											return 0;
										}
										var reserveMet = ""
										if (reserve_amount <= newBidAmount) {
											reserveMet = "yes"
										}

										//Lets get bid extension threshold and period from settings.
										// Lets check if bid is still live , time remains and amount is ok and not initial bid
										let bidQl = "select id, bid_amount from bid where property_id = " + propertyId + " and is_canceled = false and is_retracted = false and bid_type IN('2','3') order by id desc limit 1;";
										pool.query(bidQl, function (err, checkBidRec) {
											if (err) {
												// pool.end(() => { });
												logger.log("error", 'ADDNEW_BID_6 ERROR QUERY : ' + bidQl);
												socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
												return 0;
											} else {
												timeExtQry = "select l.id as property_id, ";
												timeExtQry += "(select id from property_settings where property_id=" + propertyId + " and is_broker=false and is_agent=false order by id desc limit 1) as personal_id, ";
												timeExtQry += "(select is_log_time_extension from property_settings where property_id=" + propertyId + " and is_broker=false and is_agent=false order by id desc limit 1) as personal_is_log_time_extension, ";
												timeExtQry += "(select remain_time_to_add_extension from property_settings where property_id=" + propertyId + " and is_broker=false and is_agent=false order by id desc limit 1) as personal_remain_time_to_add_extension, ";
												timeExtQry += "(select log_time_extension from property_settings where property_id=" + propertyId + " and is_broker=false and is_agent=false order by id desc limit 1) as personal_log_time_extension, ";
												timeExtQry += "(select id from property_settings where domain_id=" + domainId + " and user_id=" + listing_agent_id + " and is_agent=true order by id desc limit 1) as agent__id, ";
												timeExtQry += "(select is_log_time_extension from property_settings where domain_id=" + domainId + " and user_id=" + listing_agent_id + " and is_agent=true order by id desc limit 1) as agent_is_log_time_extension, ";
												timeExtQry += "(select remain_time_to_add_extension from property_settings where domain_id=" + domainId + " and user_id=" + listing_agent_id + " and is_agent=true order by id desc limit 1) as agent_remain_time_to_add_extension, ";
												timeExtQry += "(select log_time_extension from property_settings where domain_id=" + domainId + " and user_id=" + listing_agent_id + " and is_agent=true order by id desc limit 1) as agent_log_time_extension, ";
												timeExtQry += "(select id from property_settings where domain_id=" + domainId + " and is_broker=true order by id desc limit 1) as broker_id, ";
												timeExtQry += "(select is_log_time_extension from property_settings where domain_id=" + domainId + " and is_broker=true order by id desc limit 1) as broker_is_log_time_extension, ";
												timeExtQry += "(select remain_time_to_add_extension from property_settings where domain_id=" + domainId + " and is_broker=true order by id desc limit 1) as broker_remain_time_to_add_extension, ";
												timeExtQry += "(select log_time_extension from property_settings where domain_id=" + domainId + " and is_broker=true order by id desc limit 1) as broker_log_time_extension ";
												timeExtQry += "from property_listing l where l.id=" + propertyId;

												pool.query(timeExtQry, function (err, timeExtRes) {
													if (err) {
														logger.log("error", 'ADDNEW_BID_7 ERROR QUERY : ' + timeExtQry);
														socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
														return 0;
													}
													else {
														if (timeExtRes?.rowCount > 0) {
															//Add extension time
															var extData = timeExtRes?.rows[0];
															var remain_time_to_add_extension = 0;
															var log_time_extension = 0;
															if (extData?.personal_id != null && extData?.personal_is_log_time_extension) {
																remain_time_to_add_extension = extData?.personal_remain_time_to_add_extension
																log_time_extension = extData.personal_log_time_extension
															} else if (extData?.personal_id == null && extData?.agent__id != null && extData?.agent_is_log_time_extension) {
																remain_time_to_add_extension = extData?.agent_remain_time_to_add_extension
																log_time_extension = extData.agent_log_time_extension
															} else if (extData?.personal_id == null && extData?.agent__id == null && extData?.broker_id != null && extData?.broker_is_log_time_extension) {
																remain_time_to_add_extension = extData?.broker_remain_time_to_add_extension
																log_time_extension = extData?.broker_log_time_extension
															}
															
															if (timeLeft <= remain_time_to_add_extension && log_time_extension > 0 && parseInt(auction_id) > 0) {
																var todayDate = new Date().toLocaleString("en-US", { timeZone: global.config.TIMEZONE });
																todayDate = new Date(todayDate);
																log_time_extension = adjustEndDate(todayDate, log_time_extension);
																log_time_extension = Number(timeLeft) + Number(log_time_extension);
																let extensionQry = "UPDATE property_auction SET end_date = (CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago' + (" + log_time_extension + " || ' minutes')::interval) AT TIME ZONE 'America/Chicago' WHERE id = " + auction_id;
																executeQuery(extensionQry);
															}
														}
													}
												});
												//Add extension time
												var bidderUserIdForEmail = "";
												if (checkBidRec?.rowCount > 0) {
													var bid_data = checkBidRec?.rows[0];
													var min_bid_amount = parseInt(bid_data?.bid_amount) + bidIncrement
													//Lets check if bid amount is correct
													if (min_bid_amount > bidAmount) {
														socket.emit("addNewBid", { "status": 400, "msg": "Please increase your bid amount.", "error": 1 });
														return 0;
													}
													//---------------------------------------
													let chkAutoBidQry = "select * from auto_bid_amount where user_id !=" + userId + " and property_id=" + propertyId + " and auto_bid_amount > " + bidAmount + " and status_id=1 order by id desc;";
													pool.query(chkAutoBidQry, function (err, chkAutoBidQryRes) {
														if (err) {
															logger.log("error", 'ERROR QUERY : ' + chkAutoBidQry);
															socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
															return 0;
														}
														else {
															if (chkAutoBidQryRes?.rowCount > 0) {
																chkAutoBidQryResponseValue = chkAutoBidQryRes?.rows[0];
																var highestAutoBidUserId = chkAutoBidQryResponseValue['user_id'];
																// Add data into bid
																let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
																bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + propertyId + "," + auction_id + "," + userId + ", '2', '0', " + domainId + ", '" + ip_address + "', " + data?.registration_id + ")";
																pool.query(bidAddQl, function (err, addBidRes) {
																	if (err) {
																		logger.log("error", 'ADDNEW_BID_8 ERROR QUERY : ' + bidAddQl);
																		socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																		return 0;
																	} else {
																		var amountToBid = parseInt(bidAmount) + parseInt(data?.bid_increment);
																		let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
																		bidAddQl += "VALUES(now()," + amountToBid + ",'1','1','0','0'," + propertyId + "," + auction_id + "," + highestAutoBidUserId + ", '2', '0', " + domainId + ", '" + ip_address + "', " + data?.registration_id + ")";
																		pool.query(bidAddQl, function (err, addBidRes) {
																			if (err) {
																				logger.log("error", 'ADDNEW_BID_9 ERROR QUERY : ' + bidAddQl);
																				socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																				return 0;
																			} else {
																				bidderUserIdForEmail = highestAutoBidUserId;
																				createSendMessage(domainId, propertyId, bidderUserIdForEmail, bidAmount);
																			}
																		});
																	}
																});
															}
															else {
																// Add data into bid
																let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
																bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + propertyId + "," + auction_id + "," + userId + ", '2', '0', " + domainId + ", '" + ip_address + "', " + data?.registration_id + ")";
																pool.query(bidAddQl, function (err, addBidRes) {
																	if (err) {
																		logger.log("error", 'ADDNEW_BID_10 ERROR QUERY : ' + bidAddQl);
																		socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																		return 0;
																	} else {
																		bidderUserIdForEmail = userId;
																		createSendMessage(domainId, propertyId, bidderUserIdForEmail, bidAmount);
																	}
																});
															}
														}
													});

												} else {
													let chkAutoBidQry = "select * from auto_bid_amount where user_id !=" + userId + " and property_id=" + propertyId + " and auto_bid_amount >= " + bidAmount + " and status_id=1 order by id desc;";
													pool.query(chkAutoBidQry, function (err, chkAutoBidQryRes) {
														if (err) {
															logger.log("error", 'ADDNEW_BID_11 ERROR QUERY : ' + chkAutoBidQry);
															socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
															return 0;
														}
														else {
															if (chkAutoBidQryRes?.rowCount > 0) {
																chkAutoBidQryResponseValue = chkAutoBidQryRes?.rows[0];
																var highestAutoBidUserId = chkAutoBidQryResponseValue['user_id'];
																// Add data into bid
																let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
																bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + propertyId + "," + auction_id + "," + userId + ", '2', '0', " + domainId + ", '" + ip_address + "', " + data?.registration_id + ")";
																pool.query(bidAddQl, function (err, addBidRes) {
																	if (err) {
																		logger.log("error", 'ADDNEW_BID_12 ERROR QUERY : ' + bidAddQl);
																		socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																		return 0;
																	} else {
																		let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
																		bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + propertyId + "," + auction_id + "," + highestAutoBidUserId + ", '2', '0', " + domainId + ", '" + ip_address + "', " + data?.registration_id + ")";
																		pool.query(bidAddQl, function (err, addBidRes) {
																			if (err) {
																				logger.log("error", 'ADDNEW_BID_13 ERROR QUERY : ' + bidAddQl);
																				socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																				return 0;
																			} else {
																				bidderUserIdForEmail = highestAutoBidUserId;
																				createSendMessage(domainId, propertyId, bidderUserIdForEmail, bidAmount);
																			}
																		});
																	}
																});
															}
															else {
																// Add data into bid
																let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
																bidAddQl += "VALUES(now()," + bidAmount + ",'1','1','0','0'," + propertyId + "," + auction_id + "," + userId + ", '2', '0', " + domainId + ", '" + ip_address + "', " + data?.registration_id + ")";
																pool.query(bidAddQl, function (err, addBidRes) {
																	if (err) {
																		logger.log("error", 'ADDNEW_BID_14 ERROR QUERY : ' + bidAddQl);
																		socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																		return 0;
																	} else {
																		bidderUserIdForEmail = userId;
																		createSendMessage(domainId, propertyId, bidderUserIdForEmail, bidAmount);
																	}
																});
															}
														}
													});

												}
											}
											let propertyQl = "UPDATE property_listing set read_by_auction_dashboard=false where id=" + propertyId;
											pool.query(propertyQl, function (err, addBidRes) {
												if (err) {
													logger.log("error", 'ADDNEW_BID_15 ERROR QUERY : ' + propertyQl);
													socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
													return 0;
												}
											});
											var msg = "You Have The Highest Bid.";
											if (reserveMet == "yes") {
												var msg = "You Have The Highest Bid.";
											}
											socket.emit("addNewBid", { "msg": msg, "status": 201, "error": 0 });
											global.io.emit("re-sync", { "msg": msg, "status": 201, "error": 0, data });
											return 0;
										});
									} else {
										socket.emit("addNewBid", { "msg": "Auction not exist.", "status": 400, "error": 1 });
										return 0;
									}
								}
							});
						} else {
							socket.emit("addNewBid", { "msg": "Registration not approved for bidding.", "status": 400, "error": 1 });
							return 0;
						}
					}
				});
			} else {
				logger.log("warn", `ADDNEW_BID_16 INVALID FIELDS: Raw Data: ${JSON.stringify(data)}`);
				socket.emit("addNewBid", { 'msg': 'Invalid request.', 'status': 400, 'error': 1 });
				return 0;
			}
		} catch (err) {
			logger.log("error", 'ADDNEW_BID_17 ERROR : ' + err.message);
			socket.emit("bidHistory", { "message": err.message, "status": 500 });
			return 0;
		}
	},

	setAutoBid: async function setAutoBid(socket, data) {
		try {
			let propertyId = data?.property_id || null;
			let userId = data?.user_id || null;
			let auctionId = data?.auction_id || null;
			let bidIncrement = data?.bid_increment || null;
			let autoBidAmount = data?.auto_bid_amount || null;
			let domainId = data?.domain_id || null;
			if (propertyId && userId && auctionId && autoBidAmount && domainId) {
				// lets check if property already sold
				let propertyQuery = "select id from property_listing where id = " + propertyId + " and domain_id = " + domainId + " and status_id=1;";
				pool.query(propertyQuery, function (err, startDateQueryRes) {
					if (err) {
						logger.log("error", 'SETAUTO_BID_1 ERROR QUERY : ' + propertyQuery);
						socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
						return 0;
					} else {
						if (startDateQueryRes?.rowCount < 1) {
							socket.emit("setAutoBid", { "status": 400, "msg": "Property sold.", "error": 1 });
							return 0;
						}
					}
				});

				if (parseInt(autoBidAmount) % parseInt(bidIncrement) != 0) {
					socket.emit("setAutoBid", { "status": 400, "msg": "Amount should be multiple of bid increment.", "error": 1 });
					return 0;
				}

				const checkPaymentStatusQuery = `
					SELECT bt.status_id 
					FROM bid_registration br 
					LEFT JOIN bid_transaction bt ON br.transaction_id = bt.id 
					WHERE br.property_id = ${propertyId} 
						AND br.user_id = ${userId} 
						AND bt.status_id = 34 
						AND bt.gateway_status = 'APPROVED'
					`;

				const paymentStatus = await pool.query(checkPaymentStatusQuery);
				if (paymentStatus?.rowCount > 0) {
					const updateQuery = `
					  UPDATE bid_registration 
					  SET is_approved = 2 
					  WHERE property_id = ${propertyId} AND user_id = ${userId}
					`;
			  
					await pool.query(updateQuery);
				}

				let autoBidAmountQuery = "select id from auto_bid_amount where property_id = " + propertyId + " and domain_id = " + domainId + " and auto_bid_amount = " + autoBidAmount + " and user_id != " + userId + " and status_id=1;";
				pool.query(autoBidAmountQuery, function (err, autoBidAmountRes) {
					if (err) {
						logger.log("error", 'SETAUTO_BID_2 ERROR QUERY : ' + propertyQuery);
						socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
						return 0;
					}
					else {
						if (autoBidAmountRes?.rowCount >= 1) {
							socket.emit("setAutoBid", { "status": 400, "msg": "Can't set autobid amount similar to other.", "error": 1 });
							return 0;
						} else {
							// lets check if bid is already started
							let startDateQuery = "select id from property_auction where property_id = " + propertyId + " and domain_id = " + domainId + " and status_id=1 and extract(epoch from ( start_date::timestamp -  now()::timestamp)) > 0;";
							pool.query(startDateQuery, function (err, startDateRes) {
								if (err) {
									logger.log("error", 'SETAUTO_BID_3 ERROR QUERY : ' + startDateQuery);
									socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
									return 0;
								}
								else {
									if (startDateRes?.rowCount > 0) {
										socket.emit("setAutoBid", { "status": 201, "msg": "Auction not started.", "error": 1 });
										return 0;
									} else {
										// lets check if bid is ended
										startDateQuery = "select id from property_auction where property_id = " + propertyId + " and domain_id = " + domainId + " and extract(epoch from ( end_date::timestamp -  now()::timestamp)) < 0;";
										pool.query(startDateQuery, function (err, startDateRes) {
											if (err) {
												logger.log("error", 'SETAUTO_BID_4 ERROR QUERY here : ' + startDateQuery);
												socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
												return 0;
											}
											else {
												if (startDateRes?.rowCount > 0) {
													socket.emit("setAutoBid", { "status": 201, "msg": "Auction ended.", "error": 1 });
													return 0;
												} else {
													//Now authenticate user against bid registration
													let chkQry = "select br.id as id, approval_limit ";
													chkQry += "from bid_limit bl join bid_registration br on bl.registration_id = br.id ";
													chkQry += "where br.user_id = " + userId + " and br.property_id = " + propertyId + " and br.domain_id = " + domainId + " and br.status_id=1 and br.is_approved=2 and br.is_reviewed=true and bl.is_approved=2 and bl.status_id=1 "
													chkQry += "order by bl.id desc limit 1;";
													pool.query(chkQry, function (err, checkRes) {
														if (err) {
															logger.log("error", 'SETAUTO_BID_5 ERROR QUERY : ' + chkQry);
															socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
															return 0;
														}
														else {
															if (checkRes?.rowCount > 0) {
																var registration_detail = checkRes?.rows[0];
																if (parseInt(registration_detail?.approval_limit) < parseInt(autoBidAmount)) {
																	socket.emit("setAutoBid", { "msg": "Your approval limit has been reached.<br/> Please contact to admin.", "status": 400, "error": 1 });
																	return 0;
																}
																// check bidding exist or not
																let chkBidMaxAmount = "select bid_amount, user_id from bid where property_id=" + propertyId + " order by id desc limit 1;"
																pool.query(chkBidMaxAmount, function (err, chkBidMaxAmountRes) {
																	if (err) {
																		logger.log("error", 'SETAUTO_BID_6 ERROR QUERY : ' + chkBidMaxAmount);
																		socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																		return 0;
																	}
																	else {
																		let setAutoBidQry = "";
																		if (chkBidMaxAmountRes?.rowCount > 0) {
																			let bidMaxAmountData = chkBidMaxAmountRes?.rows[0];
																			let maxBidderUserId = bidMaxAmountData['user_id'];
																			let maxBidderBidAmount = bidMaxAmountData['bid_amount'];
																			// condition check autobid amount should be greater than next bidding amount
																			if (data?.auto_bid_amount >= parseInt(bidMaxAmountData['bid_amount']) + parseInt(bidIncrement)) {
																				//----Check autobid amount set or not-----
																				let chkAutoBidQry = "select * from auto_bid_amount where user_id=" + userId + " and property_id=" + propertyId + " and status_id=1 order by id desc;";
																				pool.query(chkAutoBidQry, function (err, chkAutoBidRes) {
																					if (err) {
																						logger.log("error", 'SETAUTO_BID_7 ERROR QUERY : ' + chkAutoBidQry);
																						socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																						return 0;
																					}
																					else {
																						// ---------Check autobid set-----------
																						var bidding_will_happen = 1
																						if (chkAutoBidRes?.rowCount > 0) {
																							var chkAutoBidResData = chkAutoBidRes?.rows[0];
																							if (autoBidAmount > chkAutoBidResData['auto_bid_amount']) {
																								setAutoBidQry = "update auto_bid_amount set auto_bid_amount=" + autoBidAmount + ", updated_on = now() where user_id=" + userId + " and property_id=" + propertyId + " and status_id=1";
																							} else {
																								bidding_will_happen = 0
																								setAutoBidQry = "update auto_bid_amount set auto_bid_amount=" + autoBidAmount + ", updated_on = now() where user_id=" + userId + " and property_id=" + propertyId + " and status_id=1";
																							}
																						}
																						else {
																							setAutoBidQry = "insert into auto_bid_amount (user_id, property_id, auction_id, auto_bid_amount, status_id, added_on, updated_on, registration_id, domain_id) values (" + userId + ", " + propertyId + ", " + auctionId + ", " + autoBidAmount + ", 1, now(), now(), " + data?.registration_id + ", " + domainId + ");";
																						}
																						if (setAutoBidQry != "") {
																							pool.query(setAutoBidQry, function (err, setAutoBidRes) {
																								if (err) {
																									logger.log("error", 'SETAUTO_BID_8 ERROR QUERY : ' + setAutoBidQry);
																									socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																									return 0;
																								}
																								else {
																									if ((bidding_will_happen == 1 && maxBidderUserId != userId) || true) {
																										let nextBidAmount = parseInt(maxBidderBidAmount) + parseInt(bidIncrement);
																										let maxAutoBidAmountQry = "select * from auto_bid_amount where property_id=" + propertyId + " and auto_bid_amount >= " + nextBidAmount + " and user_id !=" + userId + " and status_id = 1 order by updated_on asc limit 1;";
																										pool.query(maxAutoBidAmountQry, function (err, maxAutoBidAmountRes) {
																											if (err) {
																												logger.log("error", 'SETAUTO_BID_9 ERROR QUERY : ' + maxAutoBidAmountQry);
																												socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																												return 0;
																											} else {
																												if (maxAutoBidAmountRes?.rowCount > 0) {
																													var maxAutoBidAmountData = maxAutoBidAmountRes?.rows[0];
																													// -----Bidding here for max auto bid amount
																													let userHighestAutoBidAmount = maxAutoBidAmountData['auto_bid_amount'];
																													let userHighestAutoBidAmountUserId = maxAutoBidAmountData['user_id'];
																													let userHighestAutoBidAmountRegistrationId = maxAutoBidAmountData['registration_id'];
																													let userHighestAutoBidAmountAuctionId = maxAutoBidAmountData['auction_id'];
																													let userHighestAutoBidAmountDomainId = maxAutoBidAmountData['domain_id'];
																													// check auto bid amount greater than current bidder
																													if (userHighestAutoBidAmount > autoBidAmount) {
																														autoBidFirst(socket, data, nextBidAmount, userHighestAutoBidAmountAuctionId, userHighestAutoBidAmountUserId, userHighestAutoBidAmountDomainId, userHighestAutoBidAmountRegistrationId);
																														return 0;
																													}
																													else if (parseInt(userHighestAutoBidAmount) == parseInt(autoBidAmount) && parseInt(autoBidAmount) == parseInt(nextBidAmount)) {
																														socket.emit("setAutoBid", { "msg": "Autobid amount set successfully.", "status": 201, "error": 0 });
																														return 0;
																													}
																													else if (userHighestAutoBidAmount == autoBidAmount) {
																														autoBidSecond(socket, data, nextBidAmount, userHighestAutoBidAmountAuctionId, userHighestAutoBidAmountUserId, userHighestAutoBidAmountDomainId, userHighestAutoBidAmountRegistrationId);
																														return 0;
																													}
																													else {
																														autoBid(socket, data, nextBidAmount, userHighestAutoBidAmount, userHighestAutoBidAmountAuctionId, userHighestAutoBidAmountUserId, userHighestAutoBidAmountDomainId, userHighestAutoBidAmountRegistrationId);
																														return 0;
																													}
																												}
																												else if ((parseInt(maxBidderUserId) != parseInt(userId)) || true) {
																													//Below code for auction end date extended
																													let startDateQuery = "select id, start_price, bid_increments, reserve_amount, bid_extension_time_period, bid_extension_amount, CEIL((extract (epoch from (end_date::timestamp - now()::timestamp))::float)/(60)) as time_left_min, end_date, ";
																													startDateQuery += "(select agent_id from property_listing where id=" + propertyId + " order by id desc limit 1) as listing_agent_id ";
																													startDateQuery += "from property_auction where property_id = " + propertyId + " order by id desc limit 1";
																													pool.query(startDateQuery, function (err, propertyAuction) {
																														if (err) {
																															logger.log("error", 'SETAUTO_BID_10 ERROR QUERY : ' + startDateQuery);
																															socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																															return 0;
																														}
																														else {
																															if (propertyAuction?.rowCount > 0) {
																																var auction_data = propertyAuction?.rows[0];
																																const listing_agent_id = parseInt(auction_data?.listing_agent_id);
																																const timeLeft = auction_data?.time_left_min;
																																const auction_id = parseInt(auction_data?.id);
																																timeExtQry = "select l.id as property_id, ";
																																timeExtQry += "(select id from property_settings where property_id=" + propertyId + " and is_broker=false and is_agent=false order by id desc limit 1) as personal_id, ";
																																timeExtQry += "(select is_log_time_extension from property_settings where property_id=" + propertyId + " and is_broker=false and is_agent=false order by id desc limit 1) as personal_is_log_time_extension, ";
																																timeExtQry += "(select remain_time_to_add_extension from property_settings where property_id=" + propertyId + " and is_broker=false and is_agent=false order by id desc limit 1) as personal_remain_time_to_add_extension, ";
																																timeExtQry += "(select log_time_extension from property_settings where property_id=" + propertyId + " and is_broker=false and is_agent=false order by id desc limit 1) as personal_log_time_extension, ";
																																timeExtQry += "(select id from property_settings where domain_id=" + domainId + " and user_id=" + listing_agent_id + " and is_agent=true order by id desc limit 1) as agent__id, ";
																																timeExtQry += "(select is_log_time_extension from property_settings where domain_id=" + domainId + " and user_id=" + listing_agent_id + " and is_agent=true order by id desc limit 1) as agent_is_log_time_extension, ";
																																timeExtQry += "(select remain_time_to_add_extension from property_settings where domain_id=" + domainId + " and user_id=" + listing_agent_id + " and is_agent=true order by id desc limit 1) as agent_remain_time_to_add_extension, ";
																																timeExtQry += "(select log_time_extension from property_settings where domain_id=" + domainId + " and user_id=" + listing_agent_id + " and is_agent=true order by id desc limit 1) as agent_log_time_extension, ";
																																timeExtQry += "(select id from property_settings where domain_id=" + domainId + " and is_broker=true order by id desc limit 1) as broker_id, ";
																																timeExtQry += "(select is_log_time_extension from property_settings where domain_id=" + domainId + " and is_broker=true order by id desc limit 1) as broker_is_log_time_extension, ";
																																timeExtQry += "(select remain_time_to_add_extension from property_settings where domain_id=" + domainId + " and is_broker=true order by id desc limit 1) as broker_remain_time_to_add_extension, ";
																																timeExtQry += "(select log_time_extension from property_settings where domain_id=" + domainId + " and is_broker=true order by id desc limit 1) as broker_log_time_extension ";
																																timeExtQry += "from property_listing l where l.id=" + propertyId;
																																pool.query(timeExtQry, function (err, timeExtRes) {
																																	if (err) {
																																		logger.log("error", 'SETAUTO_BID_11 ERROR QUERY : ' + timeExtQry);
																																		socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																																		return 0;
																																	}
																																	else {
																																		if (timeExtRes?.rowCount > 0) {
																																			var extData = timeExtRes?.rows[0];
																																			var remain_time_to_add_extension = 0;
																																			var log_time_extension = 0;
																																			if (extData?.personal_id != null && extData?.personal_is_log_time_extension) {
																																				remain_time_to_add_extension = extData?.personal_remain_time_to_add_extension
																																				log_time_extension = extData?.personal_log_time_extension
																																			} else if (extData?.personal_id == null && extData?.agent__id != null && extData?.agent_is_log_time_extension) {
																																				remain_time_to_add_extension = extData?.agent_remain_time_to_add_extension
																																				log_time_extension = extData?.agent_log_time_extension
																																			} else if (extData?.personal_id == null && extData?.agent__id == null && extData?.broker_id != null && extData?.broker_is_log_time_extension) {
																																				remain_time_to_add_extension = extData?.broker_remain_time_to_add_extension
																																				log_time_extension = extData?.broker_log_time_extension
																																			}

																																			if (timeLeft <= remain_time_to_add_extension && log_time_extension > 0 && parseInt(auction_id) > 0) {
																																				var todayDate = new Date().toLocaleString("en-US", { timeZone: global.config.TIMEZONE });
																																				todayDate = new Date(todayDate);
																																				log_time_extension = adjustEndDate(todayDate, log_time_extension);
																																				log_time_extension = Number(timeLeft) + Number(log_time_extension);
																																				let extensionQry = "UPDATE property_auction SET end_date = (CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago' + (" + log_time_extension + " || ' minutes')::interval) AT TIME ZONE 'America/Chicago' WHERE id = " + auction_id;
																																				executeQuery(extensionQry);
																																			}
																																		}
																																	}
																																});
																															}
																														}
																													});
																													let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
																													bidAddQl += "VALUES(now()," + nextBidAmount + ",'1','1','0','0'," + propertyId + "," + auctionId + "," + userId + ", '2', '0', " + domainId + ", '" + data?.ip_address + "', " + data?.registration_id + ")";
																													pool.query(bidAddQl, function (err, addBidRes) {
																														if (err) {
																															logger.log("error", 'SETAUTO_BID_12 ERROR QUERY : ' + bidAddQl);
																															socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																															return 0;
																														} else {
																															createSendMessage(domainId, propertyId, userId, parseInt(nextBidAmount));
																															socket.emit("setAutoBid", { "msg": "Autobid amount set successfully.", "status": 201, "error": 0 });
																															global.io.emit("re-sync", { "msg": msg, "status": 201, "error": 0, data });
																															return 0;
																														}
																													});
																												}
																												else {
																													socket.emit("setAutoBid", { "msg": "Autobid amount set successfully.", "status": 201, "error": 0 });
																													global.io.emit("re-sync", { "msg": msg, "status": 201, "error": 0, data });
																													return 0;
																												}
																											}
																										});
																									} else {
																										socket.emit("setAutoBid", { "msg": "You are already the highest bidder. Autobid amount set successfully.", "status": 400, "error": 1 });
																										return 0;
																									}
																								}
																							});
																						}
																					}
																				});
																			}
																			else {
																				socket.emit("setAutoBid", { "msg": "Please increase the value.", "status": 400, "error": 1 });
																				return 0;
																			}
																		}
																		else {
																			//----Check previous autobid amount set or not-----
																			if (autoBidAmount >= data?.min_bid_amount) {
																				let chkAutoBidQry = "select * from auto_bid_amount where user_id=" + userId + " and property_id=" + propertyId + " and status_id=1 order by id desc;";
																				pool.query(chkAutoBidQry, function (err, chkAutoBidRes) {
																					if (err) {
																						logger.log("error", 'SETAUTO_BID_13 ERROR QUERY : ' + chkAutoBidQry);
																						socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																						return 0;
																					}
																					else {
																						// ---------Check autobid set-----------
																						if (chkAutoBidRes?.rowCount > 0) {
																							var chkAutoBidResData = chkAutoBidRes?.rows[0];
																							if (data.auto_bid_amount > chkAutoBidResData['auto_bid_amount']) {
																								setAutoBidQry = "update auto_bid_amount set auto_bid_amount=" + autoBidAmount + ", updated_on = now() where user_id=" + userId + " and property_id=" + propertyId + " and status_id=1";
																							}
																							else {
																								socket.emit("setAutoBid", { "msg": "Please increase the value.", "status": 400, "error": 1 });
																								return 0;
																							}
																						}
																						else {
																							setAutoBidQry = "insert into auto_bid_amount (user_id, property_id, auction_id, auto_bid_amount, status_id, added_on, updated_on, registration_id, domain_id) values (" + userId + ", " + propertyId + ", " + data?.auction_id + ", " + autoBidAmount + ", 1, now(), now(), " + data?.registration_id + ", " + domainId + ");";
																						}
																						if (setAutoBidQry != "") {
																							pool.query(setAutoBidQry, function (err, setAutoBidRes) {
																								if (err) {
																									logger.log("error", 'SETAUTO_BID_14 ERROR QUERY : ' + setAutoBidQry);
																									socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																									return 0;
																								}
																								else {
																									//Below code for auction end date extended
																									let startDateQuery = "select id, start_price, bid_increments, reserve_amount, bid_extension_time_period, bid_extension_amount, CEIL((extract (epoch from (end_date::timestamp - now()::timestamp))::float)/(60)) as time_left_min, end_date, ";
																									startDateQuery += "(select agent_id from property_listing where id=" + propertyId + " order by id desc limit 1) as listing_agent_id ";
																									startDateQuery += "from property_auction where property_id = " + propertyId + " order by id desc limit 1";
																									pool.query(startDateQuery, function (err, propertyAuction) {
																										if (err) {
																											logger.log("error", 'SETAUTO_BID_15 ERROR QUERY : ' + startDateQuery);
																											socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																											return 0;
																										}
																										else {
																											if (propertyAuction.rowCount > 0) {
																												var auction_data = propertyAuction?.rows[0];
																												const listing_agent_id = parseInt(auction_data?.listing_agent_id);
																												const timeLeft = auction_data?.time_left_min;
																												const auction_id = parseInt(auction_data?.id);
																												timeExtQry = "select l.id as property_id, ";
																												timeExtQry += "(select id from property_settings where property_id=" + propertyId + " and is_broker=false and is_agent=false order by id desc limit 1) as personal_id, ";
																												timeExtQry += "(select is_log_time_extension from property_settings where property_id=" + propertyId + " and is_broker=false and is_agent=false order by id desc limit 1) as personal_is_log_time_extension, ";
																												timeExtQry += "(select remain_time_to_add_extension from property_settings where property_id=" + propertyId + " and is_broker=false and is_agent=false order by id desc limit 1) as personal_remain_time_to_add_extension, ";
																												timeExtQry += "(select log_time_extension from property_settings where property_id=" + propertyId + " and is_broker=false and is_agent=false order by id desc limit 1) as personal_log_time_extension, ";
																												timeExtQry += "(select id from property_settings where domain_id=" + domainId + " and user_id=" + listing_agent_id + " and is_agent=true order by id desc limit 1) as agent__id, ";
																												timeExtQry += "(select is_log_time_extension from property_settings where domain_id=" + domainId + " and user_id=" + listing_agent_id + " and is_agent=true order by id desc limit 1) as agent_is_log_time_extension, ";
																												timeExtQry += "(select remain_time_to_add_extension from property_settings where domain_id=" + domainId + " and user_id=" + listing_agent_id + " and is_agent=true order by id desc limit 1) as agent_remain_time_to_add_extension, ";
																												timeExtQry += "(select log_time_extension from property_settings where domain_id=" + domainId + " and user_id=" + listing_agent_id + " and is_agent=true order by id desc limit 1) as agent_log_time_extension, ";
																												timeExtQry += "(select id from property_settings where domain_id=" + domainId + " and is_broker=true order by id desc limit 1) as broker_id, ";
																												timeExtQry += "(select is_log_time_extension from property_settings where domain_id=" + domainId + " and is_broker=true order by id desc limit 1) as broker_is_log_time_extension, ";
																												timeExtQry += "(select remain_time_to_add_extension from property_settings where domain_id=" + domainId + " and is_broker=true order by id desc limit 1) as broker_remain_time_to_add_extension, ";
																												timeExtQry += "(select log_time_extension from property_settings where domain_id=" + domainId + " and is_broker=true order by id desc limit 1) as broker_log_time_extension ";
																												timeExtQry += "from property_listing l where l.id=" + propertyId;
																												pool.query(timeExtQry, function (err, timeExtRes) {
																													if (err) {
																														logger.log("error", 'SETAUTO_BID_15 ERROR QUERY : ' + timeExtQry);
																														socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																														return 0;
																													}
																													else {
																														if (timeExtRes?.rowCount > 0) {
																															var extData = timeExtRes?.rows[0];
																															var remain_time_to_add_extension = 0;
																															var log_time_extension = 0;
																															if (extData?.personal_id != null && extData?.personal_is_log_time_extension) {
																																remain_time_to_add_extension = extData?.personal_remain_time_to_add_extension
																																log_time_extension = extData?.personal_log_time_extension
																															} else if (extData?.personal_id == null && extData?.agent__id != null && extData?.agent_is_log_time_extension) {
																																remain_time_to_add_extension = extData?.agent_remain_time_to_add_extension
																																log_time_extension = extData?.agent_log_time_extension
																															} else if (extData?.personal_id == null && extData?.agent__id == null && extData?.broker_id != null && extData?.broker_is_log_time_extension) {
																																remain_time_to_add_extension = extData?.broker_remain_time_to_add_extension
																																log_time_extension = extData?.broker_log_time_extension
																															}

																															if (timeLeft <= remain_time_to_add_extension && log_time_extension > 0 && parseInt(auction_id) > 0) {
																																var todayDate = new Date().toLocaleString("en-US", { timeZone: global.config.TIMEZONE });
																																todayDate = new Date(todayDate);
																																log_time_extension = adjustEndDate(todayDate, log_time_extension);
																																log_time_extension = Number(timeLeft) + Number(log_time_extension);
																																let extensionQry = "UPDATE property_auction SET end_date = (CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago' + (" + log_time_extension + " || ' minutes')::interval) AT TIME ZONE 'America/Chicago' WHERE id = " + auction_id;
																																executeQuery(extensionQry);
																															}
																														}
																													}
																												});
																											}
																										}
																									});
																									//---------First Bid Here-------
																									let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
																									bidAddQl += "VALUES(now()," + data?.min_bid_amount + ",'1','1','0','0'," + propertyId + "," + data?.auction_id + "," + userId + ", '2', '0', " + domainId + ", '" + data?.ip_address + "', " + data?.registration_id + ")";
																									pool.query(bidAddQl, function (err, addBidRes) {
																										if (err) {
																											logger.log("error", 'SETAUTO_BID_16 ERROR QUERY : ' + bidAddQl);
																											socket.emit("addNewBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
																											return 0;
																										} else {
																											createSendMessage(domainId, propertyId, userId, parseInt(data?.min_bid_amount));
																											socket.emit("setAutoBid", { "msg": "Autobid amount set successfully.", "status": 201, "error": 0 });
																											return 0;
																										}
																									});
																								}
																							});
																						}
																					}
																				});
																			}
																			else {
																				socket.emit("setAutoBid", { "msg": "Please increase the value.", "status": 400, "error": 1 });
																				return 0;
																			}
																		}
																	}
																});
															} else {
																socket.emit("setAutoBid", { "msg": "Registration not approved for set autobid.", "status": 400, "error": 1 });
																return 0;
															}
														}
													});
												}
											}
										});
									}
								}
							});
						}
					}
				});
			} else {
				logger.log("warn", `SETAUTO_BID_17 INVALID FIELDS: Raw Data: ${JSON.stringify(data)}`);
				socket.emit("setAutoBid", { 'msg': 'Invalid request.', 'status': 400, 'error': 1 });
				return 0;
			}
		} catch (err) {
			logger.log("error", 'SETAUTO_BID_18 ERROR : ' + err.message);
			socket.emit("bidHistory", { "message": err.message, "status": 500 });
			return 0;
		}
	},

	toggleAutoBidStatus: function toggleAutoBidStatus(socket, data) {
		/**
		 * This is listner function to update auto bid status
		 * requrest parameter:
		 * 			listing_id: as integer
		 * 			auction_id:	as integer
		 * response:
		 * 		 db object as json
		 */
		try {
			if (parseInt(data.user_id) > 0 && parseInt(data.property_id) > 0 && parseInt(data.domain_id) > 0 && (parseInt(data.status) === 1 || parseInt(data.status) === 2)) {
				const updateAutoBidStatus = "Update auto_bid_amount SET status_id = " + data.status + ",updated_on =now() WHERE property_id=" + parseInt(data.property_id) + " and  user_id=" + parseInt(data.user_id) + " and domain_id=" + parseInt(data.domain_id);
				pool.query(updateAutoBidStatus, function (err, historyRes) {
					if (err) {
						logger.log("toggleAutoBidStatus", 'ERROR QUERY : ' + updateAutoBidStatus);
						socket.emit("toggleAutoBidStatus", { "msg": err, 'status': 400, "error": 1 });
						return 0;
					}
					else {
						socket.emit("toggleAutoBidStatus", { "msg": "status updated", "status": 201, "error": 0 });
						return 0;
					}
				});
			}
			else {
				//required parameter not found
				logger.log("error", 'ERROR : Missing requered parameter');
				socket.emit("toggleAutoBidStatus", { "msg": "Forbidden.", "status": 400, "error": 1 });
				return 0;
			}
		} catch (err) {
			//trapped error
			logger.log("error", 'ERROR : ' + err.message);
			socket.emit("toggleAutoBidStatus", { "msg": err.message, "status": 400, "error": 1 });
			return 0;
		}
	},

	bidHistory: function bidHistory(socket, data) {
		/**
		 * This is listner function to update bid history during live
		 * bid process.
		 * requrest parameter:
		 * 			listing_id: as integer
		 * 			auction_id:	as integer
		 * response:
		 * 		 db object as json
		 */
		//logger.log("error",  "listingId"+ JSON.stringify(data));
		try {
			if (parseInt(data.auction_id) > 0 && parseInt(data.property_id) > 0 && parseInt(data.domain_id) > 0) {

				let historyQl = "SELECT bidder_rand_name, bid_date, bid_amount, bid_type from view_bid_history ";
				historyQl += " WHERE auction_id = " + data.auction_id;
				historyQl += " AND property_id = " + data.property_id;

				pool.query(historyQl, function (err, historyRes) {
					if (err) {
						logger.log("error", 'ERROR QUERY : ' + historyQl);
						socket.emit("checkBid", { "msg": err, 'status': 400, "error": 1 });
						return 0;
					}
					else {
						if (historyRes.rowCount) {
							socket.emit("bidHistory", { "data": historyRes.rows, "status": 201, "error": 0 });
						}
						else {
							socket.emit("bidHistory", { "msg": "No history record found", "status": 201, "error": 0 });
						}
						return 0;
					}
				});
			}
			else {
				//required parameter not found
				logger.log("error", 'ERROR : Missing requered parameter');
				socket.emit("bidHistory", { "msg": "Forbidden.", "status": 400, "error": 1 });
				return 0;
			}
		} catch (err) {
			//trapped error
			logger.log("error", 'ERROR : ' + err.message);
			socket.emit("bidHistory", { "msg": err.message, "status": 400, "error": 1 });
			return 0;
		}
	},

	checkAuction: function (socket, data) {
		try {
			let domainId = parseInt(data?.domain_id || 0);

			if(domainId){
				//Lets get all auctions where status is 1;
				let auctionQry = "select auction_id AS id, property_id, time_left, reserve_amount as reserve_price, max_bid, winner_id,auction_type from view_auction_time_left where time_left < 1 and domain_id=" + domainId + ";";
				pool.query(auctionQry, function (err, result) {
					if (err) {
						logger.log("error", 'CHECKAUCTION_1 ERROR AUCTION STATUS QUERY : ' + auctionQry);
						console.log("checkAuction: Error in query execution", err);
						return 0;
					}
					else {
						//if record found 
						if (result?.rowCount > 0) {
							//lets check if reserve met update to sold else expire
							let rows = result?.rows;
							let bidMax = 0;
							let reservePrice = 0;
							let propertyStatus = 0;
							let auctionStatus = 0;
							let statusQl = '';
							let auctionQl = '';
							let winnerId = '';
							let auctionType = '';

							rows.forEach(function (val) {
								//if max bid amount is more than reserve then mark listing sold else expire
								reservePrice = parseInt(val['reserve_price']);
								bidMax = parseInt(val['max_bid']);
								winnerId = parseInt(val['winner_id']);
								auctionType = parseInt(val['auction_type']);
								if (auctionType == 1) {
									auctionStatus = 9;
									if (bidMax >= reservePrice) {
										propertyStatus = 9; //sold	
										closing_status_id = 35; //under offer	
									} else if (bidMax == null || bidMax == '' || isNaN(bidMax)) {
										propertyStatus = 8; //not sold
										closing_status_id = 8
									} else {
										propertyStatus = 9; //sold	
										closing_status_id = 16 //Pending Review
									}
									if(val['property_id'] == 214){
										console.log("checkAuction: Auction found for property_id:", val['property_id']);
										console.log(bidMax, reservePrice, propertyStatus, winnerId,);
									}
									if (parseInt(propertyStatus) == 8) {
										statusQl = "Update property_listing SET status_id = " + propertyStatus + ", closing_status_id =" + closing_status_id + ", updated_on =now() WHERE id=" + parseInt(val['property_id']) + " and domain_id=" + domainId;
									} else {
										if(bidMax < reservePrice){
											statusQl = "Update property_listing SET status_id = 9, closing_status_id =" + closing_status_id + ", updated_on =now() WHERE id=" + parseInt(val['property_id']) + " and domain_id=" + domainId;
										}else{
											statusQl = "Update property_listing SET status_id = 9, closing_status_id =" + closing_status_id + ", updated_on =now(), sold_price=" + bidMax + ", winner_id = " + winnerId + ",date_sold =now() WHERE id=" + parseInt(val['property_id']) + " and domain_id=" + domainId;
										}
										
										//---------------------Email send here----------------
										// if (val['winner_id']) sendMessage(domainId, parseInt(val['property_id']), winnerId, bidMax);
									}
									if(val['property_id'] == 214){
										console.log(statusQl,"statusQl");
									}	
									executeQuery(statusQl);
									if (parseInt(propertyStatus) != 8) {
										auctionQl = "Update property_auction SET status_id = " + auctionStatus + " Where id = " + parseInt(val['id']) + " and domain_id=" + domainId;
										executeQuery(auctionQl);
									}
								}
							});
							return 0;
						}
						console.log("checkAuction: No auctions found for domain_id:", data.domain_id);
					}

				});
			} else {
				console.log("checkAuction: Missing domain_id");
			}
		}
		catch (err) {
			logger.log("error", 'CHECKAUCTION_2 ERROR AUCTION STATUS ERROR : ' + err.stack);
			console.log("error", 'AUCTION STATUS ERROR : ' + err.message);
			return 0;
		}
	},

	auctionStartMail: function (socket, data) {
		try {
			if (data.domain_id == undefined || data.domain_id == "" || data.domain_id <= 0) {
				console.log("auctionStartMail: Missing domain_id");
			}
			let auctionQry = `SELECT 
									pl.id as property_id,
									pl.status_id, 
									pa.start_date,
									(EXTRACT(EPOCH FROM (pa.start_date::timestamp - NOW()::timestamp))::integer) AS start_time_left_sec
								FROM 
									property_listing pl
								JOIN 
									property_auction pa ON pl.id = pa.property_id
								WHERE 
									pl.status_id = 1 AND pl.start_email_sent = 0
									AND (EXTRACT(EPOCH FROM (pa.start_date::timestamp - NOW()::timestamp)) BETWEEN -900 AND -1)`;
			pool.query(auctionQry, function (err, result) {
				if (err) {
					logger.log("error", 'ERROR AUCTION STATUS QUERY : ' + auctionQry);
					console.log("auctionStartMail: Error in query execution", err);
					return 0;
				}
				else {
					//if record found 
					if (result.rowCount > 0) {
						let rows = result.rows;
						let propertyIds = [];
						rows.forEach(function (val) {
								sendAuctionStartMessage(data.domain_id, parseInt(val['property_id']));
								propertyIds.push(parseInt(val['property_id']));
						});
						if (propertyIds.length > 0) {
							let updateQry = `UPDATE property_listing 
											 SET start_email_sent = 1 
											 WHERE id = ANY($1)`;
							
							pool.query(updateQry, [propertyIds], function (updateErr) {
								if (updateErr) {
									logger.log("error", "ERROR UPDATING start_email_sent : " + updateErr.message);
								} else {
									logger.log("info", "Updated start_email_sent flag for properties: " + propertyIds.join(", "));
								}
							});
						}
						return 0;
					}
				}
			});
		}
		catch (err) {
			logger.log("error", 'AUCTION STATUS ERROR : ' + err.message);
			return 0;
		}
	},

	// ---------------------Check auction data here---------------------
	checkAuctionData: function (socket, data) {
		if (data.property_id == undefined || data.property_id == "" || data.property_id <= 0) {
			socket.emit("checkAuctionData", { "msg": "Property id missing." });
		}
		let property_id = parseInt(data.property_id)
		// -----Fetch auction data from db-------
		let auctionQry = "select id,start_date,end_date,bid_increments, reserve_amount, start_price from property_auction where status_id = 1 and property_id= " + property_id;
		try {
			pool.query(auctionQry, function (err, result) {
				if (err) {
					logger.log("error", 'ERROR AUCTION STATUS QUERY : ' + auctionQry);
					console.error(err)
					console.error('Error executing query', err.stack)
				} else if (result.rowCount > 0) {
					socket.emit("checkAuctionData", { "data": result.rows[0], "error": 201, "code": 1, "msg": "Fetch data" });
					socket.broadcast.emit("checkAuctionData", { "data": result.rows[0], "error": 201, "code": 1, "msg": "Fetch data" });
				}
			})
		}
		catch (err) {
			logger.log("error", 'AUCTION STATUS ERROR : ' + err.message);
			return 0;
		}
	},

	// --------------------Testing function---------------------
	test: function (socket, data) {
		try {
			console.log("Coverted User Id is", data);
			socket.emit("test", { "msg": "Hiiiiiiii" });
			return 0;
		}
		catch (err) {
			logger.log("error", 'AUCTION STATUS ERROR : ' + err.message);
			return 0;
		}
	},

	// --------------------Watcher function---------------------
	watcher: function (socket) {
		try {
			socket.emit("watcher", { "no_watcher": "1 Watcher" });
		}
		catch (err) {
			logger.log("error", 'AUCTION STATUS ERROR : ' + err.message);
			return 0;
		}
	},

	checkAuctionStatus: function checkAuctions(socket, data) {
		/**
		 * This is listner function to add property watcher during live
		 * bid process.
		 * requrest parameter:
		 * 			property_id: as integer
		 * 			user_id:	as integer
		 * response:
		 * 		 db object as json
		 */
		//logger.log("error",  "listingId"+ JSON.stringify(data));
		try {
			if (parseInt(data.property_id) > 0 && parseInt(data.auction_id) > 0) {
				let flshQuery = "SELECT pl.id as proeprty_id,pl.winner_id as winner_id,pl.status_id as property_status,pa.start_date,pa.end_date,pa.status_id as auction_status,pa.reserve_amount,";
					flshQuery += " ((extract (epoch from (pa.end_date::timestamp - now()::timestamp))::integer)) as time_left_hr, ((extract (epoch from (pa.start_date::timestamp - now()::timestamp))::integer)) as start_time_left_hr, pa.ending_soon_threshold";
					flshQuery += " FROM property_auction as pa";
					flshQuery += " INNER JOIN property_listing as pl ON pl.id=pa.property_id ";
					flshQuery += "where pl.id=" + data.property_id + " and pa.id=" + data.auction_id + " Limit 1";
					pool.query(flshQuery, function (err, result) {
						if (err) {
							logger.log("error", 'ERROR QUERY : ' + flshQuery);
							socket.emit("checkAuctionStatus", { "msg": err, 'status': 400, "error": 1, "user_id": data.user_id });
							socket.in(data.property_id + '_' + data.user_id).emit("checkAuctionStatus", { "msg": err, 'status': 400, "error": 1, "user_id": data.user_id });
							return 0;

						} else {
							if (result.rowCount > 0) {
								socket.emit("checkAuctionStatus", { "data": result.rows[0], "msg": "Fetch data", "status": 201, "error": 0, "user_id": data.user_id });
								socket.in(data.property_id + '_' + data.user_id).emit("checkAuctionStatus", { "data": result.rows, "msg": "Fetch data", "status": 201, "error": 0, "user_id": data.user_id });
								return 0;
							} else {
								socket.emit("checkAuctionStatus", { "msg": "No data found!", "status": 400, "error": 1, "data": {} });
								socket.in(data.property_id + '_' + data.user_id).emit("checkAuctionStatus", { "msg": "No data found!", "status": 400, "error": 1, "data": {}, "user_id": data.user_id });
								return 0;
							}
						}

					});
			}
			else {
				//required parameter not found
				logger.log("error", 'ERROR : Missing requered parameter');
				socket.emit("checkAuctionStatus", { "msg": "Missing requered parameter.", "status": 400, "error": 1 });
				return 0;
			}
		} catch (err) {
			//trapped error
			logger.log("error", 'ERROR : ' + err.message);
			socket.emit("checkAuctionStatus", { "msg": err.message, "status": 400, "error": 1 });
			return 0;
		}
	},

	checkAuctionEnd: function checkAuctionEnds(socket, data) {
		/**
		 * This is listner function to add property watcher during live
		 * bid process.
		 * requrest parameter:
		 * 			property_id: as integer
		 * 			user_id:	as integer
		 * response:
		 * 		 db object as json
		 */
		//logger.log("error",  "listingId"+ JSON.stringify(data));
		try {
			if (parseInt(data.property_id) > 0 && parseInt(data.auction_id) > 0) {
				let flshQuery = "SELECT pl.id as proeprty_id,pl.winner_id as winner_id,pl.status_id as property_status,pa.start_date,pa.end_date,pa.status_id as auction_status,pa.reserve_amount,";
				flshQuery += " ((extract (epoch from (pa.end_date::timestamp - now()::timestamp))::integer)) as time_left_hr, ((extract (epoch from (pa.start_date::timestamp - now()::timestamp))::integer)) as start_time_left_hr, pa.ending_soon_threshold";
				flshQuery += " FROM property_auction as pa";
				flshQuery += " INNER JOIN property_listing as pl ON pl.id=pa.property_id ";
				flshQuery += "where pl.id=" + data.property_id + " and pa.id=" + data.auction_id + " Limit 1";
				pool.query(flshQuery, function (err, result) {
					if (err) {
						logger.log("error", 'ERROR QUERY : ' + flshQuery);
						socket.emit("checkAuctionStatus", { "msg": err, 'status': 400, "error": 1, "user_id": data.user_id });
						socket.in(data.property_id + '_' + data.user_id).emit("checkAuctionStatus", { "msg": err, 'status': 400, "error": 1, "user_id": data.user_id });
						return 0;
					} else {
						if (result.rowCount > 0) {
							socket.emit("checkAuctionStatus", { "data": result.rows[0], "msg": "Fetch data", "status": 201, "error": 0, "user_id": data.user_id });
							socket.in(data.property_id + '_' + data.user_id).emit("checkAuctionStatus", { "data": result.rows, "msg": "Fetch data", "status": 201, "error": 0, "user_id": data.user_id });
							return 0;
						} else {
							socket.emit("checkAuctionStatus", { "msg": "No data found!", "status": 400, "error": 1, "data": {} });
							socket.in(data.property_id + '_' + data.user_id).emit("checkAuctionStatus", { "msg": "No data found!", "status": 400, "error": 1, "data": {}, "user_id": data.user_id });
							return 0;
						}
					}
				});
			}
			else {
				//required parameter not found
				logger.log("error", 'ERROR : Missing requered parameter');
				socket.emit("checkAuctionStatus", { "msg": "Missing requered parameter.", "status": 400, "error": 1 });
				return 0;
			}
		} catch (err) {
			//trapped error
			logger.log("error", 'ERROR : ' + err.message);
			socket.emit("checkAuctionStatus", { "msg": err.message, "status": 400, "error": 1 });
			return 0;
		}
	},

	checkAuctionDashboard: function checkAuctionDashboard(socket, data) {
		try {
			if (data.property_id > 0 && data.user_id > 0 && data.auction_id > 0 && data.domain_id > 0) {
				// check if listing is available for requested domain id
				let chkQry = "select id from property_listing where id = " + data.property_id + " and domain_id=" + data.domain_id;
				pool.query(chkQry, function (err, checkRes) {
					if (err) {
						//if error in sql let log query
						logger.log("error", 'ERROR QUERY : ' + chkQry);
						socket.emit("checkAuctionDashboard", { "msg": err, "status": 400, "error": 1 });
						return 0;
					}
					else {
						if (checkRes.rowCount > 0) {
							//This will check auction records and keep bid value in sync it also take time remain and threshold value
							let sqlQuery = 'SELECT ' +
								'a.property_id    AS property_id, ' +
								'b.bid_amount as high_bid, ' +
								'b.user_id AS high_bidder_user_id, ' +
								'(select Count(*) from bid where is_canceled=false and property_id=' + data.property_id + ') as bid_count, ' +
								'(select Count(DISTINCT user_id) from bid_registration where status_id=1 and property_id=' + data.property_id + ') as bidders, ' +
								'(select Count(*) from property_watcher where property_id=' + data.property_id + ') as watchers, ' +
								'a.bid_increments AS bid_increments, ' +
								'a.reserve_amount AS reserve_amount, ' +
								'a.start_date     AS start_date, ' +
								'a.end_date       AS end_date, ' +
								'a.start_price    AS start_price, ' +
								'a.status_id      AS auction_status, ' +
								// 'ls.status_name AS status_name, ' +
								// 'CASE WHEN pl.status_id = 9 THEN cs.status_name ELSE ls.status_name END AS status_name, ' +
								'CASE WHEN pl.closing_status_id > 0 THEN cs.status_name ELSE ls.status_name END AS status_name, ' +
								'ls.id AS listing_status, ' +
								'pl.read_by_auction_dashboard as new_bid_added, ' +
								'((Extract (epoch FROM (a.end_date::timestamp   - Now()::timestamp))::integer)) AS time_left_hr, ' +
								'((Extract (epoch FROM (a.start_date::timestamp - Now()::timestamp))::integer)) AS start_time_left_hr, ' +
								'a.ending_soon_threshold ' +
								'FROM     property_auction a ' +
								'JOIN property_listing pl ' +
								'on a.property_id = pl.id ' +
								'JOIN lookup_status ls ' +
								'on pl.status_id = ls.id ' +
								'JOIN lookup_status cs ' +
								'on pl.closing_status_id = cs.id ' +
								'LEFT OUTER JOIN bid b ' +
								"ON b.property_id = a.property_id and bid_type IN('2', '3') and is_canceled=false " +
								'WHERE   a.property_id = ' + data.property_id +
								' AND a.id = ' + data.auction_id +
								' AND a.domain_id= ' + data.domain_id +
								' ORDER BY b.bid_amount DESC, ' +
								'b.bid_date DESC ' +
								'limit 1';
							pool.query(sqlQuery, function (err, result) {
								if (err) {
									//check logger wokring
									logger.log("error", 'ERROR QUERY : ' + sqlQuery);
									socket.emit("checkAuctionDashboard", { "msg": err, "error": 1, "status": 400 });
									return 0;
								}
								else {
									if (result.rowCount > 0) {
										// check if auction have high bidder,if so fetch bidder details
										var highBidderId = result.rows[0].high_bidder_user_id
										if (highBidderId) {
											hidhBidderQuery = 'select ba.*, b.ip_address, ls.state_name  from bid_registration_address ba ' +
												'join bid_registration b on b.id= ba.registration_id ' +
												'join lookup_state ls on ls.id = ba.state_id ' +
												"where ba.status_id= 1 and ba.address_type IN('2', '3') and b.user_id=" + highBidderId + " and b.property_id=" + data.property_id + " LIMIT 1";
											pool.query(hidhBidderQuery, function (err, highBidder) {
												if (err) {
													//check logger wokring
													logger.log("error", 'ERROR QUERY : ' + hidhBidderQuery);
													socket.emit("checkAuctionDashboard", { "data": result.rows[0], "high_bidder_data": {}, "msg": "Fetch data High Bidder error found", "status": 201, "error": 0 });
													return 0;
												}
												else {
													highBidder = (highBidder.rows[0] == undefined) ? {} : highBidder.rows[0]
													socket.emit("checkAuctionDashboard", { "data": result.rows[0], "high_bidder_data": highBidder, "msg": "Fetch data High bidder found", "status": 201, "error": 0 });
												}
											});
										} else {
											socket.emit("checkAuctionDashboard", { "data": result.rows[0], "high_bidder_data": {}, "msg": "Fetch data No high bidder", "status": 201, "error": 0 });
										}
									} else {
										socket.emit("checkAuctionDashboard", { 'msg': 'No Data Found.', 'status': 400, "error": 1 });
									}
								}
							});
						}
						else {
							socket.emit("checkAuctionDashboard", { 'msg': 'Invalid request.', 'status': 400, "error": 1 });
						}
					}
				});
			}
			else {
				socket.emit("checkAuctionDashboard", { 'msg': 'Invalid request.', 'status': 403, "error": 1 });
			}
		}
		catch (err) {
			logger.log("error", 'AUCTION Dash ERROR : ' + err.message);
			return 0;
		}
	},

	checkInsiderAuctionDashboard: function checkInsiderAuctionDashboard(socket, data) {
		try {
			if (data.property_id > 0 && data.user_id > 0 && data.auction_id > 0 && data.domain_id > 0) {
				// check if listing is available for requested domain id
				let chkQry = "select id from property_listing where id = " + data.property_id + " and domain_id=" + data.domain_id;
				pool.query(chkQry, function (err, checkRes) {
					if (err) {
						//if error in sql let log query
						logger.log("error", 'ERROR QUERY : ' + chkQry);
						socket.emit("checkInsiderAuctionDashboard", { "msg": err, "status": 400, "error": 1 });
						return 0;
					}
					else {
						if (checkRes.rowCount > 0) {
							//This will check auction records and keep bid value in sync it also take time remain and threshold value
							let sqlQuery = 'SELECT ' +
								'a.property_id    AS property_id, ' +
								"CASE " +
								"WHEN b.insider_auction_step != 2 THEN b.bid_amount " +
								"ELSE (select bi.bid_amount from bid bi join insider_auction_step_winner iw on bi.id = iw.bid_id  where bi.is_canceled=false and bi.property_id=" + data.property_id + " and iw.insider_auction_step =2 and iw.status_id =1 ORDER BY bi.bid_amount DESC, bi.bid_date DESC limit 1) " +
								"END AS high_bid, " +
								"CASE " +
								"WHEN b.insider_auction_step != 2 THEN b.user_id " +
								"ELSE (select bi.user_id from bid bi join insider_auction_step_winner iw on bi.id = iw.bid_id  where bi.is_canceled=false and bi.property_id=" + data.property_id + " and iw.insider_auction_step =2 and iw.status_id =1 ORDER BY bi.bid_amount DESC, bi.bid_date DESC limit 1) " +
								"END AS high_bidder_user_id, " +
								'(select Count(*) from bid where is_canceled=false and property_id=' + data.property_id + ') as bid_count, ' +
								'(select Count(DISTINCT user_id) from bid_registration where status_id=1 and property_id=' + data.property_id + ') as bidders, ' +
								'(select Count(*) from property_watcher where property_id=' + data.property_id + ') as watchers, ' +
								'a.bid_increments AS bid_increments, ' +
								'a.start_date     AS start_date, ' +
								'a.dutch_end_time AS dutch_end_time, ' +
								'a.sealed_start_time AS sealed_start_time, ' +
								'a.sealed_end_time AS sealed_end_time, ' +
								'a.english_start_time AS english_start_time, ' +
								'a.end_date       AS end_date, ' +
								'a.start_price    AS start_price, ' +
								'a.status_id      AS auction_status, ' +
								'ls.status_name AS status_name, ' +
								'ls.id AS listing_status, ' +
								'pl.read_by_auction_dashboard as new_bid_added, ' +
								'((Extract (epoch FROM (a.end_date::timestamp   - Now()::timestamp))::integer)) AS time_left_hr, ' +
								'((Extract (epoch FROM (a.start_date::timestamp - Now()::timestamp))::integer)) AS start_time_left_hr, ' +
								'a.ending_soon_threshold ' +
								'FROM     property_auction a ' +
								'JOIN property_listing pl ' +
								'on a.property_id = pl.id ' +
								'JOIN lookup_status ls ' +
								'on pl.status_id = ls.id ' +
								'LEFT OUTER JOIN bid b ' +
								"ON b.property_id = a.property_id and bid_type IN('2', '3') and is_canceled=false " +
								'WHERE   a.property_id = ' + data.property_id +
								' AND a.id = ' + data.auction_id +
								' AND a.domain_id= ' + data.domain_id +
								' ORDER BY b.bid_amount DESC, ' +
								'b.bid_date DESC ' +
								'limit 1';
							pool.query(sqlQuery, function (err, result) {
								if (err) {
									//check logger wokring
									logger.log("error", 'ERROR QUERY : ' + sqlQuery);
									socket.emit("checkInsiderAuctionDashboard", { "msg": err, "error": 1, "status": 400 });
									return 0;
								}
								else {
									if (result.rowCount > 0) {
										// check if auction have high bidder,if so fetch bidder details
										var highBidderId = result.rows[0].high_bidder_user_id
										if (highBidderId) {
											hidhBidderQuery = 'select ba.*, b.ip_address, ls.state_name  from bid_registration_address ba ' +
												'join bid_registration b on b.id= ba.registration_id ' +
												'join lookup_state ls on ls.id = ba.state_id ' +
												"where ba.status_id= 1 and ba.address_type IN('2', '3') and b.user_id=" + highBidderId + " and b.property_id=" + data.property_id + " LIMIT 1";
											pool.query(hidhBidderQuery, function (err, highBidder) {
												if (err) {
													//check logger wokring
													logger.log("error", 'ERROR QUERY : ' + hidhBidderQuery);
													socket.emit("checkInsiderAuctionDashboard", { "data": result.rows[0], "high_bidder_data": {}, "msg": "Fetch data High Bidder error found", "status": 201, "error": 0 });
													return 0;
												}
												else {
													highBidder = (highBidder.rows[0] == undefined) ? {} : highBidder.rows[0]
													socket.emit("checkInsiderAuctionDashboard", { "data": result.rows[0], "high_bidder_data": highBidder, "msg": "Fetch data High bidder found", "status": 201, "error": 0 });
												}
											});
										} else {
											socket.emit("checkInsiderAuctionDashboard", { "data": result.rows[0], "high_bidder_data": {}, "msg": "Fetch data No high bidder", "status": 201, "error": 0 });
										}
									} else {
										socket.emit("checkInsiderAuctionDashboard", { 'msg': 'No Data Found.', 'status': 400, "error": 1 });
									}
								}
							});
						}
						else {
							socket.emit("checkInsiderAuctionDashboard", { 'msg': 'Invalid request.', 'status': 400, "error": 1 });
						}
					}
				});
			}
			else {
				socket.emit("checkInsiderAuctionDashboard", { 'msg': 'Invalid request.', 'status': 403, "error": 1 });
			}
		}
		catch (err) {
			logger.log("error", 'AUCTION Dash ERROR : ' + err.message);
			return 0;
		}
	},

	deleteCurrentBid: function (socket, data) {
		try {
			if (data.domain_id == undefined || data.domain_id == "" || data.domain_id <= 0 || data.user_id == undefined || data.user_id == "" || data.property_id == undefined || data.property_id == "") {
				socket.emit("deleteCurrentBid", { "msg": "Missing params", "status": 400, "error": 1 });
			}
			let auctionQry = "select * from bid where domain_id=" + parseInt(data.domain_id) + " and property_id=" + parseInt(data.property_id) + " ORDER BY id desc LIMIT 1;";
			pool.query(auctionQry, function (err, result) {
				if (err) {
					logger.log("error", 'ERROR AUCTION STATUS QUERY : ' + auctionQry);
					socket.in(data.property_id + '_' + data.user_id).emit("deleteCurrentBid", { "msg": err, "status": 400, "error": 1 });
					return 0;
				}
				else {
					//if record found 
					if (result.rowCount > 0) {
						//lets check if reserve met update to sold else expire
						let rows = result.rows[0];
						let user_id = rows['user_id'];
						let row_id = rows['id'];
						if (parseInt(data.user_id) == parseInt(user_id)) {
							auctionQl = "Delete FROM bid WHERE id = " + row_id + " AND property_id = " + parseInt(data.property_id) + " and domain_id=" + parseInt(data.domain_id);
							executeQuery(auctionQl);
							socket.emit("deleteCurrentBid", { "msg": "Successfully Deleted", "status": 201, "error": 0 });
							return 0;
						} else {
							socket.emit("deleteCurrentBid", { "msg": "You can not delete this,because you are not highest bidder.", "status": 201, "error": 1 });
							return 0;
						}
					} else {
						socket.emit("deleteCurrentBid", { "msg": "Record not found", "status": 400, "error": 1 });
					}
				}
			});
		}
		catch (err) {
			logger.log("error", 'AUCTION STATUS ERROR : ' + err.message);
			return 0;
		}
	},

	loadChatRooms: function loadChatRooms(socket, data) {
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
			if (parseInt(data.user_id) > 0 && data.user_type) {
				user_id = parseInt(data.user_id)
				if (data.user_type != 'agent' && data.user_type != 'buyer' && data.user_type != 'broker' && data.user_type != 'admin') {
					logger.log("error", 'ERROR : Missing required parameter');
					socket.emit("loadChatRooms", { "msg": "Forbidden.", "status": 400, "error": 1 });
					return 0;
				}

				let chatRoomQ1 = "SELECT " +
					"mc.*, " +
					"ch.id as child_id, " +
					"ch.added_on as child_added_on, " +
					"us.site_id as seller_site_id, " +
					"ch.sender_id, " +
					"ch.is_read , " +
					"CASE " +
					"WHEN mc.seller_id = " + user_id + " THEN upb.doc_file_name " +
					"ELSE ups.doc_file_name " +
					"END AS doc_file_name, " +
					"CASE " +
					"WHEN mc.seller_id = " + user_id + " THEN upb.bucket_name " +
					"ELSE ups.bucket_name " +
					"END AS bucket_name, " +
					"CASE " +
					"WHEN mc.seller_id = " + user_id + " THEN CONCAT(ub.first_name,' ', ub.last_name) " +
					"ELSE CONCAT(us.first_name,' ', us.last_name) " +
					"END AS name, " +
					"CASE " +
					"WHEN mc.seller_id = " + user_id + " THEN ub.email " +
					"ELSE us.email " +
					"END AS email, " +
					"CASE " +
					"WHEN mc.seller_id = " + user_id + " THEN ub.phone_no " +
					"ELSE us.phone_no " +
					"END AS phone_no, " +
					"CASE  " +
					"WHEN mc.property_id is not null  THEN CONCAT(pl.address_one, ', ', pl.city, ',' , ls.state_name,', ', pl.postal_code ) " +
					"ELSE '' " +
					"END as property_name, " +
					"(select count(*) from chat where master_id=mc.id and receiver_id = " + user_id + " and status_id=1 and is_read=false) as unread_msg_cnt, " +
					"ch.message," +
					"cd.document_id as chat_document_id," +
					"upc.doc_file_name as chat_document_file_name," +
					"upc.bucket_name as chat_document_bucket_name, " +
					"nd.domain_name as domain_name " +
					"from master_chat mc " +
					"JOIN chat ch on ch.master_id = mc.id " +
					"JOIN users us on us.id = mc.seller_id " +
					"JOIN users ub on ub.id = mc.buyer_id " +
					"LEFT JOIN " +
					"(" +
					"SELECT    MAX(id) max_id, chat_id " +
					"FROM      chat_documents " +
					"GROUP BY  chat_id " +
					") cd_max ON (ch.id = cd_max.chat_id) " +
					"LEFT JOIN chat_documents cd on cd.id = cd_max.max_id " +
					"LEFT JOIN  user_uploads upc on upc.id = cd.document_id " +
					"LEFT JOIN property_listing pl on pl.id = mc.property_id " +
					"LEFT JOIN network_domain nd on nd.id = pl.domain_id " +
					"LEFT JOIN lookup_state ls on ls.id = pl.state_id " +
					"LEFT JOIN user_uploads ups on ups.id = NULLIF(us.profile_image, '')::int " +
					"LEFT JOIN user_uploads upb on upb.id = NULLIF(ub.profile_image, '')::int " +
					"LEFT OUTER JOIN chat ch2 ON (mc.id = ch2.master_id " +
					"AND (ch.added_on < ch2.added_on " +
					"OR (ch.added_on = ch2.added_on AND ch.id < ch2.id))) " +
					"WHERE ch2.id IS NULL and mc.status_id = 1 and us.status_id = 1 and ub.status_id = 1"

				// if buyer
				if (data.user_type == "buyer") {
					chatRoomQ1 += " and mc.buyer_id=" + data.user_id + " and mc.domain_id=" + data.domain_id
				}

				// if admin
				if (data.user_type == 'broker' || data.user_type == 'agent') {
					chatRoomQ1 += " and mc.domain_id=" + data.domain_id
				}

				//  if agent
				if (data.user_type == 'agent') {
					chatRoomQ1 += " and mc.seller_id=" + data.user_id
				}

				// check if last message
				last_msg_id = (data.last_msg_id) ? parseInt(data.last_msg_id) : 0
				msg_type = (data.msg_type) ? data.msg_type : ''
				if (last_msg_id) {
					if (msg_type == 'pre_msg')
						chatRoomQ1 += " and ch.id < " + last_msg_id
					else
						chatRoomQ1 += " and ch.id > " + last_msg_id
				}

				// apply filter
				if (data.filter_data) {
					filter_data = data.filter_data.toString().toLowerCase();
					if (data.user_type == 'broker') { // for broker dash
						if (filter_data == 'buyer')
							chatRoomQ1 += " and mc.seller_id=" + data.user_id
						else if (filter_data == 'agent')
							chatRoomQ1 += " and mc.seller_id <> " + data.user_id

					} else if (data.user_type == 'agent') { // for agent dash

						if (filter_data == 'buyer')
							chatRoomQ1 += " and mc.seller_id=" + data.user_id
						else if (filter_data == 'broker')
							chatRoomQ1 += " and mc.buyer_id=" + data.user_id

					} else if (data.user_type == 'buyer') {
						if (filter_data == 'agent')
							chatRoomQ1 += " and (us.site_id!=" + data.domain_id + " or us.site_id is null) "
						else if (filter_data == 'broker')
							chatRoomQ1 += " and us.site_id=" + data.domain_id
					} else if (data.user_type == 'admin') {
						if (filter_data == 'buyer') {
							chatRoomQ1 += " and mc.seller_id != ch.sender_id"
						} else if (filter_data == 'agent') {
							chatRoomQ1 += " and mc.seller_id = ch.sender_id and (us.site_id != mc.domain_id  or us.site_id is null)"
						} else if (filter_data == 'broker') {
							chatRoomQ1 += " and mc.seller_id = ch.sender_id and us.site_id = mc.domain_id"
						}
					}
				}

				// order by last message
				chatRoomQ1 += " order by ch.id desc LIMIT 15";

				pool.query(chatRoomQ1, function (err, chatRoomRes) {
					if (err) {
						logger.log("error", 'ERROR QUERY : ' + chatRoomQ1);
						socket.emit("loadChatRooms", { "msg": err, 'status': 400, "error": 1 });
						return 0;
					}
					else {
						if (chatRoomRes.rowCount) {
							socket.emit("loadChatRooms", { "data": chatRoomRes.rows, 'msg_type': msg_type, "status": 201, "error": 0 });
						}
						else {
							socket.emit("loadChatRooms", { "msg": "No chat room found", data: [], 'msg_type': msg_type, "status": 201, "error": 0 });
						}
						return 0;
					}
				});
			}
			else {
				//required parameter not found
				logger.log("error", 'ERROR : Missing required parameter');
				socket.emit("loadChatRooms", { "msg": "Forbidden.", "status": 400, "error": 1 });
				return 0;
			}
		} catch (err) {
			//trapped error
			logger.log("error", 'ERROR : ' + err.message);
			socket.emit("loadChatRooms", { "msg": err.message, "status": 400, "error": 1 });
			return 0;
		}

	},

	loadChatRoomConversation: function loadChatRoomConversation(socket, data) {
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
			if (parseInt(data.user_id) > 0 && data.master_id && data.user_type) {
				user_id = parseInt(data.user_id)
				if (data.user_type == 'broker' || data.user_type == 'agent') {
					disabledChatCase = "CASE WHEN mc.seller_id = " + user_id + " THEN false ELSE true END as disable_chat, "
				} else if (data.user_type == 'buyer') {
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
					"cd.document_id as chat_document_id, " +
					"upc.doc_file_name as chat_document_file_name, " +
					"upc.bucket_name as chat_document_bucket_name " +
					"from chat ch " +
					"LEFT JOIN chat_documents cd on cd.chat_id = ch.id " +
					"LEFT JOIN  user_uploads upc on upc.id = cd.document_id " +
					"JOIN master_chat mc on mc.id = ch.master_id " +
					"JOIN users us on us.id = ch.sender_id " +
					"JOIN users useller on useller.id = mc.seller_id " +
					"LEFT JOIN user_uploads ups on ups.id = NULLIF(us.profile_image, '')::int " +
					"WHERE mc.status_id = 1 and ch.master_id=" +
					data.master_id + " and ch.status_id=1 and us.status_id = 1 and useller.status_id = 1 ";

				// check if last message
				last_msg_id = (data.last_msg_id) ? parseInt(data.last_msg_id) : 0
				msg_type = (data.msg_type) ? data.msg_type : ''
				if (last_msg_id) {
					if (msg_type == 'pre_msg')
						chatRoomConversation += " and ch.id < " + last_msg_id
					else
						chatRoomConversation += " and ch.id > " + last_msg_id
				}

				// order by last message
				chatRoomConversation += " order by ch.id desc LIMIT 15";

				pool.query(chatRoomConversation, function (err, chatRoomRes) {
					if (err) {
						logger.log("error", 'ERROR QUERY : ' + chatRoomConversation);
						socket.emit("loadChatRoomConversation", { "msg": err, 'status': 400, "error": 1 });
						return 0;
					}
					else {
						if (chatRoomRes.rowCount) {
							if (chatRoomRes.rows[0].disable_chat == false) {
								// mark message as read
								try {
									let updateIsRead = "UPDATE chat SET is_read=true WHERE is_read=false and master_id = " + data.master_id + " and receiver_id=" + user_id;
									pool.query(updateIsRead, function (err) {
										if (err) {
											logger.log("error", 'ERROR QUERY : ' + updateIsRead);
										}
									});
								} catch (error) {
									logger.log("error", 'ERROR QUERY : ' + updateIsRead);
								}
							}
							socket.emit("loadChatRoomConversation", { "data": chatRoomRes.rows, 'msg_type': msg_type, "status": 201, "error": 0 });
						}
						else {
							socket.emit("loadChatRoomConversation", { "msg": "No message found in room", data: [], 'msg_type': msg_type, "status": 201, "error": 0 });
						}
						return 0;
					}
				});
			}
			else {
				//required parameter not found
				logger.log("error", 'ERROR : Missing required parameter');
				socket.emit("loadChatRoomConversation", { "msg": "Forbidden.", "status": 400, "error": 1 });
				return 0;
			}
		} catch (err) {
			//trapped error
			logger.log("error", 'ERROR : ' + err.message);
			socket.emit("loadChatRoomConversation", { "msg": err.message, "status": 400, "error": 1 });
			return 0;
		}

	},

	sendMessageToUser: function sendMessageToUser(socket, data) {
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
			if (parseInt(data.user_id) > 0 && data.master_id && data.domain_id && (data.message || data.chat_doc_ids.length > 0)) {
				user_id = parseInt(data.user_id)

				checkAuthorization = 'select ' +
					'CASE ' +
					'WHEN mc.buyer_id = ' + user_id + '  THEN mc.seller_id ' +
					'ELSE mc.buyer_id ' +
					'END as receiver_id  ' +
					'from users us ' +
					'join master_chat mc on mc.buyer_id=us.id or mc.seller_id=us.id ' +
					'left outer join network_user nu on nu.domain_id = mc.domain_id and nu.user_id = us.id ' +
					'where ' +
					'mc.id=' + data.master_id + ' and ' +
					'mc.domain_id=' + data.domain_id + ' and ' +
					'mc.status_id=1 and ' +
					'us.id = ' + user_id + ' and ' +
					'us.status_id=1 and ' +
					'(mc.seller_id=' + user_id + ' or mc.buyer_id=' + user_id + ') and ' +
					'((us.user_type_id=2 and us.site_id=' + data.domain_id + ') or ' +
					'nu.status_id =1)'

				pool.query(checkAuthorization, function (err, authRes) {
					if (err) {
						logger.log("error", 'ERROR QUERY : ' + checkAuthorization);
						socket.emit("sendMessageToUser", { "msg": err, 'status': 400, "error": 1 });
						return 0;
					}
					else {
						if (authRes.rows[0] && authRes.rows[0].receiver_id) {
							if (data.message != '') {
								message = data.message
							} else {
								message = ''
							}
							let saveChatResponse = "INSERT into chat (added_on, updated_on, master_id, message, sender_id, receiver_id, status_id, is_read) " +
								"VALUES(now(), now(), " + data.master_id + ",'" + message + "'," + user_id + ", " + authRes.rows[0].receiver_id + ", 1, false) returning *";
							pool.query(saveChatResponse, function (err, saveChatRes) {
								if (err) {
									console.log(err)
									logger.log("error", 'ERROR QUERY : ' + saveChatResponse);
									socket.emit("sendMessageToUser", { "msg": err, 'status': 400, "error": 1 });
									return 0;
								}
								else {
									if (saveChatRes.rowCount) {
										if (saveChatRes.rows[0].id && data.chat_doc_ids) {
											// insert data documents
											for (const val of data.chat_doc_ids) {
												bidAddQl = "INSERT into chat_documents (chat_id, document_id, added_on, updated_on)";
												bidAddQl += "VALUES(" + saveChatRes.rows[0].id + ", " + val + ", now(), now())";
												executeQuery(bidAddQl);
											}
										}
										socket.emit("sendMessageToUser", { "msg": 'Message sent successfully', 'msg_type': msg_type, "status": 201, "error": 0 });
									}
									return 0;
								}
							});
						}
					}
				});
			}
			else {
				//required parameter not found
				logger.log("error", 'ERROR : Missing required parameter');
				socket.emit("sendMessageToUser", { "msg": "Forbidden.", "status": 400, "error": 1 });
				return 0;
			}
		} catch (err) {
			//trapped error
			logger.log("error", 'ERROR : ' + err.message);
			socket.emit("sendMessageToUser", { "msg": err.message, "status": 400, "error": 1 });
			return 0;
		}

	},

	userMessageCount: function userMessageCount(socket, data) {
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
			if (parseInt(data.user_id) > 0 && data.domain_id && data.user_type) {
				user_id = parseInt(data.user_id)

				userMessageCnt = 'select count(*) from chat as c ' +
					'join master_chat mc on mc.id = c.master_id ' +
					'where c.is_read=false ' +
					'and mc.status_id=1 ' +
					'and c.status_id=1 ';

				// include only domain specific count for users
				if (data.user_type == 'broker' || data.user_type == 'agent' || data.user_type == 'buyer') {
					userMessageCnt += ' and mc.domain_id = ' + data.domain_id
				}

				//filter for agents under brokers
				if (data.user_type == 'agent') {
					userMessageCnt += ' and mc.seller_id=' + user_id + ' and c.receiver_id=' + user_id
				}

				if (data.user_type == 'broker') {
					userMessageCnt += ' and c.receiver_id=' + user_id
				}

				// filter for buyer under agents/broker
				if (data.user_type == 'buyer') {
					userMessageCnt += ' and mc.buyer_id=' + user_id + ' and c.receiver_id=' + user_id
				}

				pool.query(userMessageCnt, function (err, countRes) {
					if (err) {
						logger.log("error", 'ERROR QUERY : ' + userMessageCnt);
						socket.emit("userMessageCount", { "msg": err, 'status': 400, "error": 1 });
						return 0;
					}
					else {
						socket.emit("userMessageCount", { "data": countRes.rows, 'user_type': data.user_type, "status": 201, "error": 0 });
					}
				});
			}
			else {
				//required parameter not found
				logger.log("error", 'ERROR : Missing required parameter');
				socket.emit("userMessageCount", { "msg": "Forbidden.", "status": 400, "error": 1 });
				return 0;
			}
		} catch (err) {
			//trapped error
			logger.log("error", 'ERROR : ' + err.message);
			socket.emit("userMessageCount", { "msg": err.message, "status": 400, "error": 1 });
			return 0;
		}

	},
}

/**
 * This js funciton display number with thousand separator
 */
function convertToDisplayFormat(num) {
	var removeTrailingZero = parseFloat(num).toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
	removeTrailingZero = removeTrailingZero.replace('.00', '');
	return removeTrailingZero;
}

function executeQuery(sql) {
	if (sql) {

		try {
			pool.query(sql, function (err, result) {
				if (err) {
					logger.log("error", 'ERROR BID QUERY : ' + sql);
					return 0;
				}
				else {

					return "ok";
				}
			});
		}
		catch (err) {
			logger.log("error", 'BID ERROR : ' + err.message);
			return 0;
		}
	}
}

function messageAPICall(postData) {
	return;
	const https = require(global.config.API_PROTOCOL);
	const data = postData;
	const options = {
		hostname: global.config.API_URL,
		port: global.config.API_PORT,
		path: '/api-bid/send-bid-email/',
		method: 'POST',
		rejectUnauthorized: false,
		headers: {
			'Content-Type': 'application/json;',
			'Connection': 'keep-alive',
			'Content-Length': data.length,
			'Authorization': global.config.API_TOKEN
		}
	};

	let reqPost = https.request(options, function (res) {
		if (res.statusCode !== 200) {
			// to do
			logger.log("error", 'Email Error : Code ' + res.statusMessage + "; Message :" + res.statusMessage);
		}

		res.on('data', (d) => {
			process.stdout.write(d);
		})

		res.on('end', () => {
			global.io.emit("syncNotifications")
		});
	});


	/** Log error*/
	reqPost.on('error', function (e) {
		console.error(e.code);
		console.error(e);
		logger.log("error", 'ERROR MESSAGE : ' + e);
	});

	reqPost.write(data);
	//gracely close connection	
	reqPost.end();
}

/**
 * this function used to call api
 *
 */

function listingSettingAPICall() {
	return;
	postData = JSON.stringify(
		{
			"domain_id": 68,
			"property_id": 74
		}
	);
	//const https = require('https');
	const https = require(global.config.API_PROTOCOL);
	const data = postData;
	console.log(data.length);
	const options = {
		//   hostname: global.config.API_URL,
		//   port: 443,
		hostname: global.config.API_URL,
		port: global.config.API_PORT,
		path: '/api-property/property-setting/',
		method: 'POST',
		rejectUnauthorized: false,
		headers: {
			'Content-Type': 'application/json;',
			'Connection': 'keep-alive',
			'Content-Length': data.length,
			'Authorization': global.config.API_TOKEN
		}
	};
	let reqPost = https.request(options, function (res) {

		if (res.statusCode !== 200) {
			// to do
			logger.log("error", 'Email Error : Code ' + res.statusMessage + "; Message :" + res.statusMessage);
		}

		reqPost.on('data', (d) => {
			process.stdout.write(d);
		})
	});


	/** Log error*/
	reqPost.on('error', function (e) {
		console.error(e);
		logger.log("error", 'ERROR MESSAGE : ' + e);
	});

	reqPost.write(data);
	//gracely close connection
	reqPost.end();
}

/**
 * This function create message body and send message
 * @param 1 : template id as int required
 * @param 2 : profile id as int required
 * @param 3 : listing id as int required
 * @param 4 : optional
 * @param 5 : optional
 * @param 6 : optional
 */
function createSendMessage(domain_id, property_id, user_id, bid_amount = 0) {
	//    extraFields = {
	//         "days": 1,
	//         "send_alert": true,
	//         "send_msg": true
	//    }
	returnBody = JSON.stringify(
		{
			"domain_id": domain_id,
			"property_id": property_id,
			"user_id": user_id,
			"bid_amount": bid_amount
		}
	);
	//console.log(returnBody);
	msg = messageAPICall(returnBody);
	return;
}

function sendAuctionStartMessage(domain_id, property_id) {
	returnBody = JSON.stringify(
		{
			"domain_id": domain_id,
			"property_id": property_id,
		}
	);
	msg = sendAuctionStartAPI(returnBody);
	return;
}

function sendAuctionStartAPI(postData) {
	return;
	const https = require(global.config.API_PROTOCOL);
	const data = postData;
	const options = {
		hostname: global.config.API_URL,
		port: global.config.API_PORT,
		path: '/api-property/send-auction-start-email/',
		method: 'POST',
		rejectUnauthorized: false,
		headers: {
			'Content-Type': 'application/json;',
			'Connection': 'keep-alive',
			'Content-Length': data.length,
			'Authorization': global.config.API_TOKEN
		}
	};

	let reqPost = https.request(options, function (res) {
		if (res.statusCode !== 200) {
			// to do
			logger.log("error", 'Email Error : Code ' + res.statusMessage + "; Message :" + res.statusMessage);
		}

		res.on('data', (d) => {
			process.stdout.write(d);
		})

		res.on('end', () => {
			global.io.emit("syncNotifications")
		});

	});


	/** Log error*/
	reqPost.on('error', function (e) {
		console.error(e.code);
		console.error(e);
		logger.log("error", 'ERROR MESSAGE : ' + e);
	});

	reqPost.write(data);
	//gracely close connection
	reqPost.end();
}

function sendMessage(domain_id, property_id, user_id, bid_amount = 0) {
	returnBody = JSON.stringify(
		{
			"domain_id": domain_id,
			"property_id": property_id,
			"user_id": user_id,
			"bid_amount": bid_amount
		}
	);
	msg = emailAPICall(returnBody);
	return;
}

function emailAPICall(postData) {
	return;
	// const https = require('https');
	const https = require(global.config.API_PROTOCOL);
	const data = postData;
	const options = {
		hostname: global.config.API_URL,
		port: global.config.API_PORT,
		//port: 443,
		//          host : '127.0.0.1',
		//          port : 8000,
		path: '/api-bid/send-english-auction-email/',
		method: 'POST',
		rejectUnauthorized: false,
		headers: {
			'Content-Type': 'application/json;',
			'Connection': 'keep-alive',
			'Content-Length': data.length,
			'Authorization': global.config.API_TOKEN
		}
	};

	let reqPost = https.request(options, function (res) {
		if (res.statusCode !== 200) {
			// to do
			logger.log("error", 'Email Error : Code ' + res.statusMessage + "; Message :" + res.statusMessage);
		}

		res.on('data', (d) => {
			process.stdout.write(d);
		})

		res.on('end', () => {
			global.io.emit("syncNotifications")
		});

	});


	/** Log error*/
	reqPost.on('error', function (e) {
		console.error(e.code);
		console.error(e);
		logger.log("error", 'ERROR MESSAGE : ' + e);
	});

	reqPost.write(data);
	//gracely close connection
	reqPost.end();
}

function autoBidFirst(socket, data, nextBidAmount, userHighestAutoBidAmountAuctionId, userHighestAutoBidAmountUserId, userHighestAutoBidAmountDomainId, userHighestAutoBidAmountRegistrationId) {
	let bidAddQry = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
	bidAddQry += "VALUES(now()," + nextBidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + data.user_id + ", '1', '0', " + data.domain_id + ", '" + data.ip_address + "', " + data.registration_id + ")";
	pool.query(bidAddQry, function (err, addBidRes) {
		if (err) {
			logger.log("error", 'ERROR QUERY : ' + bidAddQry);
			socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
			return 0;
		}
		else {
			// Bidding for auto bid
			nextBidAmount = parseInt(nextBidAmount) + parseInt(data.bid_increment);
			if (nextBidAmount > data.auto_bid_amount) {
				let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
				bidAddQl += "VALUES(now()," + nextBidAmount + ",'1','1','0','0'," + data.property_id + "," + userHighestAutoBidAmountAuctionId + "," + userHighestAutoBidAmountUserId + ", '2', '0', " + userHighestAutoBidAmountDomainId + ", '', " + userHighestAutoBidAmountRegistrationId + ")";
				pool.query(bidAddQl, function (err, addBidRes) {
					if (err) {
						logger.log("error", 'ERROR QUERY : ' + bidAddQl);
						socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
						return 0;
					} else {
						var currentBidAmt = parseInt(nextBidAmount) - parseInt(data.bid_increment);
						createSendMessage(data.domain_id, data.property_id, userHighestAutoBidAmountUserId, nextBidAmount);
						socket.emit("setAutoBid", { "msg": "Autobid amount set successfully.", "status": 201, "error": 0 });
						return true;
					}
				});
				return true;
			} else {
				let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
				bidAddQl += "VALUES(now()," + nextBidAmount + ",'1','1','0','0'," + data.property_id + "," + userHighestAutoBidAmountAuctionId + "," + userHighestAutoBidAmountUserId + ", '2', '0', " + userHighestAutoBidAmountDomainId + ", '', " + userHighestAutoBidAmountRegistrationId + ")";

				pool.query(bidAddQl, function (err, addBidRes) {
					if (err) {
						logger.log("error", 'ERROR QUERY : ' + bidAddQl);
						socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
						return 0;
					} else {
						nextBidAmount = parseInt(nextBidAmount) + parseInt(data.bid_increment);
						autoBidFirst(socket, data, nextBidAmount, userHighestAutoBidAmountAuctionId, userHighestAutoBidAmountUserId, userHighestAutoBidAmountDomainId, userHighestAutoBidAmountRegistrationId);
					}
				});
			}
		}
	});

}

function autoBidSecond(socket, data, nextBidAmount, userHighestAutoBidAmountAuctionId, userHighestAutoBidAmountUserId, userHighestAutoBidAmountDomainId, userHighestAutoBidAmountRegistrationId) {
	let bidAddQry = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
	bidAddQry += "VALUES(now()," + nextBidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + data.user_id + ", '1', '0', " + data.domain_id + ", '" + data.ip_address + "', " + data.registration_id + ")";
	pool.query(bidAddQry, function (err, addBidRes) {
		if (err) {
			logger.log("error", 'ERROR QUERY : ' + bidAddQry);
			socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
			return 0;
		}
		else {
			// Bidding for auto bid
			nextBidAmount = parseInt(nextBidAmount) + parseInt(data.bid_increment);
			if (nextBidAmount >= data.auto_bid_amount - parseInt(data.bid_increment)) {
				let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
				bidAddQl += "VALUES(now()," + nextBidAmount + ",'1','1','0','0'," + data.property_id + "," + userHighestAutoBidAmountAuctionId + "," + userHighestAutoBidAmountUserId + ", '2', '0', " + userHighestAutoBidAmountDomainId + ", '', " + userHighestAutoBidAmountRegistrationId + ")";
				pool.query(bidAddQl, function (err, addBidRes) {
					if (err) {
						logger.log("error", 'ERROR QUERY : ' + bidAddQl);
						socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
						return 0;
					} else {
						var currentBidAmt = parseInt(nextBidAmount) - parseInt(data.bid_increment);
						createSendMessage(data.domain_id, data.property_id, userHighestAutoBidAmountUserId, nextBidAmount);
						socket.emit("setAutoBid", { "msg": "Autobid amount set successfully.", "status": 201, "error": 0 });
						return true;
					}
				});
				return true;
			} else {
				let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
				bidAddQl += "VALUES(now()," + nextBidAmount + ",'1','1','0','0'," + data.property_id + "," + userHighestAutoBidAmountAuctionId + "," + userHighestAutoBidAmountUserId + ", '2', '0', " + userHighestAutoBidAmountDomainId + ", '', " + userHighestAutoBidAmountRegistrationId + ")";
				pool.query(bidAddQl, function (err, addBidRes) {
					if (err) {
						logger.log("error", 'ERROR QUERY : ' + bidAddQl);
						socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
						return 0;
					} else {
						nextBidAmount = parseInt(nextBidAmount) + parseInt(data.bid_increment);
						autoBidSecond(socket, data, nextBidAmount, userHighestAutoBidAmountAuctionId, userHighestAutoBidAmountUserId, userHighestAutoBidAmountDomainId, userHighestAutoBidAmountRegistrationId);
					}
				});
			}
		}
	});

}

function autoBid(socket, data, nextBidAmount, userHighestAutoBidAmount, userHighestAutoBidAmountAuctionId, userHighestAutoBidAmountUserId, userHighestAutoBidAmountDomainId, userHighestAutoBidAmountRegistrationId) {
	if (nextBidAmount >= userHighestAutoBidAmount) {
		socket.emit("setAutoBid", { "msg": "Autobid amount set successfully.", "status": 201, "error": 0 });
		let bidAddQry = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
		bidAddQry += "VALUES(now()," + nextBidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + data.user_id + ", '2', '0', " + data.domain_id + ", '" + data.ip_address + "', " + data.registration_id + ")";
		pool.query(bidAddQry, function (err, addBidRes) {
			if (err) {
				logger.log("error", 'ERROR QUERY : ' + bidAddQry);
				socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
				return 0;
			} else {
				var currentBidAmt = parseInt(nextBidAmount) - parseInt(data.bid_increment);
				createSendMessage(data.domain_id, data.property_id, data.user_id, nextBidAmount);
				return true;
			}
		});
		return true;
	}
	let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
	bidAddQl += "VALUES(now()," + nextBidAmount + ",'1','1','0','0'," + data.property_id + "," + data.auction_id + "," + data.user_id + ", '2', '0', " + data.domain_id + ", '" + data.ip_address + "', " + data.registration_id + ")";
	pool.query(bidAddQl, function (err, addBidRes) {
		if (err) {
			logger.log("error", 'ERROR QUERY : ' + bidAddQl);
			socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
			return 0;
		} else {
			nextBidAmount = parseInt(nextBidAmount) + parseInt(data.bid_increment);
			let bidAddQry = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_retracted,is_canceled, property_id, auction_id, user_id, bid_type, selected_highest_bid, domain_id, ip_address, registration_id)";
			bidAddQry += "VALUES(now()," + nextBidAmount + ",'1','1','0','0'," + data.property_id + "," + userHighestAutoBidAmountAuctionId + "," + userHighestAutoBidAmountUserId + ", '1', '0', " + userHighestAutoBidAmountDomainId + ", '', " + userHighestAutoBidAmountRegistrationId + ")";
			pool.query(bidAddQry, function (err, addBidRes) {
				if (err) {
					logger.log("error", 'ERROR QUERY : ' + bidAddQry);
					socket.emit("setAutoBid", { "msg": "An error occurred that prevented your bid from being placed. Please try again.", "status": 400, "error": 1 });
					return 0;
				} else {
					nextBidAmount = parseInt(nextBidAmount) + parseInt(data.bid_increment);
					autoBid(socket, data, nextBidAmount, userHighestAutoBidAmount, userHighestAutoBidAmountAuctionId, userHighestAutoBidAmountUserId, userHighestAutoBidAmountDomainId, userHighestAutoBidAmountRegistrationId);
				}
			});
		}
	});
}

function getHolidaysByDate(year, month, day) {
	const hd = new Holidays('US');
	const holidays = hd.getHolidays(year);
	const publicHolidays = holidays.filter(holiday => holiday.type === 'public');
	// Filter holidays by the provided year, month, and dayconst
	filteredHolidays = publicHolidays.filter(holiday => {
		const holidayDate = new Date(holiday.date);
		const holidayYear = holidayDate.getFullYear();
		const holidayMonth = holidayDate.getMonth() + 1;// Get month (1-12)
		const holidayDay = holidayDate.getDate(); // Get day of the month
		return holidayYear === year && holidayMonth === month && holidayDay === day;
	});
	return filteredHolidays;
}

function adjustEndDate(auctionEndDate, logTimeExtension, recursive_end = '') {
	let adjustedDate = new Date(auctionEndDate);
	// If recursive_end is provided, return it
	if (recursive_end && recursive_end !== '') {
		return recursive_end;
	}
	// Apply the current log time extension
	adjustedDate.setTime(adjustedDate.getTime() + logTimeExtension * 60 * 1000);
	// Extract the year, month, and date
	const year = adjustedDate.getFullYear();
	const month = adjustedDate.getMonth() + 1; // Months are zero-based, so add 1
	const date = adjustedDate.getDate();
	// Check if the date is a public holiday
	const holidaysOnDate = getHolidaysByDate(year, month, date);
	if (holidaysOnDate.length > 0) {
		// Add 24 hours if it's a public holiday
		logTimeExtension = (parseInt(logTimeExtension) + 1440).toString();
		return adjustEndDate(auctionEndDate, logTimeExtension); // Return the recursive call
	}
	// Get the day of the week
	const options = { weekday: 'long' };
	const dayName = adjustedDate.toLocaleDateString('en-US', options).toLowerCase();
	// Check if the date is a Saturday or Sunday
	if (dayName === 'saturday') {
		// Add 48 hours if it's Saturday
		logTimeExtension = (parseInt(logTimeExtension) + 2880).toString();
		return adjustEndDate(auctionEndDate, logTimeExtension); // Return the recursive call
	} else if (dayName === 'sunday') {
		// Add 24 hours if it's Sunday
		logTimeExtension = (parseInt(logTimeExtension) + 1440).toString();
		return adjustEndDate(auctionEndDate, logTimeExtension); // Return the recursive call
	} else {
		// If it's neither a weekend nor a public holiday, return the final extension
		recursive_end = logTimeExtension;
		return adjustEndDate(auctionEndDate, logTimeExtension, recursive_end); // Return the recursive call
	}
}

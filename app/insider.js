/**
 * This module contains insider hybrid auction
 * update at user end. 
 * Date: Feb 28, 2022
 * Author: Gautam
 */ 
const moment = require('moment'); //for time conversion handling
const pool = require('../connection');
const logger = require('../app/logger');

const criticalLimitPercentage = 95;

module.exports = {
    /**
    * ------------------Dutch Auction--------------
    **/
    dutchAuction: async function dutchAuction(socket, data) {
	    try{
            if(data.property_id && data.user_id && data.auction_id && data.bid_amount && data.domain_id && data.ip_address){
                // lets check if property already sold
                let propertyQuery = "select id from property_listing where id = "+data.property_id+" and domain_id = "+data.domain_id+" and status_id=1 and sale_by_type_id=2;";
                pool.query(propertyQuery, function (err, startDateQueryRes) {
                   if(err){
                        logger.log("error", 'ERROR QUERY : ' + propertyQuery);
                        socket.emit("dutchAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                        return 0;
                    }
                    else{
                        //listingSettingAPICall();
                        if(startDateQueryRes.rowCount < 1){
                           socket.emit("dutchAuction", {"status":400,"msg": "Property sold.", "error": 1});
                           return 0;
                        }else{
                            // lets check if auction step
                            let auctionSteps = "select id from insider_auction_step_winner where property_id = "+data.property_id+" and domain_id = "+data.domain_id+" and status_id=1 and insider_auction_step=1;";
                            pool.query(auctionSteps, function (err, auctionStepQueryRes) {
                                if(err){
                                    logger.log("error", 'ERROR QUERY : ' + auctionSteps);
                                    socket.emit("dutchAuction", {"msg": "Step one already completed.","status":400, "error": 1});
                                    return 0;
                                }
                                else{
                                    //listingSettingAPICall();
                                    if(auctionStepQueryRes.rowCount > 0){
                                        socket.emit("dutchAuction", {"status":400,"msg": "Step one already completed.", "error": 1});
                                        return 0;
                                    }else{
                                        let chkQry = "select id from bid_registration where property_id = "+data.property_id+" and status_id = 1 and is_approved = 2 and is_reviewed = true and user_id ="+data.user_id;
                                        // console.log(chkQry);
                                        pool.query(chkQry, function (err, checkRes) {
                                            if(err){
                                                logger.log("error", 'ERROR QUERY : ' + chkQry);
                                                socket.emit("dutchAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                return 0;
                                            }
                                            else{
                                                if(checkRes.rowCount > 0){
                                                    var registration_detail = checkRes.rows[0];
                                                    let startDateQuery = "select id, dutch_pause_time, sealed_pause_time, english_time, sealed_time, start_price, insider_decreased_price, CEIL((extract (epoch from (dutch_end_time::timestamp - now()::timestamp))::float)/(60)) as time_left_min from property_auction where property_id = "+data.property_id+" and extract(epoch from ( start_date::timestamp -  now()::timestamp)) <= 0 and extract(epoch from ( dutch_end_time::timestamp -  now()::timestamp)) >= 0 order by id desc limit 1";

                                                    pool.query(startDateQuery, function (err, propertyAuction) {
                                                        if(err){
                                                            logger.log("error", 'ERROR QUERY : ' + startDateQuery);
                                                            socket.emit("dutchAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                            return 0;
                                                        }
                                                        else{
                                                            if(propertyAuction.rowCount > 0){
                                                                var auction_data = propertyAuction.rows[0];
                                                                const listing_agent_id = parseInt(auction_data.listing_agent_id);
                                                                const minBidAmount = parseInt(auction_data.start_price);
                                                                const bidAmount = parseInt(data.bid_amount);
                                                                const auction_id = parseInt(auction_data.id);
                                                                const ip_address = data.ip_address;
                                                                const insider_decreased_price = data.insider_decreased_price;
                                                                const dutch_pause_time = parseInt(auction_data.dutch_pause_time);
                                                                const sealed_time = parseInt(auction_data.sealed_time);
                                                                const sealed_pause_time = parseInt(auction_data.sealed_pause_time);
                                                                const english_time = parseInt(auction_data.english_time);

                                                                //Lets check if bid amount is correct
                                                                if(insider_decreased_price > bidAmount || minBidAmount < bidAmount){
                                                                    socket.emit("dutchAuction", {"status":400,"msg": "Please increase bid amount.", "error": 1});
                                                                    return 0;
                                                                }else{
                                                                    let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_canceled, property_id, auction_id, user_id, bid_type, domain_id, ip_address, auction_type, insider_auction_step)";
                                                                    bidAddQl += "VALUES(now(),"+bidAmount+",'1','1', '0',"+data.property_id+","+auction_id+","+data.user_id+", '2', "+data.domain_id+", '"+ip_address+"', 2, 1)";
                                                                    pool.query(bidAddQl,function (err, addBidRes) {
                                                                        if(err){
                                                                            // pool.end(() => {});
                                                                            logger.log("error", 'ERROR QUERY : ' + bidAddQl);
                                                                            socket.emit("dutchAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                                            return 0;
                                                                        }else{
                                                                            bid_id = ""
                                                                            let bidDataQ = "select id from bid where property_id="+data.property_id+ " and auction_type=2 and insider_auction_step=1 and is_canceled=false order by id desc limit 1";
                                                                            pool.query(bidDataQ,function (err, addBidRes) {
                                                                                if(err){
                                                                                    // pool.end(() => {});
                                                                                    logger.log("error", 'ERROR QUERY : ' + bidDataQ);
                                                                                    socket.emit("dutchAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                                                    return 0;
                                                                                }else{
                                                                                    var bid_data = addBidRes.rows[0];
                                                                                    bid_id = bid_data.id
                                                                                    let bidStepAddQl = "INSERT into insider_auction_step_winner (bid_id, amount,domain_id,user_id, property_id, auction_id, insider_auction_step, status_id, added_on, updated_on)";
                                                                                    bidStepAddQl += "VALUES("+bid_id+", "+bidAmount+", "+data.domain_id+", "+data.user_id+", "+data.property_id+","+auction_id+", 1, 1, now(), now())";
                                                                                    pool.query(bidStepAddQl,function (err, addBidRes) {
                                                                                        if(err){
                                                                                            //console.log(err)
                                                                                            // pool.end(() => {});
                                                                                            logger.log("error", 'ERROR QUERY : ' + bidStepAddQl);
                                                                                            socket.emit("dutchAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                                                            return 0;
                                                                                        }else{
                                                                                            add_sealed_start_time = dutch_pause_time;
                                                                                            add_sealed_end_time = dutch_pause_time + sealed_time;
                                                                                            add_english_start_time = dutch_pause_time + sealed_time + sealed_pause_time;
                                                                                            add_english_end_time = dutch_pause_time + sealed_time + sealed_pause_time + english_time;
                                                                                            let auctionUpdateAddQl = "update property_auction set dutch_end_time= now(), sealed_start_time= now() + INTERVAL '"+add_sealed_start_time+" minute', sealed_end_time= now() + INTERVAL '"+add_sealed_end_time+" minute', english_start_time= now() + INTERVAL '"+add_english_start_time+" minute', end_date= now() + INTERVAL '"+add_english_end_time+" minute'  where id="+auction_id+";";
                                                                                            pool.query(auctionUpdateAddQl,function (err, addBidRes) {
                                                                                                if(err){
                                                                                                    logger.log("error", 'ERROR QUERY : ' + auctionUpdateAddQl);
                                                                                                    socket.emit("dutchAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                                                                    return 0;
                                                                                                }
                                                                                            });
                                                                                        }
                                                                                    });
                                                                                }
                                                                            });
                                                                        }
                                                                    });
                                                                }
                                                            }else{
                                                                socket.emit("dutchAuction", {"msg": "Auction not exist.","status":400, "error": 1});
                                                                return 0;
                                                            }
                                                            socket.emit("dutchAuction", {"msg": "Bid Successfully.", "bid_amount": data.bid_amount, "status":201, "error": 0});
                                                            //---------------------Email send here----------------
                                                            createSendMessage(data.domain_id, data.property_id, data.user_id, parseInt(data.bid_amount), "dutch");
                                                            return 0;
                                                        }
                                                    });
                                                }else{
                                                    socket.emit("dutchAuction", {"msg": "Registration not approved for bidding.","status":400, "error": 1});
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
            }else{
				socket.emit("dutchAuction", {'msg': 'Invalid request.','status':400, 'error': 1});
				return 0;
			}
	    }catch(err){
            logger.log("error", 'ERROR : ' + err.message);
            socket.emit("dutchAuction",{"message":err.message,"status":500});
            return 0;
        }
	},

    dutchAuctionRateDecrease: async function dutchAuctionRateDecrease(socket, data) {
        try{
            let propertyQuery = "select id from property_listing where id = "+data.property_id+" and domain_id = "+data.domain_id+" and status_id=1 and sale_by_type_id=2;";
             pool.query(propertyQuery, function (err, startDateQueryRes) {
                if(err){
                    logger.log("error", 'ERROR QUERY : ' + dutchAuctionQuery);
                    socket.emit("dutchAuctionRateDecrease", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                    return 0;
                }else{
                    if(startDateQueryRes.rowCount < 1){
                        socket.emit("dutchAuctionRateDecrease", {"status":400,"msg": "Property sold.", "error": 1});
                        return 0;
                    }else{
                        let dutchAuctionQuery = "select id from insider_auction_step_winner where property_id = "+data.property_id+" and domain_id = "+data.domain_id+" and status_id=1 and insider_auction_step=1;";
                        pool.query(dutchAuctionQuery, function (err, startDateQueryRes) {
                            if(err){
                                logger.log("error", 'ERROR QUERY : ' + dutchAuctionQuery);
                                socket.emit("dutchAuctionRateDecrease", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                return 0;
                            }
                            else{
                                if(startDateQueryRes.rowCount > 0){
                                    socket.emit("dutchAuctionRateDecrease", {"status":400,"msg": "Dutch auction completed.", "error": 1});
                                    return 0;
                                }else{
                                    let auctionQuery = "select id, start_price, insider_decreased_price, dutch_time, insider_price_decrease, ((extract(epoch from ( now()::timestamp - start_date::timestamp ))::integer)) as dutch_remain_time from property_auction where property_id = "+data.property_id+" and domain_id = "+data.domain_id+" and status_id=1 and auction_id=2;";
                                    pool.query(auctionQuery, function (err, auctionQueryRes) {
                                        if(err){
                                            logger.log("error", 'ERROR QUERY : ' + auctionQuery);
                                            socket.emit("dutchAuctionRateDecrease", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                            return 0;
                                        }
                                        else{
                                            if(auctionQueryRes.rowCount < 1){
                                                socket.emit("dutchAuctionRateDecrease", {"status":400,"msg": "Property sold.", "error": 1});
                                                return 0;
                                            }else{

                                                var listing_data = auctionQueryRes.rows[0];
                                                insider_price_decrease = parseInt(listing_data.insider_price_decrease);
                                                step_time =  parseInt(listing_data.dutch_time);
                                                step_time_sec = step_time * 60;
                                                total_decrease_count =  step_time * 10;
                                                step_decrease_percentage =  insider_price_decrease / total_decrease_count;
                                                start_price = listing_data.start_price;
                                                insider_decreased_price = listing_data.insider_decreased_price;
                                                dutch_remain_time = parseInt(listing_data.dutch_remain_time);

                                                if(dutch_remain_time > 0 && step_time_sec >= dutch_remain_time){
                                                    let auctionAddQl = "";
                                                    for(var i = 1; i < total_decrease_count; i++)
                                                    {
                                                        step = Math.floor(dutch_remain_time / 6);
                                                        if (step > 0){
                                                            percentage = step_decrease_percentage * step;
                                                            percentage = percentage / 100;
                                                            amount = Math.ceil(start_price * percentage);
                                                            amount = parseInt(start_price - amount);
                                                            if (amount < insider_decreased_price){
                                                                auctionAddQl = "update property_auction set insider_decreased_price="+amount+" where property_id="+data.property_id+";";
                                                            }
                                                        }
                                                    }
                                                    if(auctionAddQl != ""){
                                                        pool.query(auctionAddQl,function (err, addAuctionRes) {
                                                            if(err){
                                                                logger.log("error", 'ERROR QUERY : ' + auctionAddQl);
                                                                socket.emit("dutchAuctionRateDecrease", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                                return 0;
                                                            }else{
                                                                socket.emit("dutchAuctionRateDecrease", {"msg": "Value decreased successfully.", "amount": amount, "status":201, "error": 0});
                                                                return 0;
                                                            }
                                                        });
                                                    }else{
                                                        socket.emit("dutchAuctionRateDecrease", {"msg": "Nothing to decrease.","status":201, "amount": insider_decreased_price, "error": 0});
                                                    }
                                                }else{
                                                    socket.emit("dutchAuctionRateDecrease", {"msg": "Bidding not started yet.","status":201, "error": 0});
                                                }
                                            }
                                        }
                                    });
                                }
                            }
                        });
                    }
                }
             });
        }catch(err){
            logger.log("error", 'ERROR : ' + err.message);
            socket.emit("dutchAuctionRateDecrease",{"message":err.message,"status":500});
            return 0;
        }
    },

    //-------------Dutch ended property check---------
    dutchAuctionEnded: function(socket, data){
		try{
            if (data.domain_id == undefined || data.domain_id == "" || data.domain_id <= 0){
                socket.emit("dutchAuctionEnded",{"msg":"Missing params", "status":400, "error": 1});
                return 0;
            }
            let auctionQry = "select auction_id AS id, property_id, time_left, auction_type from dutch_remain_property_view where time_left < 1 and domain_id="+ parseInt(data.domain_id)+ " and dutch_winning_amount is null;";
			pool.query(auctionQry, function (err, result) {
				if(err){
					logger.log("error", 'ERROR AUCTION STATUS QUERY : ' + auctionQry);
					 socket.emit("dutchAuctionEnded", {"msg": err, "status": 400, "error": 1});
					 return 0;
				}
				else{
					if(result.rowCount > 0){
						let rows = result.rows;
						rows.forEach(function(val){
						  statusQl = "Update property_listing SET status_id = 8, updated_on =now() WHERE id="+parseInt(val['property_id']) +" and domain_id="+parseInt(data.domain_id);
                          executeQuery(statusQl);

                          auctionQl = "Update property_auction SET status_id = 2 Where id = "+parseInt(val['id'])+" and domain_id="+parseInt(data.domain_id);
                          executeQuery(auctionQl);
						});
						socket.emit("dutchAuctionEnded", {"msg": "Successfully updated", "status": 201, "error": 0});
						return 0;
					}else{
					    socket.emit("dutchAuctionEnded", {"msg": "Record not found", "status": 400, "error": 1});
					    return 0;
					}

				}

			});
		}
		catch(err){
			logger.log("error", 'AUCTION STATUS ERROR : ' + err.message);
			return 0;
		}
	},

	sealedAuction: async function sealedAuction(socket, data) {
	    try{
            if(data.property_id && data.user_id && data.auction_id && data.bid_amount && data.domain_id && data.ip_address){
                // lets check if property already sold
                var error = 0
                let propertyQuery = "select id from property_listing where id = "+data.property_id+" and domain_id = "+data.domain_id+" and status_id=1 and sale_by_type_id=2;";
                pool.query(propertyQuery, function (err, startDateQueryRes) {
                   if(err){
                        logger.log("error", 'ERROR QUERY : ' + propertyQuery);
                        socket.emit("sealedAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                        return 0;
                    }
                    else{
                        //listingSettingAPICall();
                        if(startDateQueryRes.rowCount < 1){
                           socket.emit("sealedAuction", {"status":400,"msg": "Property sold.", "error": 1});
                           return 0;
                        }else{
                            // lets check if auction step
                            let auctionSteps = "select id, user_id, amount from insider_auction_step_winner where property_id = "+data.property_id+" and domain_id = "+data.domain_id+" and status_id=1 and insider_auction_step=1;";
                            pool.query(auctionSteps, function (err, auctionStepQueryRes) {
                                if(err){
                                    logger.log("error", 'ERROR QUERY : ' + auctionSteps);
                                    socket.emit("sealedAuction", {"msg": "Step one already completed.","status":400, "error": 1});
                                    return 0;
                                }
                                else{
                                    //listingSettingAPICall();
                                    if(auctionStepQueryRes.rowCount < 1){
                                        socket.emit("sealedAuction", {"status":400,"msg": "Step one not completed.", "error": 1});
                                        return 0;
                                    }else{
                                        var step_one_auction_data = auctionStepQueryRes.rows[0];
                                        step_one_winner_user_id = step_one_auction_data.user_id
                                        step_one_winner_amount = step_one_auction_data.amount

                                        let checkStepsTwo = "select id from insider_auction_step_winner where property_id = "+data.property_id+" and domain_id = "+data.domain_id+" and status_id=1 and insider_auction_step=2;";
                                        pool.query(checkStepsTwo, function (err, auctionStepQueryRes) {
                                            if(err){
                                                logger.log("error", 'ERROR QUERY : ' + checkStepsTwo);
                                                socket.emit("sealedAuction", {"msg": "Step one already completed.","status":400, "error": 1});
                                                return 0;
                                            }else{
                                                 if(auctionStepQueryRes.rowCount > 0){
                                                    socket.emit("dutchAuction", {"status":400,"msg": "Step two already completed.", "error": 1});
                                                    return 0;
                                                }else{
                                                    let chkQry = "select id from bid_registration where property_id = "+data.property_id+" and status_id = 1 and is_approved = 2 and is_reviewed = true and user_id ="+data.user_id;
                                                    // console.log(chkQry);
                                                    pool.query(chkQry, function (err, checkRes) {
                                                        if(err){
                                                            logger.log("error", 'ERROR QUERY : ' + chkQry);
                                                            socket.emit("sealedAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                            return 0;
                                                        }
                                                        else{
                                                            if(checkRes.rowCount > 0){
                                                                var registration_detail = checkRes.rows[0];
                                                                //let startDateQuery = "select id, dutch_pause_time, sealed_pause_time, english_time, sealed_time, start_price, insider_decreased_price, CEIL((extract (epoch from (dutch_end_time::timestamp - now()::timestamp))::float)/(60)) as time_left_min from property_auction where property_id = "+data.property_id+" order by id desc limit 1";
                                                                let startDateQuery = "select id, dutch_pause_time, sealed_pause_time, english_time, sealed_time, start_price, insider_decreased_price, CEIL((extract (epoch from (dutch_end_time::timestamp - now()::timestamp))::float)/(60)) as time_left_min from property_auction where property_id = "+data.property_id+" and extract(epoch from ( sealed_start_time::timestamp -  now()::timestamp)) <= 0 and extract(epoch from ( sealed_end_time::timestamp -  now()::timestamp)) >= 0 order by id desc limit 1";

                                                                pool.query(startDateQuery, function (err, propertyAuction) {
                                                                    if(err){
                                                                        logger.log("error", 'ERROR QUERY : ' + startDateQuery);
                                                                        socket.emit("sealedAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                                        return 0;
                                                                    }
                                                                    else{
                                                                        if(propertyAuction.rowCount > 0){
                                                                            var auction_data = propertyAuction.rows[0];
                                                                            const listing_agent_id = parseInt(auction_data.listing_agent_id);
                                                                            const minBidAmount = parseInt(auction_data.start_price);
                                                                            const bidAmount = parseInt(data.bid_amount);
                                                                            const auction_id = parseInt(auction_data.id);
                                                                            const ip_address = data.ip_address;
                                                                            const insider_decreased_price = data.insider_decreased_price;
                                                                            const dutch_pause_time = parseInt(auction_data.dutch_pause_time);
                                                                            const sealed_time = parseInt(auction_data.sealed_time);
                                                                            const sealed_pause_time = parseInt(auction_data.sealed_pause_time);
                                                                            const english_time = parseInt(auction_data.english_time);
                                                                            //Lets check if bid amount is correct
                                                                            if(step_one_winner_amount >= bidAmount){
                                                                                socket.emit("sealedAuction", {"status":400,"msg": "Please increase bid amount.", "error": 1});
                                                                                return 0;
                                                                            }else if(step_one_winner_user_id == data.user_id){
                                                                                socket.emit("sealedAuction", {"status":400,"msg": "You can't bid in round second.", "error": 1});
                                                                                return 0;
                                                                            }else{
                                                                                //let checkBidQl = "select id, bid_amount from bid where property_id="+data.property_id+" and insider_auction_step=2 and auction_type=2 and is_canceled=false order by id desc;";
                                                                                let checkBidQl = "select id, bid_amount from bid where property_id="+data.property_id+" and user_id="+data.user_id+" and insider_auction_step=2 and auction_type=2 and is_canceled=false order by id desc;";
                                                                                pool.query(checkBidQl,function (err, checkBidRes) {
                                                                                    if(err){
                                                                                        // pool.end(() => {});
                                                                                        logger.log("error", 'ERROR QUERY : ' + checkBidQl);
                                                                                        socket.emit("sealedAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                                                        return 0;
                                                                                    }else{
                                                                                        let bidAddQl = ""
                                                                                        if(checkBidRes.rowCount > 0){
                                                                                            var check_bid_data = checkBidRes.rows[0];
                                                                                            bid_amount = check_bid_data.bid_amount;
                                                                                            bid_id = check_bid_data.id;
                                                                                            if(bid_amount >= bidAmount){
                                                                                                socket.emit("sealedAuction", {"status":400,"msg": "Please increase bid amount.", "error": 1});
                                                                                                return 0;
                                                                                            }else{
                                                                                                bidAddQl = "update bid set bid_amount="+bidAmount+", bid_date=now(), ip_address='"+ip_address+"' where id="+bid_id+";";
                                                                                                pool.query(bidAddQl,function (err, addBidRes) {
                                                                                                    if(err){
                                                                                                        logger.log("error", 'ERROR QUERY : ' + bidAddQl);
                                                                                                        socket.emit("sealedAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                                                                        return 0;
                                                                                                    }else{
                                                                                                        socket.emit("sealedAuction", {"msg": "Bid Successfully.", "bid_amount": data.bid_amount, "status":201, "error": 0});
                                                                                                        //---------------------Email send here----------------
                                                                                                        createSendMessage(data.domain_id, data.property_id, data.user_id, parseInt(data.bid_amount), "sealed");
                                                                                                        return 0;
                                                                                                    }
                                                                                                });
                                                                                            }

                                                                                        }else{
                                                                                            bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_canceled, property_id, auction_id, user_id, bid_type, domain_id, ip_address, auction_type, insider_auction_step)";
                                                                                            bidAddQl += "VALUES(now(),"+bidAmount+",'1','1', '0',"+data.property_id+","+auction_id+","+data.user_id+", '2', "+data.domain_id+", '"+ip_address+"', 2, 2)";
                                                                                            pool.query(bidAddQl,function (err, addBidRes) {
                                                                                                if(err){
                                                                                                    // pool.end(() => {});
                                                                                                    logger.log("error", 'ERROR QUERY : ' + bidAddQl);
                                                                                                    socket.emit("sealedAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                                                                    return 0;
                                                                                                }else{
                                                                                                    socket.emit("sealedAuction", {"msg": "Bid Successfully.", "bid_amount": data.bid_amount, "status":201, "error": 0});
                                                                                                    //---------------------Email send here----------------
                                                                                                    createSendMessage(data.domain_id, data.property_id, data.user_id, parseInt(data.bid_amount), "sealed");
                                                                                                    return 0;
                                                                                                }
                                                                                            });
                                                                                        }
                                                                                    }
                                                                                });
                                                                            }
                                                                        }else{
                                                                            socket.emit("sealedAuction", {"msg": "Auction not exist.","status":400, "error": 1});
                                                                            return 0;
                                                                        }

                                                                    }
                                                                });
                                                            }else{
                                                                socket.emit("sealedAuction", {"msg": "Registration not approved for bidding.","status":400, "error": 1});
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
            }else{
				socket.emit("sealedAuction", {'msg': 'Invalid request.','status':400, 'error': 1});
				return 0;
			}
	    }catch(err){
            logger.log("error", 'ERROR : ' + err.message);
            socket.emit("sealedAuction",{"message":err.message,"status":500});
            return 0;
        }
	},

	//-------------Sealed bid ended property check---------
    sealedAuctionEnded: function(socket, data){
		try{
            if (data.domain_id == undefined || data.domain_id == "" || data.domain_id <= 0){
                socket.emit("sealedAuctionEnded",{"msg":"Missing params", "status":400, "error": 1});
                return 0;
            }
            let auctionQry = "select auction_id AS id, dutch_winning_amount, property_id, time_left, auction_type, dutch_user_id from sealed_remain_property_view where time_left < 1 and domain_id="+ parseInt(data.domain_id)+ " and dutch_winning_amount > 0;";
			pool.query(auctionQry, function (err, result) {
				if(err){
					logger.log("error", 'ERROR AUCTION STATUS QUERY : ' + auctionQry);
					 socket.emit("sealedAuctionEnded", {"msg": err, "status": 400, "error": 1});
					 return 0;
				}
				else{
					if(result.rowCount > 0){
						let rows = result.rows;
						rows.forEach(function(val){
						    dutch_user_id = parseInt(val['dutch_user_id']);
						    dutch_amount = parseInt(val['dutch_winning_amount']);
						    let bidCheckQry = "select id, user_id, bid_amount from bid where property_id="+val['property_id']+" and domain_id="+ parseInt(data.domain_id)+ " and auction_type=2 and insider_auction_step=2 and is_canceled=false order by bid_amount desc, id asc;";
                            pool.query(bidCheckQry, function (err, bidCheckRes) {
                                if(err){
                                    logger.log("error", 'ERROR AUCTION STATUS QUERY : ' + bidCheckQry);
                                     socket.emit("sealedAuctionEnded", {"msg": err, "status": 400, "error": 1});
                                     return 0;
                                }else{
                                    if(bidCheckRes.rowCount > 0){
                                        var bid_check_data = bidCheckRes.rows[0];
                                        bid_id = parseInt(bid_check_data.id);
                                        sealed_user_id = parseInt(bid_check_data.user_id);
						                sealed_amount = parseInt(bid_check_data.bid_amount);
						                let bidSealedStepQry = "select id from insider_auction_step_winner where property_id="+val['property_id']+" and domain_id="+ parseInt(data.domain_id)+ " and insider_auction_step=2 and status_id=1;";
                                        pool.query(bidSealedStepQry, function (err, bidSealedStepRes) {
                                            if(err){
                                                logger.log("error", 'ERROR AUCTION STATUS QUERY : ' + bidCheckQry);
                                                 socket.emit("sealedAuctionEnded", {"msg": err, "status": 400, "error": 1});
                                                 return 0;
                                            }else{
                                                if(bidSealedStepRes.rowCount < 1){
                                                    let statusQl = "INSERT into insider_auction_step_winner (bid_id, amount,domain_id,user_id, property_id, auction_id, insider_auction_step, status_id, added_on, updated_on)";
                                                    statusQl += "VALUES("+bid_id+", "+sealed_amount+", "+data.domain_id+", "+sealed_user_id+", "+parseInt(val['property_id'])+","+parseInt(val['id'])+", 2, 1, now(), now())";
                                                    executeQuery(statusQl);
                                                    //---------------------Email send here----------------
                                                    createSendMessage(data.domain_id, parseInt(val['property_id']), sealed_user_id, parseInt(sealed_amount), "sealed_ended");
                                                    return 0;
                                                }
                                            }
                                        });
                                    }else{
                                        statusQl = "Update property_listing SET status_id = 9, closing_status_id =16, updated_on =now(), sold_price="+dutch_amount+", winner_id = "+dutch_user_id+", date_sold =now() WHERE id="+parseInt(val['property_id'])+" and domain_id="+parseInt(data.domain_id);
                                        executeQuery(statusQl);

                                        auctionQl = "Update property_auction SET status_id = 2 Where id = "+parseInt(val['id'])+" and domain_id="+parseInt(data.domain_id);
                                        executeQuery(auctionQl);
                                        //---------------------Email send here----------------
                                        createSendMessage(data.domain_id, parseInt(val['property_id']), dutch_user_id, parseInt(dutch_amount), "english_ended");
                                        return 0;
                                    }
                                }
                            });
						});
						socket.emit("sealedAuctionEnded", {"msg": "Successfully updated", "status": 201, "error": 0});
						return 0;
					}else{
					    socket.emit("sealedAuctionEnded", {"msg": "Record not found", "status": 400, "error": 1});
					    return 0;
					}

				}

			});
		}
		catch(err){
			logger.log("error", 'AUCTION STATUS ERROR : ' + err.message);
			return 0;
		}
	},

	englishAuction: async function englishAuction(socket, data) {
	    try{
            if(data.property_id && data.user_id && data.auction_id && data.bid_amount && data.domain_id && data.ip_address){
                // lets check if property already sold
                var error = 0
                let propertyQuery = "select id, agent_id from property_listing where id = "+data.property_id+" and domain_id = "+data.domain_id+" and status_id=1 and sale_by_type_id=2;";
                pool.query(propertyQuery, function (err, startDateQueryRes) {
                   if(err){
                        logger.log("error", 'ERROR QUERY : ' + propertyQuery);
                        socket.emit("englishAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                        return 0;
                    }
                    else{
                        //listingSettingAPICall();
                        if(startDateQueryRes.rowCount < 1){
                           socket.emit("englishAuction", {"status":400,"msg": "Property sold.", "error": 1});
                           return 0;
                        }else{
                            // lets check if auction step
                            var start_date_data = startDateQueryRes.rows[0];
                            const listing_agent_id = start_date_data.agent_id;
                            let auctionSteps = "select id, user_id, amount from insider_auction_step_winner where property_id = "+data.property_id+" and domain_id = "+data.domain_id+" and status_id=1 and insider_auction_step=2;";
                            pool.query(auctionSteps, function (err, auctionStepQueryRes) {
                                if(err){
                                    logger.log("error", 'ERROR QUERY : ' + auctionSteps);
                                    socket.emit("englishAuction", {"msg": "Step one already completed.","status":400, "error": 1});
                                    return 0;
                                }
                                else{
                                    //listingSettingAPICall();
                                    if(auctionStepQueryRes.rowCount < 1){
                                        socket.emit("englishAuction", {"status":400,"msg": "Step two not completed.", "error": 1});
                                        return 0;
                                    }else{
                                        var step_two_auction_data = auctionStepQueryRes.rows[0];
                                        step_two_winner_amount = step_two_auction_data.amount;

                                        let chkQry = "select id from bid_registration where property_id = "+data.property_id+" and status_id = 1 and is_approved = 2 and is_reviewed = true and user_id ="+data.user_id;
                                        // console.log(chkQry);
                                        pool.query(chkQry, function (err, checkRes) {
                                            if(err){
                                                logger.log("error", 'ERROR QUERY : ' + chkQry);
                                                socket.emit("englishAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                return 0;
                                            }
                                            else{
                                                if(checkRes.rowCount > 0){
                                                    var registration_detail = checkRes.rows[0];
                                                    //let startDateQuery = "select id, dutch_pause_time, sealed_pause_time, english_time, sealed_time, start_price, insider_decreased_price, CEIL((extract (epoch from (dutch_end_time::timestamp - now()::timestamp))::float)/(60)) as time_left_min from property_auction where property_id = "+data.property_id+" order by id desc limit 1";
                                                    let startDateQuery = "select id, bid_increments, dutch_pause_time, sealed_pause_time, english_time, sealed_time, start_price, insider_decreased_price, CEIL((extract (epoch from (end_date::timestamp - now()::timestamp))::float)/(60)) as time_left_min from property_auction where property_id = "+data.property_id+" and extract(epoch from ( english_start_time::timestamp -  now()::timestamp)) <= 0 and extract(epoch from ( end_date::timestamp -  now()::timestamp)) >= 0 order by id desc limit 1";

                                                    pool.query(startDateQuery, function (err, propertyAuction) {
                                                        if(err){
                                                            logger.log("error", 'ERROR QUERY : ' + startDateQuery);
                                                            socket.emit("englishAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                            return 0;
                                                        }
                                                        else{
                                                            if(propertyAuction.rowCount > 0){
                                                                var auction_data = propertyAuction.rows[0];
                                                                //const listing_agent_id = parseInt(auction_data.listing_agent_id);
                                                                const minBidAmount = parseInt(auction_data.start_price);
                                                                const bidAmount = parseInt(data.bid_amount);
                                                                const auction_id = parseInt(auction_data.id);
                                                                const bid_increments = parseInt(auction_data.bid_increments);
                                                                const ip_address = data.ip_address;
                                                                const timeLeft = auction_data.time_left_min;
                                                                //Lets check if bid amount is correct
                                                                let checkStepUserQl = "select id from insider_auction_step_winner where status_id=1 and property_id="+data.property_id+" and user_id in("+data.user_id+");";
                                                                pool.query(checkStepUserQl,function (err, checkStepUserRes) {
                                                                    if(err){
                                                                        // pool.end(() => {});
                                                                        logger.log("error", 'ERROR QUERY : ' + checkStepUserQl);
                                                                        socket.emit("englishAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                                        return 0;
                                                                    }else{
                                                                        if(checkStepUserRes.rowCount < 1){
                                                                            socket.emit("englishAuction", {"msg": "You can't bid.","status":400, "error": 1});
                                                                        }else{
                                                                            let checkBidQl = "select id, bid_amount from bid where property_id="+data.property_id+" and auction_type=2 and is_canceled=false order by id desc;";
                                                                            pool.query(checkBidQl,function (err, checkBidRes) {
                                                                                if(err){
                                                                                    // pool.end(() => {});
                                                                                    logger.log("error", 'ERROR QUERY : ' + checkBidQl);
                                                                                    socket.emit("englishAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                                                    return 0;
                                                                                }else{
                                                                                    let bidAddQl = ""
                                                                                    if(checkBidRes.rowCount > 0){
                                                                                        var check_bid_data = checkBidRes.rows[0];
                                                                                        bid_amount = check_bid_data.bid_amount;
                                                                                        incremented_bid_amount = parseInt(bid_amount) + parseInt(bid_increments);
                                                                                        if(incremented_bid_amount > bidAmount){
                                                                                            socket.emit("englishAuction", {"status":400,"msg": "Please increase bid amount.", "error": 1});
                                                                                            return 0;
                                                                                        }else{
                                                                                            timeExtQry = "select l.id as property_id, ";
                                                                                            timeExtQry += "(select id from property_settings where property_id="+data.property_id+" and is_broker=false and is_agent=false order by id desc limit 1) as personal_id, ";
                                                                                            timeExtQry += "(select is_log_time_extension from property_settings where property_id="+data.property_id+" and is_broker=false and is_agent=false order by id desc limit 1) as personal_is_log_time_extension, ";
                                                                                            timeExtQry += "(select remain_time_to_add_extension from property_settings where property_id="+data.property_id+" and is_broker=false and is_agent=false order by id desc limit 1) as personal_remain_time_to_add_extension, ";
                                                                                            timeExtQry += "(select log_time_extension from property_settings where property_id="+data.property_id+" and is_broker=false and is_agent=false order by id desc limit 1) as personal_log_time_extension, ";
                                                                                            timeExtQry += "(select id from property_settings where domain_id="+data.domain_id+" and user_id="+listing_agent_id+" and is_agent=true order by id desc limit 1) as agent__id, ";
                                                                                            timeExtQry += "(select is_log_time_extension from property_settings where domain_id="+data.domain_id+" and user_id="+listing_agent_id+" and is_agent=true order by id desc limit 1) as agent_is_log_time_extension, ";
                                                                                            timeExtQry += "(select remain_time_to_add_extension from property_settings where domain_id="+data.domain_id+" and user_id="+listing_agent_id+" and is_agent=true order by id desc limit 1) as agent_remain_time_to_add_extension, ";
                                                                                            timeExtQry += "(select log_time_extension from property_settings where domain_id="+data.domain_id+" and user_id="+listing_agent_id+" and is_agent=true order by id desc limit 1) as agent_log_time_extension, ";
                                                                                            timeExtQry += "(select id from property_settings where domain_id="+data.domain_id+" and is_broker=true order by id desc limit 1) as broker_id, ";
                                                                                            timeExtQry += "(select is_log_time_extension from property_settings where domain_id="+data.domain_id+" and is_broker=true order by id desc limit 1) as broker_is_log_time_extension, ";
                                                                                            timeExtQry += "(select remain_time_to_add_extension from property_settings where domain_id="+data.domain_id+" and is_broker=true order by id desc limit 1) as broker_remain_time_to_add_extension, ";
                                                                                            timeExtQry += "(select log_time_extension from property_settings where domain_id="+data.domain_id+" and is_broker=true order by id desc limit 1) as broker_log_time_extension ";
                                                                                            timeExtQry += "from property_listing l where l.id="+data.property_id;

                                                                                            pool.query(timeExtQry, function (err, timeExtRes) {
                                                                                               if(err){
                                                                                                    logger.log("error", 'ERROR QUERY : ' + timeExtQry);
                                                                                                    socket.emit("addNewBid", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                                                                    return 0;
                                                                                               }
                                                                                               else{
                                                                                                    if(timeExtRes.rowCount > 0){
                                                                                                        //Add extension time
                                                                                                        var extData = timeExtRes.rows[0];
                                                                                                        var remain_time_to_add_extension = 0;
                                                                                                        var log_time_extension = 0;
                                                                                                        if (extData.personal_id != null && extData.personal_is_log_time_extension){
                                                                                                            remain_time_to_add_extension = extData.personal_remain_time_to_add_extension
                                                                                                            log_time_extension = extData.personal_log_time_extension
                                                                                                        }else if(extData.personal_id == null && extData.agent__id != null && extData.agent_is_log_time_extension){
                                                                                                            remain_time_to_add_extension = extData.agent_remain_time_to_add_extension
                                                                                                            log_time_extension = extData.agent_log_time_extension
                                                                                                        }else if(extData.personal_id == null && extData.agent__id == null && extData.broker_id != null && extData.broker_is_log_time_extension){
                                                                                                            remain_time_to_add_extension = extData.broker_remain_time_to_add_extension
                                                                                                            log_time_extension = extData.broker_log_time_extension
                                                                                                        }
                                                                                                        //if(timeLeft <= extensionPeriod && extensionAmt > 0 && parseInt(auction_id) > 0 ){
                                                                                                        if(timeLeft <= remain_time_to_add_extension && log_time_extension > 0 && parseInt(auction_id) > 0 ){
                                                                                                            let extensionQry = "UPDATE property_auction SET end_date = (end_date::timestamp + ("+log_time_extension+" ||' minutes')::interval) WHERE id = "+auction_id;

                                                                                                            executeQuery(extensionQry);
                                                                                                        }
                                                                                                    }
                                                                                               }
                                                                                            });

                                                                                            bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_canceled, property_id, auction_id, user_id, bid_type, domain_id, ip_address, auction_type, insider_auction_step)";
                                                                                            bidAddQl += "VALUES(now(),"+bidAmount+",'1','1', '0',"+data.property_id+","+auction_id+","+data.user_id+", '2', "+data.domain_id+", '"+ip_address+"', 2, 3)";
                                                                                            pool.query(bidAddQl,function (err, addBidRes) {
                                                                                                if(err){
                                                                                                    // pool.end(() => {});
                                                                                                    logger.log("error", 'ERROR QUERY : ' + bidAddQl);
                                                                                                    socket.emit("englishAuction", {"msg": "An error occurred that prevented your bid from being placed. Please try again.","status":400, "error": 1});
                                                                                                    return 0;
                                                                                                }else{
                                                                                                    socket.emit("englishAuction", {"msg": "Bid Successfully.", "bid_amount": data.bid_amount, "status":201, "error": 0});
                                                                                                    //---------------------Email send here----------------
                                                                                                    createSendMessage(data.domain_id, data.property_id, data.user_id, parseInt(data.bid_amount));
                                                                                                    return 0;
                                                                                                }
                                                                                            });
                                                                                        }

                                                                                    }else{
                                                                                        socket.emit("englishAuction", {"msg": "Bid not exist.","status":400, "error": 1});
                                                                                        return 0;
                                                                                    }
                                                                                }
                                                                            });
                                                                        }
                                                                    }
                                                                });
                                                            }else{
                                                                socket.emit("englishAuction", {"msg": "Auction not exist.","status":400, "error": 1});
                                                                return 0;
                                                            }

                                                        }
                                                    });
                                                }else{
                                                    socket.emit("englishAuction", {"msg": "Registration not approved for bidding.","status":400, "error": 1});
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
            }else{
				socket.emit("englishAuction", {'msg': 'Invalid request.','status':400, 'error': 1});
				return 0;
			}
	    }catch(err){
            logger.log("error", 'ERROR : ' + err.message);
            socket.emit("englishAuction",{"message":err.message,"status":500});
            return 0;
        }
	},

	//-------------English auction ended---------
    englishAuctionEnded: function(socket, data){
		try{
            if (data.domain_id == undefined || data.domain_id == "" || data.domain_id <= 0){
                socket.emit("englishAuctionEnded",{"msg":"Missing params", "status":400, "error": 1});
                return 0;
            }
            let auctionQry = "select auction_id AS id, property_id, time_left, auction_type, seal_user_id, seal_winning_amount from english_remain_property_view where time_left < 1 and domain_id="+ parseInt(data.domain_id)+ " and seal_winning_amount > 0;";
			pool.query(auctionQry, function (err, result) {
				if(err){
					logger.log("error", 'ERROR AUCTION STATUS QUERY : ' + auctionQry);
					 socket.emit("englishAuctionEnded", {"msg": err, "status": 400, "error": 1});
					 return 0;
				}
				else{
					if(result.rowCount > 0){
						let rows = result.rows;
						rows.forEach(function(val){
						    seal_user_id = parseInt(val['seal_user_id']);
						    seal_amount = parseInt(val['seal_winning_amount']);
						    let bidCheckQry = "select id, user_id, bid_amount from bid where property_id="+val['property_id']+" and domain_id="+ parseInt(data.domain_id)+ " and auction_type=2 and insider_auction_step=3 and is_canceled=false order by id desc;";
                            pool.query(bidCheckQry, function (err, bidCheckRes) {
                                if(err){
                                    logger.log("error", 'ERROR AUCTION STATUS QUERY : ' + bidCheckQry);
                                     socket.emit("englishAuctionEnded", {"msg": err, "status": 400, "error": 1});
                                     return 0;
                                }else{
                                    if(bidCheckRes.rowCount > 0){
                                        var bid_check_data = bidCheckRes.rows[0];
                                        bid_id = parseInt(bid_check_data.id);
                                        english_user_id = parseInt(bid_check_data.user_id);
						                english_amount = parseInt(bid_check_data.bid_amount);
						                let bidEnglishStepQry = "select id from insider_auction_step_winner where property_id="+val['property_id']+" and domain_id="+ parseInt(data.domain_id)+ " and insider_auction_step=3 and status_id=1;";
                                        pool.query(bidEnglishStepQry, function (err, bidEnglishStepRes) {
                                            if(err){
                                                logger.log("error", 'ERROR AUCTION STATUS QUERY : ' + bidEnglishStepQry);
                                                 socket.emit("englishAuctionEnded", {"msg": err, "status": 400, "error": 1});
                                                 return 0;
                                            }else{
                                                if(bidEnglishStepRes.rowCount < 1){
                                                    let statusQl = "INSERT into insider_auction_step_winner (bid_id, amount,domain_id,user_id, property_id, auction_id, insider_auction_step, status_id, added_on, updated_on)";
                                                    statusQl += "VALUES("+bid_id+", "+english_amount+", "+data.domain_id+", "+english_user_id+", "+parseInt(val['property_id'])+","+parseInt(val['id'])+", 3, 1, now(), now())";
                                                    executeQuery(statusQl);

                                                    statusQl = "Update property_listing SET status_id = 9, closing_status_id =16, updated_on =now(), sold_price="+english_amount+", winner_id = "+english_user_id+", date_sold =now() WHERE id="+parseInt(val['property_id'])+" and domain_id="+parseInt(data.domain_id);
                                                    executeQuery(statusQl);

                                                    auctionQl = "Update property_auction SET status_id = 2 Where id = "+parseInt(val['id'])+" and domain_id="+parseInt(data.domain_id);
                                                    executeQuery(auctionQl);

                                                    //---------------------Email send here----------------
                                                    createSendMessage(data.domain_id, parseInt(val['property_id']), english_user_id, parseInt(english_amount), "english_ended");
                                                    return 0;
                                                }
                                            }
                                        });
                                    }else{
                                        statusQl = "Update property_listing SET status_id = 9, closing_status_id =16, updated_on =now(), sold_price="+seal_amount+", winner_id = "+seal_user_id+", date_sold =now() WHERE id="+parseInt(val['property_id'])+" and domain_id="+parseInt(data.domain_id);
                                        executeQuery(statusQl);

                                        auctionQl = "Update property_auction SET status_id = 2 Where id = "+parseInt(val['id'])+" and domain_id="+parseInt(data.domain_id);
                                        executeQuery(auctionQl);
                                        //---------------------Email send here----------------
                                        createSendMessage(data.domain_id, parseInt(val['property_id']), seal_user_id, parseInt(seal_amount), "english_ended");
                                        return 0;
                                    }
                                }
                            });
						});
						socket.emit("englishAuctionEnded", {"msg": "Successfully updated", "status": 201, "error": 0});
						return 0;
					}else{
					    socket.emit("englishAuctionEnded", {"msg": "Record not found", "status": 400, "error": 1});
					    return 0;
					}

				}

			});
		}
		catch(err){
			logger.log("error", 'AUCTION STATUS ERROR : ' + err.message);
			return 0;
		}
	},
	//-------------Insider user dashboard---------
	insiderUserDashboard: function insiderUserDashboard(socket, data) {
		//first we check if user id , property id, auction id and domain id exist in parameter
		try{
            if(data.property_id > 0 && data.user_id > 0 && data.auction_id > 0 && data.domain_id > 0){
                //let chkQry = "select id from property_listing where id = "+data.property_id+" and status_id = 1 and domain_id="+parseInt(data.domain_id);
                let chkQry = "select id from property_listing where id = "+data.property_id+" and domain_id="+parseInt(data.domain_id)+" and sale_by_type_id=2";
                pool.query(chkQry, function (err, checkRes) {
                    if(err){
                        //if error in sql let log query
                        logger.log("error", 'ERROR QUERY : ' + chkQry);
                        socket.emit("insiderUserDashboard", {"msg": err, "status": 400, "error": 1});
                        return 0;
                    }
                    else{
                        if(checkRes.rowCount > 0){

                            //This will check bid records and keep bid value in sync it also take time remain and threshold value
                             let sqlQuery = "select b.property_id as  property_id, b.bid_amount as  high_bid_amt,b.user_id  as user_id, total as bid_count, br.is_reviewed as is_reviewed, br.is_approved as is_approved, a.bid_increments as bid_increments, a.start_date as start_date, a.end_date as end_date, a.dutch_end_time as dutch_end_time, a.sealed_start_time as sealed_start_time, a.sealed_end_time as sealed_end_time, a.english_start_time as english_start_time, a.insider_decreased_price as decreased_amount, a.start_price as start_price, a.status_id as auction_status, pl.winner_id as winner_id, pl.status_id as property_status, ls.status_name as property_status_name, pl.sale_by_type_id as sale_by_type_id, pl.closing_status_id as closing_status, plc.status_name as closing_status_name, ";
                             //sqlQuery += "(select closing_status_id from property_listing where id="+data.property_id+") as closing_status, ";
                             //sqlQuery += "(select lookup_status.status_name from property_listing join lookup_status on property_listing.closing_status_id = lookup_status.id where property_listing.id="+data.property_id+") as  closing_status_name, ";
                             sqlQuery += "(select max(bid_amount) from bid where user_id = "+data.user_id+" and auction_id = "+data.auction_id+") as my_max_bid_val, ((extract (epoch from (a.end_date::timestamp - now()::timestamp))::integer)) as time_left_hr, ((extract (epoch from (a.start_date::timestamp - now()::timestamp))::integer)) as start_time_left_hr, a.ending_soon_threshold,end_date,";
                             sqlQuery += "(select amount from insider_auction_step_winner where insider_auction_step=1 and status_id=1 and property_id = "+data.property_id+" order by id desc limit 1) as round_one_winning_amount,";
                             sqlQuery += "(select amount from insider_auction_step_winner where insider_auction_step=2 and status_id=1 and property_id = "+data.property_id+" order by id desc limit 1) as round_two_winning_amount,";
                             sqlQuery += "(select amount from insider_auction_step_winner where insider_auction_step=3 and status_id=1 and property_id = "+data.property_id+" order by id desc limit 1) as round_three_winning_amount,";
                             sqlQuery += "(select user_id from insider_auction_step_winner where insider_auction_step=1 and status_id=1 and property_id = "+data.property_id+" order by id desc limit 1) as dutch_winning_user_id,";
                             sqlQuery += "(select user_id from insider_auction_step_winner where insider_auction_step=2 and status_id=1 and property_id = "+data.property_id+" order by id desc limit 1) as sealed_winning_user_id,";
                             sqlQuery += "(select user_id from insider_auction_step_winner where insider_auction_step=3 and status_id=1 and property_id = "+data.property_id+" order by id desc limit 1) as english_winning_user_id,";
                             sqlQuery += "(select bid_amount from bid where is_canceled=False and insider_auction_step=2 and user_id="+data.user_id+" and property_id = "+data.property_id+" order by id desc limit 1) as user_sealed_bid_amount ";
                             sqlQuery += " from bid b join property_auction a on a.id = b.auction_id Join bid_count_view bv on b.property_id = bv.property_id";
                             sqlQuery += " join bid_registration br on  b.property_id = br.property_id ";
                             sqlQuery += " join property_listing pl on  b.property_id = pl.id ";
                             sqlQuery += " join lookup_status ls on pl.status_id = ls.id ";
                             sqlQuery += " left join lookup_status plc on  plc.id = pl.closing_status_id ";
                             //sqlQuery += " where a.end_date >= now() and a.status_id= 1 and br.status_id=1 and br.user_id="+data.user_id;
                             sqlQuery += " where a.end_date >= now() and br.status_id=1 and br.user_id="+data.user_id+" and pl.sale_by_type_id=2";
                             sqlQuery +=" and bid_type IN('2','3')  and b.auction_id = "+data.auction_id+" and b.property_id = "+data.property_id+" and b.domain_id="+data.domain_id;
                             sqlQuery += " order by  b.bid_amount DESC , bid_date desc limit 1";

                            pool.query(sqlQuery, function (err, result) {
                                if(err){
                                    //check logger working
                                    logger.log("error", 'ERROR QUERY : ' + sqlQuery);
                                    socket.emit("insiderUserDashboard", {"msg": err, "error": 1, "status": 400});
                                    return 0;
                                }
                                else{

                                    if(result.rowCount > 0){
                                       socket.emit("insiderUserDashboard", {"data": result.rows[0], "msg": "Fetch data", "status": 201, "error": 0});
                                       socket.in(data.property_id+'_'+data.user_id).emit("insiderUserDashboard", {"data": result.rows[0], "msg": "Fetch data", "status": 201, "error": 0});
                                       //socket.broadcast.emit("checkMyBid", {"data": result.rows[0], "msg": "Fetch data", "status": 201, "error": 0});
                                    }
                                    else{
                                        let flshQuery = "Select pl.id as property_id, a.bid_increments as bid_increments, r.is_approved as is_approved, r.is_reviewed as is_reviewed, a.start_date as start_date, a.end_date as end_date, a.dutch_end_time as dutch_end_time, a.sealed_start_time as sealed_start_time, a.sealed_end_time as sealed_end_time, a.english_start_time as english_start_time, a.insider_decreased_price as decreased_amount, a.start_price as start_price, total as bid_count, a.status_id as auction_status, pl.winner_id as winner_id, pl.status_id as property_status, ls.status_name as property_status_name, pl.closing_status_id as closing_status, cls.status_name as closing_status_name, pl.sale_by_type_id as sale_by_type_id, ";
                                        flshQuery += " (select max(bid_amount) from bid where user_id = "+data.user_id+" and auction_id = "+data.auction_id+") as my_max_bid_val, ";
                                        flshQuery += " ((extract (epoch from (a.end_date::timestamp - now()::timestamp))::integer)) as time_left_hr, ((extract (epoch from (a.start_date::timestamp - now()::timestamp))::integer)) as start_time_left_hr, a.ending_soon_threshold,";
                                        flshQuery += "(select amount from insider_auction_step_winner where insider_auction_step=1 and status_id=1 and property_id = "+data.property_id+" order by id desc limit 1) as round_one_winning_amount,";
                                        flshQuery += "(select amount from insider_auction_step_winner where insider_auction_step=2 and status_id=1 and property_id = "+data.property_id+" order by id desc limit 1) as round_two_winning_amount,";
                                        flshQuery += "(select amount from insider_auction_step_winner where insider_auction_step=3 and status_id=1 and property_id = "+data.property_id+" order by id desc limit 1) as round_three_winning_amount,";
                                        flshQuery += "(select user_id from insider_auction_step_winner where insider_auction_step=1 and status_id=1 and property_id = "+data.property_id+" order by id desc limit 1) as dutch_winning_user_id,";
                                        flshQuery += "(select user_id from insider_auction_step_winner where insider_auction_step=2 and status_id=1 and property_id = "+data.property_id+" order by id desc limit 1) as sealed_winning_user_id,";
                                        flshQuery += "(select user_id from insider_auction_step_winner where insider_auction_step=3 and status_id=1 and property_id = "+data.property_id+" order by id desc limit 1) as english_winning_user_id,";
                                        flshQuery += "(select bid_amount from bid where is_canceled=False and insider_auction_step=2 and user_id="+data.user_id+" and property_id = "+data.property_id+" order by id desc limit 1) as user_sealed_bid_amount ";
                                        flshQuery += "from property_listing pl join property_auction a on pl.id = a.property_id ";
                                        flshQuery += "left join bid_registration r on pl.id = r.property_id ";
                                        flshQuery += "left join lookup_status ls on pl.status_id = ls.id ";
                                        flshQuery += " left join lookup_status cls on pl.closing_status_id = cls.id ";
                                        flshQuery += "left join bid_count_view bv on pl.id = bv.property_id "
                                        flshQuery += "where pl.id="+data.property_id+" and a.id="+data.auction_id+" and r.status_id=1 and r.user_id="+data.user_id+" and pl.sale_by_type_id=2 and pl.domain_id="+data.domain_id;
                                        flshQuery += " order by pl.id desc limit 1;";
                                        pool.query(flshQuery, function (err, checkRes) {
                                            if(err){
                                                //if error in sql let log query
                                                logger.log("error", 'ERROR QUERY : ' + flshQuery);
                                                socket.emit("insiderUserDashboard", {"msg": err, "status": 400, "error": 1});
                                                return 0;

                                            }
                                            else{
                                                if(checkRes.rowCount > 0){
													 socket.emit("insiderUserDashboard", {"data": checkRes.rows[0], "status": 201, "error": 0});
													 socket.in(data.property_id+'_'+data.user_id).emit("insiderUserDashboard", {"data": checkRes.rows[0], "status": 201, "error": 0});
                                                     //socket.broadcast.emit("checkMyBid", {"data": checkRes.rows[0], "status": 201, "error": 0});
                                                     return 0;
                                                }
                                                else{
                                                    socket.emit("insiderUserDashboard", {'msg': 'Invalid request.','status':400, "error": 1});
                                                }
                                            }
                                        });
                                    }
                                }
                            });

                        }
                        else{
                            socket.emit("insiderUserDashboard", {'msg': 'Invalid request.','status':400, "error": 1});
                        }
                    }
                });	//Auth query end
            }
            else{
                socket.emit("insiderUserDashboard", {'msg': 'Invalid request.','status':403, "error": 1});
            }
		}catch(err){
          logger.log("msg", 'ERROR : ' + err.message);
          return 0;
        }
	},

}



/**
 * This js funciton display number with thousand separator  
 */
function convertToDisplayFormat(num)
{
    var removeTrailingZero = parseFloat(num).toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    removeTrailingZero = removeTrailingZero.replace('.00','');
    return removeTrailingZero;
 }
 
 /**
  * Post Proxy bidding, Fucntion call recurssivly and place desired bid
  * automatically if there are high bid exists in system. This function 
  * take three parameter. 
  * 1. Bid increment value
  * 2. Current successfull placed bid amount
  * 3. Listing Id 
  * 4. Auction Id and
  * 5. Profile Id
  * Proxy bid shall always equals to last bid + bid increment 
  * if single max bid amount exists and last bid placed by user
  * other than max bid amount owner 
  * 
  * Not in use
  */
function placeProxyBid(incrementVal, bidAmt, listId, auctionId, profileId)
{
	
	if(incrementVal > 0 && bidAmt > 0 && listId > 0 && auctionId > 0){
		try {
			/** First we list all max amount > bid amount and profile id
			 *  where listing id and auction id is same; 
			 * this query get all user and max amount placed more than
			 * bid amount. 
			 * sort order first by amount then timestamp this measn 
			 * two user with same amount set first user get priority over
			 * next user.
			 */ 
			
			let maxAmtQl = "SELECT max_amount, profile_id";
			maxAmtQl += " FROM bid_max_amount";
			maxAmtQl += " WHERE auction_id ="+ auctionId ;
			maxAmtQl += " AND max_amount >="+ bidAmt ;
			maxAmtQl += " ORDER BY max_amount DESC, date_added ASC";
			
			pool.query(maxAmtQl, function (err, maxAmtRes) {
			
			if(err){
				 //log query and exit
				 // pool.end();
				 logger.log("error", 'ERROR QUERY : ' + maxAmtQl);
				 return 0;
			 }
			 else if(maxAmtRes.rowCount === 0){
				 //No higher max amount found let do nothing
				 return 0;
			 }
			 else{
				 const res = maxAmtRes.rows;
				 let maxBidVal = [];
				 let maxRecord = [];
				//Lets check record and record proxy bidding type id 3
				
				res.forEach(rec =>{
					//console.log(rec.profile_id + "===>>"+rec.max_amount+"==>"+bidAmt);
					//Lets ensure each profile have unique max amount value
					maxRecord =[rec.profile_id,rec.max_amount];
					maxBidVal.push(maxRecord);
					
				});
				console.log(maxBidVal);
				
				// if there are only high bid amount let post a auto proxy bid for that 
				
				if(parseInt(maxBidVal.length) === 1 && parseInt(maxBidVal[0][0]) != parseInt(profileId) && parseInt(maxBidVal[0][1]) > parseInt(bidAmt) ){
					/** For single highbid user lets outbid previous user and set high bid to true for other user
					* If highest bid amount set by last user itself lets do nothing  
					*/
					let minBidAmt = parseInt(bidAmt) + parseInt(incrementVal);
					
					let bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_canceled, listing_id_id, auction_id_id, profile_id_id, bid_type)";
					bidAddQl += "VALUES(now(),"+minBidAmt+",'1','1', '0',"+listId+","+auctionId+","+maxBidVal[0][0]+", '3')";
					
					executeQuery(bidAddQl);
				}
				else{
					/** Top list means user is hightest bid amount user 
					 * below top other will loose proxy
					 * if more than one and amount is same only first profile will bid as proxy 
					 * other become audit only bid
					 * exclude same profile in list
					 */ 
					 let i = 0;
					 let maxVal = maxAmtRes.rowCount;
					 let bidStatus = "3";
					 let proxyWinAmount = 0;
					 let bidAddQl = ""
					 
					 for(i = 1; i < maxVal ; i++){
						 //check if next amount is eqal or less than previous one
						 console.log("Counter val===>" + i);
						 bidStatus = (parseInt(maxBidVal[i][1]) == parseInt(maxBidVal[i - 1][1]))?'1':'3'; // if previous amount is equal then this bid is audit only 
						 
						 bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_canceled, listing_id_id, auction_id_id, profile_id_id, bid_type)";
						 bidAddQl += "VALUES(now(),"+maxBidVal[i][1]+",'0','1', '0',"+listId+","+auctionId+","+maxBidVal[i][0]+", "+bidStatus+")";
						 console.log(bidAddQl);
						 executeQuery(bidAddQl);
						 
					 }
					 /**
					  * Check if second last bid amount + bid increment is greater than 
					  * top bid amount set else set top as high bid 
					  * new bid amount either last bid + bid increment or last bid amount as two user have 
					  * set same max bid amount in this case the user came first 
					  * consider winner 
					  */
					 proxyWinAmount = (parseInt(maxBidVal[0][1]) > parseInt(maxBidVal[1][1]))? parseInt(maxBidVal[1][1]) + parseInt(incrementVal):maxBidVal[1][1];
					 
					 bidAddQl = "INSERT into bid (bid_date,bid_amount,highest_bid,user_qualify,is_canceled, listing_id_id, auction_id_id, profile_id_id, bid_type)";
					 bidAddQl += "VALUES(now(),"+ proxyWinAmount +",'1','1', '0',"+listId+","+auctionId+","+maxBidVal[0][0]+", '2')";
					 console.log(bidAddQl);
					 executeQuery(bidAddQl);
					 return 0;
					
				}
				
			 }
				
			});
		}
		catch(err){
			logger.log("error", 'ERROR : ' + err.message);
			socket.emit("bidHistory",{"message":err.message,"status":500});
			return 0;
		}
	} 
  
}

function executeQuery(sql){
	if(sql){
		
		try{
			pool.query(sql, function (err, result) {
			if(err){
			 //log query and exit
			 // pool.end(() => {});
			 logger.log("error", 'ERROR BID QUERY : ' + sql);
			 //socket.emit("checkBid", {"ERROR": err});
			 return 0;
			}
			else{
				
				return "ok";
			}
			});
		}
		catch(err){
			logger.log("error", 'BID ERROR : ' + err.message);
			return 0;
		}	
	}
}
/**
 * This function provide a pause in milli second 
 */
function sleep(milliseconds) {
  const date = Date.now();
  let currentDate = null;
  do {
    currentDate = Date.now();
  } while (currentDate - date < milliseconds);
}

/**
 * this function used to call mesage api from python server
 * 
 */ 
 
function messageAPICall(postData, endPoint){
	//const https = require('https');
    const https = require(global.config.API_PROTOCOL);
	const data = postData;
	const options = {
		  hostname: global.config.API_URL,
          port: global.config.API_PORT,
		//   port: 443,
//          host : '127.0.0.1',
//          port : 8000,
		  // path: '/api-bid/send-bid-email/',
		  path: endPoint,
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
		
		reqPost.on('data', (d) => {
			process.stdout.write(d);
		})
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


/**
 * this function used to call api
 *
 */

function listingSettingAPICall(){
    postData = JSON.stringify(
				{
                    "domain_id": 68,
                    "property_id": 74
                  }
		);
	// const https = require('https');
    const https = require(global.config.API_PROTOCOL);
	const data = postData;
	console.log(data.length);
	const options = {
		  hostname: global.config.API_URL,
          port: global.config.API_PORT,
		//   port: 443,
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
    console.log('hiiiiiii');
	let reqPost = https.request(options, function(res){

		if(res.statusCode !== 200){
			// to do
			logger.log("error", 'Email Error : Code '+ res.statusMessage +"; Message :" + res.statusMessage);
		}

		reqPost.on('data', (d) => {
			process.stdout.write(d);
		})
	});


	/** Log error*/
	reqPost.on('error', function(e) {
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
function createSendMessage(domain_id, property_id, user_id, bid_amount=0, mailType=""){
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
	if(mailType == "dutch"){
	    endPoint = "/api-bid/send-dutch-email/";
	}else if(mailType == "sealed"){
	    endPoint = "/api-bid/send-sealed-email/";
	}else if(mailType == "english_ended") {
	    endPoint = "/api-bid/send-english-end-email/";
	}else if(mailType == "sealed_ended") {
	    endPoint = "/api-bid/send-sealed-end-email/";
	}else{
	    endPoint = "/api-bid/send-english-email/";
	}
	msg = messageAPICall(returnBody, endPoint);
	return ;
}

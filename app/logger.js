/**
 * This module is responsible to log verious logs within node module
 * Date: July 26, 2021
 * Author: gautamk@clavax.us
 */ 


const { createLogger, format, transports } = require('winston');
const fs = require('fs');
const bodyParser = require('body-parser')
const path = require('path');
const env = process.env.NODE_ENV || 'development';
const logDir = 'log';
const curDt= new Date();
const month = curDt.getMonth() + 1;
const dtVal=  curDt.getDate()+"-"+month+"-"+curDt.getFullYear();
const filename = path.join(logDir, dtVal+'_adres.log');

/** Log setup start */
const logger = createLogger({
  // change level if in dev environment versus production
  level: env === 'development' ? 'verbose' : 'info',
  format: format.combine(
    format.timestamp({
      format: 'YYYY-MM-DD HH:mm:ss'
    }),
    format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
  ),
  transports: [
    new transports.Console({
      level: 'info',
      format: format.combine(
        format.colorize(),
        format.printf(
          info => `${info.timestamp} ${info.level}: ${info.message}`
        )
      )
    }),
    new transports.File({ filename })
  ]
});

/** Create the log directory if not exists */
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}

module.exports = logger

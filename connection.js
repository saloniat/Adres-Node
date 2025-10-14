const Pool = require('pg').Pool

/**DB configration start*/
const pool = new Pool({
  user: global.config.DB_USER,
  host: global.config.DB_HOST,
  database: global.config.DB_NAME,
  password: global.config.DB_PASS,
  port: 5432,
//   ssl: false,  // Explicitly disable SSL
  ssl: {
      rejectUnauthorized: false, // Set true with proper certificates in production
      // ca: fs.readFileSync('./ca-certificate.crt'),
      // key: fs.readFileSync('./client-key.pem'),
      // cert: fs.readFileSync('./client-cert.pem'),
  },
})

pool.connect((err, result) => {
    if (err) {
        //throw new Error(err)
        console.log("DB not connected")
        console.log(err)
        return
    }else{
        console.log("Db connected")
    }
});

module.exports = pool;

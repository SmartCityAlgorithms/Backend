const sql = require("./db.js");

// users
const User = function(user) {
    this.username = user.username;
    this.password = user.password;
    this.region = user.region;
    this.branch = user.branch;
};

User.findUser = (username, result) => {
    console.log(username)
    sql.query(`SELECT * FROM user WHERE username = ${username}`, (err, res) => {
        if (err) {
            console.log("error: ", err);
            result(err, null);
            return;
        }

        if (res.length) {
            console.log("found customer: ", res[0]);
            result(null, res[0]);
            return;
        }

        // not found Customer with the id
        result({ kind: "not_found" }, null);
    });
};

// Получение данных о предприятии по айди
User.findUserId = (user_id, result) => {
    console.log(user_id)
    sql.query(`SELECT * FROM user WHERE id = ${user_id}`, (err, res) => {
        if (err) {
            console.log("error: ", err);
            result(err, null);
            return;
        }

        if (res.length) {
            console.log("found customer: ", res[0]);
            result(null, res[0]);
            return;
        }

        // not found Customer with the id
        result({ kind: "not_found" }, null);
    });
};

module.exports = User;

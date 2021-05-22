const sql = require("./db.js");

// measure
const Measure = function(measure) {
    this.name = measure.name;
    this.title = measure.title;
    this.number_clicks = measure.number_clicks;
};

Measure.findMeasure = (id, result) => {
    sql.query(`SELECT * FROM measure WHERE id = ${id}`, (err, res) => {
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

Measure.updateMeasure = (id, measure, result) => {
    console.log(id)
    sql.query(
        "UPDATE measure SET name = ?, title = ?, number_clicks = ? WHERE id = ?",
        [measure.name, measure.title, measure.number_clicks, id],
        (err, res) => {
            if (err) {
                console.log("error: ", err);
                result(null, err);
                return;
            }

            if (res.affectedRows == 0) {
                // not found Customer with the id
                result({ kind: "not_found" }, null);
                return;
            }

            console.log("updated customer: ", { id: id });
            result(null, { id: id });
        }
    );
};

Measure.findAllMeasure = (result) => {
    sql.query('SELECT * FROM measure', (err, res) => {
        if (err) {
            console.log("error: ", err);
            result(err, null);
            return;
        }

        if (res.length) {
            console.log("found customer: ", res);
            result(null, res);
            return;
        }

        // not found Customer with the id
        result({ kind: "not_found" }, null);
    });
};

module.exports = Measure;

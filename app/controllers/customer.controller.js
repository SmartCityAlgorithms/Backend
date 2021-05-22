const Customer = require("../models/customer.model.js");
const User = require("../models/user.model.js");
const Measure = require("../models/measure.model.js");

// Create and Save a new Customer
exports.create = (req, res) => {
  // Validate request
  if (!req.body) {
    res.status(400).send({
      message: "Content can not be empty!"
    });
  }

  // Create a Customer
  const customer = new Customer({
    email: req.body.email,
    name: req.body.name,
    active: req.body.active
  });

  // Save Customer in the database
  Customer.create(customer, (err, data) => {
    if (err)
      res.status(500).send({
        message:
          err.message || "Some error occurred while creating the Customer."
      });
    else res.send(data);
  });
};

// Retrieve all Customers from the database.
exports.findAll = (req, res) => {
  Customer.getAll((err, data) => {
    if (err)
      res.status(500).send({
        message:
          err.message || "Some error occurred while retrieving customers."
      });
    else res.send(data);
  });
};

// Find a single Customer with a customerId
exports.findOne = (req, res) => {
  Customer.findById(req.params.customerId, (err, data) => {
    if (err) {
      if (err.kind === "not_found") {
        res.status(404).send({
          message: `Not found Customer with id ${req.params.customerId}.`
        });
      } else {
        res.status(500).send({
          message: "Error retrieving Customer with id " + req.params.customerId
        });
      }
    } else res.send(data);
  });
};

// Update a Customer identified by the customerId in the request
exports.update = (req, res) => {
  // Validate Request
  if (!req.body) {
    res.status(400).send({
      message: "Content can not be empty!"
    });
  }

  console.log(req.body);

  Customer.updateById(
    req.params.customerId,
    new Customer(req.body),
    (err, data) => {
      if (err) {
        if (err.kind === "not_found") {
          res.status(404).send({
            message: `Not found Customer with id ${req.params.customerId}.`
          });
        } else {
          res.status(500).send({
            message: "Error updating Customer with id " + req.params.customerId
          });
        }
      } else res.send(data);
    }
  );
};

// Delete a Customer with the specified customerId in the request
exports.delete = (req, res) => {
  Customer.remove(req.params.customerId, (err, data) => {
    if (err) {
      if (err.kind === "not_found") {
        res.status(404).send({
          message: `Not found Customer with id ${req.params.customerId}.`
        });
      } else {
        res.status(500).send({
          message: "Could not delete Customer with id " + req.params.customerId
        });
      }
    } else res.send({ message: `Customer was deleted successfully!` });
  });
};

// Delete all Customers from the database.
exports.deleteAll = (req, res) => {
  Customer.removeAll((err, data) => {
    if (err)
      res.status(500).send({
        message:
          err.message || "Some error occurred while removing all customers."
      });
    else res.send({ message: `All Customers were deleted successfully!` });
  });
};

// Авторизация
exports.auth = (req, res) => {
  try {
    User.findUser(req.body.username, (err, data) => {
      if (err) {
        if (err.kind === "not_found") {
          res.status(404).send({
            message: `Not found Customer with id ${req.body.username}.`
          });
        } else {
          res.status(500).send({
            message: "Error retrieving Customer with id " + req.body.username
          });
        }
      } else res.send(data);
    });
  } catch (e) {
    res.status(404).send({
      message: 'db error!!!'
    });
  }
};

// Данные о мер поддержки
exports.outputMeasure = (req, res) => {
  try {
    Measure.findMeasure(req.body.id_measure, (err, data_set) => {
      if (err) {
        if (err.kind === "not_found") {
          res.status(404).send({
            message: `Not found Customer with id ${req.body.username}.`
          });
        } else {
          res.status(500).send({
            message: "Error retrieving Customer with id " + req.body.username
          });
        }
      } else {
        data_set.number_clicks = data_set.number_clicks + 1;
        Measure.updateMeasure(
            data_set.id,
            new Measure(data_set),
            (err, data) => {
              if (err) {
                if (err.kind === "not_found") {
                  res.status(404).send({
                    message: `Not found Customer with id ${req.body.username}.`
                  });
                } else {
                  res.status(500).send({
                    message: "Error updating Customer with id " + req.body.username
                  });
                }
              } else {
                res.send(data_set);
              }
            }
        );
      }
    });
  } catch (e) {
    res.status(404).send({
      message: 'db error!!!'
    });
  }
};


// Данные о всех мер поддержки
exports.outputMeasureAll = (req, res) => {
  try{
    Measure.findAllMeasure((err, data_set) => {
      if (err) {
        if (err.kind === "not_found") {
          res.status(404).send({
            message: `Not found Customer with id ${req.body.username}.`
          });
        } else {
          res.status(500).send({
            message: "Error retrieving Customer with id " + req.body.username
          });
        }
      } else {
        res.send(data_set);
      }
    });
  } catch (e) {
    res.status(404).send({
      message: 'db error!!!'
    });
  }

};

// Получение ранжированых мер поддержки
exports.outputMeasureRanked = (req, res) => {
  try {
    User.findUserId(req.body.id_user, (err, data) => {
      if (err) {
        if (err.kind === "not_found") {
          res.status(404).send({
            message: `Not found Customer with id ${req.body.username}.`
          });
        } else {
          res.status(500).send({
            message: "Error retrieving Customer with id " + req.body.username
          });
        }
      } else {

        Measure.findAllMeasure((err, data_set) => {
          if (err) {
            if (err.kind === "not_found") {
              res.status(404).send({
                message: `Not found Customer with id ${req.body.username}.`
              });
            } else {
              res.status(500).send({
                message: "Error retrieving Customer with id " + req.body.username
              });
            }
          } else {

            //Сортируем по нужным полям


            var request = require('request');
            var options = {
              'method': 'POST',
              'url': 'http://192.168.1.34:4000/api/get_range_money_grants_for_inn/',
              'headers': {
                'Content-Type': 'application/json'
              },
              body: JSON.stringify({"SECRET_API_KEY":"ABC123","inn":data.branch.toString(),"kbk_list":[data_set[0].title,data_set[1].title,data_set[2].title,data_set[3].title]})

            };
            request(options, function (error, response) {
              if (error) throw new Error(error);
              let ves_set = JSON.parse(response.body);
              console.log("ves_set", ves_set["result"])
              ves_set = ves_set.result;

              let prom = Math.floor(80/ves_set.length);

              for (let i = 0; i < data_set.length; i++) {
                data_set[i]["ves"] = ves_set[i]
              }

              data_set.sort((prev, next) => prev.ves - next.ves);
              let t = 1;
              for (let i = 0; i < data_set.length; i++) {
                data_set[i]["ves"] = 20 + prom * t;
                t++;
              }
              data_set.reverse()

              // Отправляем ответ
              res.send(data_set);
              console.log(response.body);
            });
          }
        });


      }
    });
  } catch (e) {
    res.status(404).send({
      message: 'db error!!!'
    });
  }
};


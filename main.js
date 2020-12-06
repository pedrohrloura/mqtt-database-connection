var mqtt = require('mqtt');
require('dotenv').config()
const { connect, Request } = require("tedious");
const TYPES = require('tedious').TYPES;
var moment = require('moment');

//DATABASE
var config = {
    server: process.env.DB_SERVER,
    authentication: {
        options: {
            userName: process.env.DB_USERNAME,
            password: process.env.DB_PASSWORD
        },
        type: "default"
    },
    options: {
        validateBulkLoadParameters: true,
        database: process.env.DB_DATABASE,
        encrypt: true,
        trustServerCertificate: true
    }
};
connection = new connect(config);

connection.on("connect", err => {
    if (err) {
        console.error(err.message);
    } else {
        console.log("Connected to database");
    }
});

function insertTemp(temp, codAquario = 0) {
    request = new Request("INSERT INTO TEMPERATURA (TEMP, COD_AQUARIO) VALUES (@TEMP, @COD_AQUARIO);", function (err) {
        if (err) {
            console.log(err);
        }
    });
    request.addParameter('TEMP', TYPES.Int, temp);
    request.addParameter('COD_AQUARIO', TYPES.Int, codAquario);
    console.log(`Values ​​entered successfully in database`)
    connection.execSql(request);
}
function updateLastFood(codAquario) {
    request = new Request("UPDATE AQUARIO SET ULTIMO_ALIMENTO = @ULTIMO_ALIMENTO WHERE COD_AQUARIO = @COD_AQUARIO;", function (err) {
        if (err) {
            console.log(err);
        }
    });
    let time = moment(new Date()).format("YYYY-MM-DD HH:mm:ss");
    request.addParameter('ULTIMO_ALIMENTO', TYPES.DateTime, time);
    request.addParameter('COD_AQUARIO', TYPES.Int, codAquario);
    console.log(`Values ​​entered successfully in database`)
    connection.execSql(request);
}

//MQTT
options = {
    clientId: "mqttjs01",
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    clean: true
};
var client = mqtt.connect(process.env.MQTT_HOST, options)

client.on("connect", function () {
    console.log("Connected to MQTT");
})
client.on('message', function (topic, message, packet) {
    console.log("topic is " + topic);
    let data = JSON.parse(message)
    if (topic == tempTopic) {
        let codAquario = data.codAquario;
        let temp = data.temp;
        console.log("Temperature: " + temp + "°C  Aquarium: " + codAquario);
        insertTemp(temp, codAquario)
    } else if (topic == foodTopic) {
        let codAquario = data.codAquario;
        console.log(`Feeding aquarium ${codAquario}`)
        updateLastFood(codAquario);
    }
});
client.on("error", function (error) {
    console.log("Can't connect" + error);
    process.exit(1)
});
var tempTopic = process.env.TOPIC_TEMP
var foodTopic = process.env.TOPIC_FOOD
client.subscribe(tempTopic, { qos: 1 });
client.subscribe(foodTopic, { qos: 1 });

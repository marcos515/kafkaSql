/*INSERT INTO table_name (column1, column2, column3, ...)
VALUES (value1, value2, value3, ...);*/

const mysql = require('mysql2');
const config = require('./files/config.json')
const kafka = require("./kafka/kafka")
const kafkaConfig = require('./files/kafka.json')

var options = {
    host: config.host,
    user: config.user,
    database: config.database,
    password: config.password,
    waitForConnections: config.waitForConnections,
    connectionLimit: config.connectionLimit,
    queueLimit: config.queueLimit
}



{
    (async () => {
        var consumer = kafka.consumer(kafkaConfig.host, kafkaConfig.topic)
        consumer.on("message", async (message) => {
            var msg = json(message.value)

           
                let data = split(config.binding, 0)
                let data2 = JSON.stringify(split(config.binding, 1))
                var query = `INSERT INTO ${config.tableName} (\`ts\`,${data2.replace("[", "").replace("]", "").split('"').join("`")}) VALUES (${parseValuesSql(data, msg)})`
                console.log(query)
                try {
                    await main(options, query)
                } catch (err) {
                    console.log(err)
                }
            
            //console.log(msg)
            //await main(options, query)
        })

    })()
}

function json(json) {
    try {
        return JSON.parse(json)
    } catch (err) {
        return json
    }
}
async function main(options, query) {
    return new Promise((resolve, reject) => {
        const connection = mysql.createConnection(options);
        connection.on("error", (err, msg) => { reject(err) })

        connection.connect();

        connection.query(query, function (error, results, fields) {
            if (error) {
                reject(error)
            };
            //console.log(`Retornou ${results.length} linhas.`);
            connection.end();
            resolve(results)
        });

    })
}

function split(json, ind) {
    let arrat = []
    for (let index = 0; index < json.length; index++) {
        arrat.push(json[index].split("-")[ind])

    }
    return arrat
}

function search(Skey, json) {
    var newJson = json
    var retorna = 'nn'
    if (typeof json == "object") {
        newJson = JSON.stringify(json)
    }
    JSON.parse(newJson, (key, value) => {
        if (key == Skey) {
            retorna = value

        }
        return value
    })

    return retorna
}

function parseValuesSql(keys, msg) {
    var values =  `"${new Date().getTime()}",`
    
    for (let index = 0; index < keys.length; index++) {
        let s = search(keys[index], msg)
        if (keys.length - 1 == index) {
            values += `"${(s == 'nn' ? 'null' : s)}"`
        } else {
            values += `"${(s == 'nn' ? 'null' : s)}",`
        }
    }

    return values
}
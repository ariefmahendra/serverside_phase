const mqtt = require("mqtt");
const mysql = require("mysql");

// Konfigurasi koneksi MySQL
const dbConfig = {
  host: "localhost",
  user: "root",
  password: "Mirocle123-",
  database: "phases",
};

// Membuat koneksi ke MySQL
const dbConnection = mysql.createConnection(dbConfig);

// Menghubungkan ke broker MQTT
const brokerUrl = "mqtt://broker.emqx.io";
const options = {
  port: 1883,
  clientId: "severside_phases",
};

const client = mqtt.connect(brokerUrl, options);

// Menghubungkan ke broker MQTT
client.on("connect", () => {
  console.log("Terhubung ke broker MQTT");

  // Subscribe ke topik 'data_sensor_phase_changer'
  client.subscribe("data_sensor_phase_changer", (err) => {
    if (err) {
      console.error("Gagal melakukan subscribe ke topik", err);
    } else {
      console.log("Berhasil melakukan subscribe ke topik");
    }
  });
});

// Menerima pesan MQTT dan menyimpannya ke database
client.on("message", (topic, message) => {
  if (topic === "data_sensor_phase_changer") {
    // Parsing pesan sebagai JSON
    const data = JSON.parse(message.toString());

    // Memasukkan pesan ke tabel data_sensor
    const insertQuery = `
    INSERT INTO data_sensor (user_id, arus_r, arus_s, arus_t, tegangan_r, tegangan_s, tegangan_t)
    SELECT users.id, ${data.sensor_arus_0}, ${data.sensor_arus_1}, ${data.sensor_arus_2}, ${data.sensor_tegangan_0}, ${data.sensor_tegangan_1}, ${data.sensor_tegangan_2}
    FROM users
    WHERE users.status = 1;
    `;

    dbConnection.query(insertQuery, (insertErr, insertResult) => {
      if (insertErr) {
        console.error("Gagal memasukkan data ke database", insertErr);
      } else {
        console.log("Berhasil memasukkan data ke database");
      }
    });
  }
});

// Menutup koneksi ke database saat program berakhir
process.on("SIGINT", () => {
  dbConnection.end();
  process.exit();
});

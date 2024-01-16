const mysql = require('mysql2/promise');
const amqp = require('amqplib');
const fetch = require('isomorphic-fetch');

const getMysqlPool = () => {
  try {
    let pool = mysql.createPool({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_DATABASE,
      waitForConnections: true,
      connectionLimit: process.env.DB_CONNECTION_LIMIT || 10,
      queueLimit: 0
    });

    console.log('Connection pool created successfully');
    return pool;
  } catch (error) {
    console.error(`Connection pool creation failed: ${error}`);
    return null;
  }
};




const getAllBranches = async () => {

const pool = getMysqlPool();

  try {
    const connection = await pool.getConnection();
    const [rows] = await connection.query('SELECT city, town , idbranch FROM branch');
    connection.release();

    return rows;
  } catch (error) {
    console.error(`Error getting branch information: ${error}`);
    throw error;
  }
};


async function getCoordinates(city, district) {
    const apiKey = 'c55a20d9e66a41e4b32327807812a923';
    const encodedCity = encodeURIComponent(city);
    const encodedDistrict = encodeURIComponent(district);
    
    if (!encodedCity || !encodedDistrict) {
        throw new Error('Invalid city or district');
    }
  
    const apiUrl = `https://api.opencagedata.com/geocode/v1/json?q=${encodedDistrict},${encodedCity}&key=${apiKey}`;
  
    console.log("apiUrl", apiUrl);


    try {
      const response = await fetch(apiUrl);
      if (!response.ok) {
        throw new Error(`OpenCage API request failed with status: ${response.status}`);
      }
  
      const data = await response.json();
      if (!data.results || data.results.length === 0 || !data.results[0].geometry) {
        throw new Error('Invalid or missing data from OpenCage API');
      }
  
      const latitude = data.results[0].geometry.lat;
      const longitude = data.results[0].geometry.lng;
      console.log("latitude :", latitude, "longitude : ", longitude);
      return { latitude, longitude };
    } catch (error) {
      console.error('Error retrieving coordinates:', error);
      throw error;
    }
  }
  



  function haversine(lat1, lon1, lat2, lon2) {
    const R = 6371; 
    const dLat = degToRad(lat2 - lat1);
    const dLon = degToRad(lon2 - lon1);
  
    const a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(degToRad(lat1)) * Math.cos(degToRad(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
  
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    const distance = R * c;
  
    return distance;
  }


  function degToRad(deg) {
    return deg * (Math.PI / 180);
  }


  const searchBlood = async (branchId, bloodType, units) => {
    const pool = getMysqlPool();
  
    if (!pool) {
      console.error('No MySQL connection pool available.');
      return null;
    }
  
    try {
      console.log(`Searching blood for ${units} units in branch ${branchId} with blood type ${bloodType}...`);
  
      // Donor query
      const donorQuery = 'SELECT iddonor, units_of_blood FROM donor WHERE branch_id = ? AND blood_type = ?';
      const formattedDonorQuery = pool.format(donorQuery, [branchId, bloodType]);
  
      console.log('donorQuery:', formattedDonorQuery);
  
      const [donorRows] = await pool.execute(formattedDonorQuery);
      console.log(`Donors with blood type ${bloodType} in branch ${branchId}:`, donorRows);
  
      const totalBloodUnits = donorRows.reduce((sum, row) => sum + row.units_of_blood, 0);
      console.log(`Total blood units available in branch ${branchId} with blood type ${bloodType}: ${totalBloodUnits}`);
  
      if (totalBloodUnits >= units) {
        console.log(`Blood search for ${units} units in branch ${branchId} with blood type ${bloodType}. Total units: ${totalBloodUnits}`);
  
        const sortedDonors = donorRows.sort((a, b) => a.units_of_blood - b.units_of_blood);
  
        for (const donor of sortedDonors) {
          const remainingUnits = donor.units_of_blood - units;
  
          if (remainingUnits >= 0) {
            await pool.execute('UPDATE donor SET units_of_blood = ? WHERE iddonor = ?', [remainingUnits, donor.iddonor]);
            console.log(`Updated donor ${donor.iddonor}. Remaining units: ${remainingUnits}`);
            break;
          } else {
            await pool.execute('UPDATE donor SET units_of_blood = 0 WHERE iddonor = ?', [donor.iddonor]);
            units -= donor.units_of_blood;
            console.log(`Updated donor ${donor.iddonor}. Remaining units: 0. ${units} units left.`);
          }
        }
  
        return 'successful';
      } else {
        console.log(`Not enough blood units available in branch ${branchId}. Total units: ${totalBloodUnits}`);
        return 'rabbitmq';
      }
    } catch (error) {
      console.error('Error searching blood:', error);
      return null;
    }
  };
  
  
  


module.exports = {
    getCoordinates,
    haversine,
    getAllBranches,
    searchBlood,
};

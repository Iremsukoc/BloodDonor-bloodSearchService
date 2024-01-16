const amqp = require('amqplib');
require('dotenv').config();
const processBloodRequest = require('./bloodSearchServiceLogic');
const cron = require('node-cron');
const sgMail = require('@sendgrid/mail');
sgMail.setApiKey(process.env.SG_MAIL_API_KEY);
const bloodQueue = 'blood_request_queue';
const express = require('express');

const app = express();
const port = 1012;
const cors = require('cors');


const allowedOrigins = [
  'https://blooddonor-bloodsearchservice-iremsu.onrender.com',
  'https://blooddonor-api-gateway-iremsu.onrender.com'
];


const corsOptions = {
  origin: function (origin, callback) {
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('Not allowed by CORS'));
    }
  }
};

app.use(cors(corsOptions));

const consumeFromRabbitMQ = async (queueName) => {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    console.log(`Connected to RabbitMQ for queue: ${queueName}`);

    await channel.assertQueue(queueName, { durable: true });
    console.log(`Queue '${queueName}' asserted`);

    channel.prefetch(1);

    console.log(`Waiting for messages on queue: ${queueName}. To exit, press CTRL+C`);

    const processMessage = async (msg) => {
      const currentTime = new Date().getTime();
      const messageContent = JSON.parse(msg.content.toString());
      console.log(`Received message from RabbitMQ on queue ${queueName}:`, messageContent);

      if (messageContent.processed) {
        console.log('Message already processed. Deleting from the queue.');
        channel.ack(msg);
        return;
      }

      const { selectedCity, selectedTown } = messageContent;

      try {
        const coordinates = await processBloodRequest.getCoordinates(selectedCity, selectedTown);
        const branches = await processBloodRequest.getAllBranches();

        if (branches.length === 0) {
          console.log('No branches found.');
          channel.ack(msg);
          return;
        }

        let bloodSearchResult = null;
        for (const branch of branches) {
          try {
            const branchCoordinates = await processBloodRequest.getCoordinates(branch.city, branch.town);
            const distance = processBloodRequest.haversine(
              coordinates.latitude,
              coordinates.longitude,
              branchCoordinates.latitude,
              branchCoordinates.longitude
            );

            if (distance < 50) {
              console.log(`Found a match at branch: ${branch.city}, ${branch.town}, Distance: ${distance} km`);
              bloodSearchResult = await processBloodRequest.searchBlood(branch.idbranch, messageContent.bloodType, messageContent.units);
              break;
            } else {
              console.log(`No match at branch: ${branch.city}, ${branch.town}, Distance: ${distance} km`);
            }
          } catch (error) {
            console.error(`Error processing branch: ${branch.city}, ${branch.town}`, error);
          }
        }

        if (bloodSearchResult === 'successful') {
          console.log(`Blood search successful. Deleting message from the queue ${queueName}.`);
          messageContent.processed = true;
          const emailResult = await sendEmail(messageContent.email);
          console.log(emailResult);
          channel.ack(msg);
        } else {
          if (Number(messageContent.duration) > 0) {
            console.log(`Blood search unsuccessful. Re-queueing message to retry queue. Remaining duration: ${messageContent.duration}`);
            
            messageContent.duration = Math.max(0, Number(messageContent.duration) - 1);
            
            channel.sendToQueue(queueName, Buffer.from(JSON.stringify(messageContent)));
          } else {
            console.log(`Blood search unsuccessful. Deleting message from the queue ${queueName} as duration is 0.`);
            messageContent.processed = true;
            channel.ack(msg);
          }
        }
      } catch (error) {
        console.error(`Error processing blood request on queue ${queueName}:`, error);
        channel.ack(msg);
      }
    };

    channel.consume(queueName, processMessage, { noAck: false });
  } catch (error) {
    console.error(`Error consuming messages from RabbitMQ on queue ${queueName}:`, error);
  }
};

cron.schedule('0 1 * * *', async () => {
  await consumeFromRabbitMQ(bloodQueue);
  console.log('Job at 1 AM Completed');
});


app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});


async function clearQueue(queueName) {
  try {
    const connection = await amqp.connect('amqp://localhost');
    console.log('Connected to RabbitMQ');

    const channel = await connection.createChannel();
    console.log('Channel created');

    await channel.assertQueue(queueName, { durable: true });
    console.log(`Queue '${queueName}' asserted`);

    const purgeResult = await channel.purgeQueue(queueName);
    console.log(`Purged ${purgeResult.messageCount} messages from the queue '${queueName}'`);

    await connection.close();
    console.log('Connection closed');
  } catch (error) {
    console.error('Error clearing queue:', error);
  }
}

const queueName = 'blood_request_queue';
//clearQueue(queueName);




const sendEmail = async (to) => {
  const msg = {
    to : to,
    from: {
      name: 'Health Organization',
      email: 'iremsuikl@hotmail.com'
    },
    subject: "Blood Request",
    text: "The requested blood type  found",
  };

  try {
    await sgMail.send(msg) ;
    console.log('Email sent successfully.');
  } catch (error) {
    console.error('Error sending email:', error);
  }
};

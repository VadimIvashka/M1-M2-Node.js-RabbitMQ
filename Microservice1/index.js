const express = require('express');
const amqp = require('amqplib');
const winston = require('winston');

const app = express();
const port = process.env.PORT || 3000;
const rabbitmqUrl = 'http://localhost:15672'; // Замените на URL вашего RabbitMQ сервера
const queueName = 'task_queue'; // Название очереди для обработки заданий М2

// Настройка Winston
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.json(),
    defaultMeta: { service: 'microservice1' },
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'error.log', level: 'error' }),
        new winston.transports.File({ filename: 'combined.log' }),
    ],
});

app.use(express.json());

// Функция для отправки задания в очередь RabbitMQ
async function sendTaskToRabbitMQ(task) {
    try {
        const connection = await amqp.connect(rabbitmqUrl);
        const channel = await connection.createChannel();

        await channel.assertQueue(queueName, { durable: true });
        await channel.sendToQueue(queueName, Buffer.from(JSON.stringify(task)), {
            persistent: true,
        });

        logger.info('Задание успешно отправлено в очередь.');
        await channel.close();
        await connection.close();
    } catch (error) {
        logger.error('Ошибка при отправке задания в очередь:', error);
    }
}

// Обработчик для входящих HTTP запросов
app.post('/process', (req, res) => {
    // Получение данных из HTTP запроса
    const requestData = req.body;

    // Отправка задания в очередь RabbitMQ
    sendTaskToRabbitMQ(requestData);

    res.status(202).json({ message: 'Задание принято. Ожидайте результат обработки.' });
});

// Запуск сервера
app.listen(port, () => {
    logger.info(`Микросервис М1 запущен на порту ${port}`);
});
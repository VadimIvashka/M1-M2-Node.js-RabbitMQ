const amqp = require('amqplib');
const winston = require('winston');

const rabbitmqUrl = 'http://localhost:15672'; // Замените на URL вашего RabbitMQ сервера
const queueName = 'task_queue'; // Название очереди для обработки заданий

// Настройка логгера Winston
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.json(),
    defaultMeta: { service: 'microservice2' },
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'error.log', level: 'error' }),
        new winston.transports.File({ filename: 'combined.log' }),
    ],
});

// Функция для обработки задания
async function processTask(task) {
    // TODO: Реализовать обработку задания
    // В данном примере просто эмулируем обработку задания с помощью setTimeout
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve(`Результат обработки задания: ${JSON.stringify(task)}`);
        }, 3000); // Эмулируем задержку в 3 секунды
    });
}

// Функция для получения сообщений из очереди RabbitMQ
async function receiveTaskFromRabbitMQ() {
    try {
        const connection = await amqp.connect(rabbitmqUrl);
        const channel = await connection.createChannel();

        await channel.assertQueue(queueName, { durable: true });
        await channel.prefetch(1); // Принимать только одно сообщение за раз

        logger.info('Микросервис М2 ожидает задания...');

        channel.consume(queueName, async (message) => {
            if (message !== null) {
                const task = JSON.parse(message.content.toString());

                logger.info('Получено задание:', task);

                // Обработка задания
                const result = await processTask(task);

                // Отправка результата обратно в RabbitMQ
                channel.sendToQueue(
                    message.properties.replyTo,
                    Buffer.from(JSON.stringify(result)),
                    {
                        correlationId: message.properties.correlationId,
                    }
                );

                // Подтверждение успешной обработки задания
                channel.ack(message);
            }
        });
    } catch (error) {
        logger.error('Ошибка при получении сообщения из очереди:', error);
    }
}

// Запуск микросервиса М2
receiveTaskFromRabbitMQ();
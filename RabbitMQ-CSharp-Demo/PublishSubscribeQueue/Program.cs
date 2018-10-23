using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace PublishSubscribeQueue
{
    class Program
    {
        static void Main(string[] args)
        {
            Receive();
        }

        private static void Publish()
        {
            using (var connection = _connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "logs", type: "fanout");

                var message = "Hello World!";
                var body = Encoding.UTF8.GetBytes(message);
                channel.BasicPublish(exchange: "logs", routingKey: "", basicProperties: null, body: body);
                Console.WriteLine(" [x] Sent {0}", message);
            }
            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

        private static void Receive()
        {
            using (var connection = _connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "logs", type: "fanout");

                var queueName = channel.QueueDeclare().QueueName;
                channel.QueueBind(queue: queueName, exchange: "logs", routingKey: "");

                Console.WriteLine(" [*] Waiting for logs.");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] {0}", message);
                };

                channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        private static ConnectionFactory _connectionFactory
        {
            get
            {
                return new ConnectionFactory()
                {
                    HostName = "172.18.21.226",
                    VirtualHost = "CommonDev",
                    Port = 5672,
                    UserName = "admin",
                    Password = "admin"
                };
            }
        }
    }
}

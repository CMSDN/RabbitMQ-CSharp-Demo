using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace SimpleQueue
{
    class Program
    {
        static void Main(string[] args)
        {
            int i = 0;
            do
            {
                i++;
                Send();
            } while (i <= 100);
        }

        private static void Send()
        {
            using (var connection = GetConnectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "SimpleQueue", durable: false, exclusive: false, autoDelete: false, arguments: null);

                string message = "Hello World!";
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(exchange: "", routingKey: "SimpleQueue", basicProperties: null, body: body);

                Console.WriteLine(" [x] Sent {0}", message);
            }
            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

        private static void Receive()
        {
            using (var connection = GetConnectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "SimpleQueue", durable: false, exclusive: false, autoDelete: false, arguments: null);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (modal, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] Received {0}", message);
                };

                channel.BasicConsume(queue: "SimpleQueue", autoAck: true, consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        private static ConnectionFactory GetConnectionFactory
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

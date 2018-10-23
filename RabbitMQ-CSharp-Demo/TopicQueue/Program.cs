using System;
using System.Linq;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace TopicQueue
{
    class Program
    {
        static void Main(string[] args)
        {
            //while (true)
            //{
            //    string[] a = { "log.test.info", "HelloWorld", Guid.NewGuid().ToString() };
            //    EmitLogTopic(a);
            //}
            string[] b = { "log.test.*", "log.#", "log.test.#" };
            ReceiveLogstopic(b);
        }

        private static void EmitLogTopic(string[] args)
        {
            using (var connection = _connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");

                var routingKey = (args.Length > 0) ? args[0] : "anonymous.info";
                var message = (args.Length > 1)
                         ? string.Join(" ", args.Skip(1).ToArray())
                         : "Hello World!";
                var body = Encoding.UTF8.GetBytes(message);
                channel.BasicPublish(exchange: "topic_logs", routingKey: routingKey, basicProperties: null, body: body);
                Console.WriteLine(" [x] Sent '{0}':'{1}'", routingKey, message);
            }
            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

        private static void ReceiveLogstopic(string[] args)
        {
            using (var connection = _connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");
                var queueName = channel.QueueDeclare().QueueName;

                if (args.Length < 1)
                {
                    Console.Error.WriteLine("Usage: {0} [binding_key...]", Environment.GetCommandLineArgs()[0]);
                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                    Environment.ExitCode = 1;
                    return;
                }

                foreach (string bindingKey in args)
                {
                    channel.QueueBind(queue: queueName, exchange: "topic_logs", routingKey: bindingKey);
                }
                Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    var routingKey = ea.RoutingKey;
                    Console.WriteLine(" [x] Received '{0}':'{1}'", routingKey, message);
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

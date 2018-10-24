using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading.Tasks;

namespace RPCQueue
{
    class Program
    {
        static void Main(string[] args)
        {
            var rpcClient = new RpcClient();

            Console.WriteLine(" [x] Requesting fib(30)");
            var response = rpcClient.Call("30");

            Console.WriteLine(" [.] Got '{0}'", response);
            rpcClient.Close();

            //new RpcServer().Handler();
        }
    }

    public class RpcClient
    {
        private readonly IConnection _connection;
        private readonly IModel _channel;
        private readonly string _replyQueueName;
        private readonly EventingBasicConsumer _consumer;

        public RpcClient()
        {
            _connection = _connectionFactory.CreateConnection();
            _channel = _connection.CreateModel();
            _replyQueueName = _channel.QueueDeclare().QueueName;
            _consumer = new EventingBasicConsumer(_channel);
        }

        public string Call(string message)
        {
            var tcs = new TaskCompletionSource<string>();
            var resultTask = tcs.Task;

            var correlationId = Guid.NewGuid().ToString();

            IBasicProperties props = _channel.CreateBasicProperties();
            props.CorrelationId = correlationId;
            props.ReplyTo = _replyQueueName;

            EventHandler<BasicDeliverEventArgs> handler = null;
            handler = (model, ea) =>
            {
                _consumer.Received -= handler;

                var body = ea.Body;
                var response = Encoding.UTF8.GetString(body);

                tcs.SetResult(response);
            };
            _consumer.Received += handler;

            var messageBytes = Encoding.UTF8.GetBytes(message);
            _channel.BasicPublish(exchange: "", routingKey: "rpc_queue", basicProperties: props, body: messageBytes);

            _channel.BasicConsume(consumer: _consumer, queue: _replyQueueName, autoAck: true);

            return resultTask.Result;

        }

        public void Close()
        {
            _connection.Close();
        }

        private ConnectionFactory _connectionFactory
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

    public class RpcServer
    {
        public void Handler()
        {
            using (var connection = _connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "rpc_queue", durable: false, exclusive: false, autoDelete: true, arguments: null);
                channel.BasicQos(0, 1, false);
                var consumer = new EventingBasicConsumer(channel);
                channel.BasicConsume(queue: "rpc_queue", autoAck: false, consumer: consumer);
                Console.WriteLine(" [x] Awaiting RPC requests");

                consumer.Received += (model, ea) =>
                {
                    string response = null;

                    var body = ea.Body;
                    var props = ea.BasicProperties;
                    var replyProps = channel.CreateBasicProperties();
                    replyProps.CorrelationId = props.CorrelationId;

                    try
                    {
                        var message = Encoding.UTF8.GetString(body);
                        int n = int.Parse(message);
                        Console.WriteLine(" [.] fib({0})", message);
                        response = fib(n).ToString();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(" [.] " + e.Message);
                        response = "";
                    }
                    finally
                    {
                        Console.WriteLine("Sent Message:{0}", response);
                        var responseBytes = Encoding.UTF8.GetBytes(response);
                        channel.BasicPublish(exchange: "", routingKey: props.ReplyTo, basicProperties: replyProps, body: responseBytes);
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    }
                };
                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        public int fib(int n)
        {
            if (n == 0 || n == 1)
            {
                return n;
            }
            return fib(n - 1) + fib(n - 2);
        }

        private ConnectionFactory _connectionFactory
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

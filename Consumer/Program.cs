using Confluent.Kafka;
using Serilog;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();
            
            logger.Information("Testing message consumption with Kafka");

            if (args.Length != 2)
            {
                logger.Error(
                    "Enter at least 2 parameters:\n" +
                    "1) IP:port hosting Kafka\n" +
                    "2) The Topic to be used in the consumption of messages\n\n" +
                    "Example: dotnet run \"localhost:9092\" \"topic-example\"");
                return;
            }

            string bootstrapServers = args[0];
            string nomeTopic = args[1];

            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {nomeTopic}");

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"{nomeTopic}-group-0",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumer.Subscribe(nomeTopic);

                    try
                    {
                        while (true)
                        {
                            var cr = consumer.Consume(cts.Token);
                            logger.Information(
                                $"Message read: {cr.Message.Value}");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumer.Close();
                        logger.Warning("Canceled execution of Consumer...");
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error($"Exception: {ex.GetType().FullName} | " +
                             $"Message: {ex.Message}");
            }
        }
    }
}

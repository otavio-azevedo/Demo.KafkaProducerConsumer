using Confluent.Kafka;
using Serilog;

namespace Producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();

            logger.Information("Testing sending messages with Kafka");

            if (args.Length < 3)
            {
                logger.Error(
                    "Enter at least 3 parameters:\n" +
                    "1) IP:port hosting Kafka\n" +
                    "2) The Topic that will receive the messages\n" +
                    "3) The messages to be sent to a Topic in Kafka\n\n" +
                    "Example: dotnet run \"localhost:9092\" \"topic-example\" \"Hi\" \"How are you?\"");
                return;
            }

            string bootstrapServers = args[0];
            string nomeTopic = args[1];

            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {nomeTopic}");

            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    for (int i = 2; i < args.Length; i++)
                    {
                        var result = await producer.ProduceAsync(
                            nomeTopic,
                            new Message<Null, string>
                            { Value = args[i] });

                        logger.Information(
                            $"Message: {args[i]} | " +
                            $"Status: { result.Status.ToString()}");
                    }
                }

                logger.Information("Finished sending messages");
            }
            catch (Exception ex)
            {
                logger.Error($"Exception: {ex.GetType().FullName} | " +
                             $"Message: {ex.Message}");
            }
        }
    }
}
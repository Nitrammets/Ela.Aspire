using System.Text;
using RabbitMQ.Client;
using Wolverine;

namespace WorkerService1;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var message = $"Hello World! {DateTimeOffset.Now}";
            await PublishMessage(message);

            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Worker published message at: {time}", DateTimeOffset.Now);
            }

            await Task.Delay(5000, stoppingToken); // Publish every 5 seconds
        }
    }

    private Task PublishMessage(string message)
    {
        return Task.CompletedTask;
    }
}
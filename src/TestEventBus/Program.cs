// See https://aka.ms/new-console-template for more information

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventBus.Abstractions;
using EventBus.Events;
using EventBusRabbitMq;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;


var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHostedService<TestPublisherService>();
builder.Services.AddHostedService<TestConsumerService>();

var eventBusBuilder = builder.AddRabbitMqEventBus("eventbus")
    .AddSubscription<TestIntegrationEvent, TestIntegrationEventHandler>();

var host = builder.Build();
host.Run();

public class TestIntegrationEvent : IntegrationEvent
{
    public string Message { get; }

    public TestIntegrationEvent(string message)
    {
        Message = message;
    }
}

// Define a test event handler
public class TestIntegrationEventHandler : IIntegrationEventHandler<TestIntegrationEvent>
{
    private readonly ILogger<TestIntegrationEventHandler> _logger;

    public TestIntegrationEventHandler(ILogger<TestIntegrationEventHandler> logger)
    {
        _logger = logger;
    }

    public Task Handle(TestIntegrationEvent @event)
    {
        _logger.LogInformation("Received TestIntegrationEvent with message: {@Message}", @event.Message);
        return Task.CompletedTask;
    }
}


public class TestPublisherService : BackgroundService
{
    private readonly IEventBus _eventBus;
    private readonly ILogger<TestPublisherService> _logger;
    private const int PublisherCount = 8;

    public TestPublisherService(IEventBus eventBus, ILogger<TestPublisherService> logger)
    {
        _eventBus = eventBus;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var publisherTasks = Enumerable.Range(0, PublisherCount)
            .Select(i => PublisherTask(i, stoppingToken))
            .ToArray();

        return Task.WhenAll(publisherTasks);
    }

    private async Task PublisherTask(int publisherId, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var message = $"Test message from Publisher {publisherId} at {DateTime.UtcNow}";
            var @event = new TestIntegrationEvent(message);

            try
            {
                await _eventBus.PublishAsync(@event);
                _logger.LogInformation("Publisher {PublisherId} published event: {@Event}", publisherId, @event);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Publisher {PublisherId} failed to publish event", publisherId);
            }

            await Task.Delay(TimeSpan.FromMilliseconds(1), stoppingToken);
        }
    }
}

public class TestConsumerService : IHostedService
{
    private readonly IEventBus _eventBus;

    public TestConsumerService(IEventBus eventBus)
    {
        _eventBus = eventBus;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // The RabbitMqEventBus should start consuming automaticallyR
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
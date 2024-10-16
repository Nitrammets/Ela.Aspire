using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using EventBus.Abstractions;
using EventBus.Events;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace EventBusRabbitMq;

public sealed class RabbitMqEventBus : IEventBus, IDisposable, IHostedService
{
    private readonly ILogger<RabbitMqEventBus> _logger;
    private IServiceProvider _serviceProvider;
    private IOptions<EventBusOptions> _eventBusOptions;
    private IOptions<EventBusSubscriptionInfo> _eventBusSubscriptionInfo;
    
    
    private const string ExchangeName = "ela_event_bus";

    private readonly ResiliencePipeline _pipeline;
    private readonly string _queueName;
    private IConnection _rabbitMqConnection;
    
    private IModel _consumerChannel;

    public RabbitMqEventBus(ILogger<RabbitMqEventBus> logger, IServiceProvider serviceProvider, IOptions<EventBusOptions> eventBusOptions, IOptions<EventBusSubscriptionInfo> eventBusSubscriptionInfo, IConnection rabbitMqConnection)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
        _eventBusOptions = eventBusOptions;
        _eventBusSubscriptionInfo = eventBusSubscriptionInfo;
        _rabbitMqConnection = rabbitMqConnection;

        _pipeline = CreateResiliencePipeline(_eventBusOptions.Value.RetryCount);
        _queueName = _eventBusOptions.Value.SubscriptionClientName;
    }

    public Task PublishAsync(IntegrationEvent @event)
    {
        var routingKey = @event.GetType().Name;
        
        if (_logger.IsEnabled(LogLevel.Trace))
        {
            _logger.LogTrace("Creating RabbitMQ channel to publish event: {EventId} ({EventName})", @event.Id, routingKey);
        }
        
        using var channel = _rabbitMqConnection.CreateModel() ?? throw new InvalidOperationException("RabbitMQ connection is not open");

        if (_logger.IsEnabled(LogLevel.Trace))
        {
            _logger.LogTrace("Declaring RabbitMQ exchange to publish event: {EventId}", @event.Id);
        }
        
        channel.ExchangeDeclare(exchange: ExchangeName, type: "direct");

        var body = SerializeMessage(@event);

        return _pipeline.Execute(() =>
        {
            var properties = channel.CreateBasicProperties();
            properties.DeliveryMode = 2;

            try
            {
                channel.BasicPublish(
                    exchange: ExchangeName,
                    routingKey: routingKey,
                    mandatory: true,
                    basicProperties: properties,
                    body: body
                );
                return Task.CompletedTask;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to publish event: {EventId}", @event.Id);
                throw;
            }
        });
    }

    private async Task OnMessageReceived(object sender, BasicDeliverEventArgs eventArgs)
    {
        var eventName = eventArgs.RoutingKey;
        var message = Encoding.UTF8.GetString(eventArgs.Body.Span);

        try
        {
            await ProcessEvent(eventName, message);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error Processing message \"{Message}\"", message);
        }
        
        // in a REAL WORLD app this should be handled with a Dead Letter Exchange (DLX). 
        _consumerChannel.BasicAck(eventArgs.DeliveryTag, multiple: false);
    }

    private async Task ProcessEvent(string eventName, string message)
    {
        if (_logger.IsEnabled(LogLevel.Trace))
        {
            _logger.LogTrace("Processing RabbitMQ event: {EventName}", eventName);
        }

        await using var scope = _serviceProvider.CreateAsyncScope();
        
        if (!_eventBusSubscriptionInfo.Value.EventTypes.TryGetValue(eventName, out var eventType))
        {
            _logger.LogWarning("Unable to resolve event type for event name {EventName}", eventName);
            return;
        }
        
        var integrationEvent = DeserializeMessage(message, eventType);

        // REVIEW: This could be done in parallel
        
        foreach (var handler in scope.ServiceProvider.GetKeyedServices<IIntegrationEventHandler>(eventType))
        {
            await handler.Handle(integrationEvent);
        }
    }

    private IntegrationEvent DeserializeMessage(string message, Type eventType)
    {
        return JsonSerializer.Deserialize(message, eventType, _eventBusSubscriptionInfo.Value.JsonSerializerOptions) as IntegrationEvent ?? throw new InvalidOperationException("Can't deserialize event");
    }


    private byte[] SerializeMessage(IntegrationEvent @event)
    {
        return JsonSerializer.SerializeToUtf8Bytes(@event, @event.GetType(),
            _eventBusSubscriptionInfo.Value.JsonSerializerOptions);
    }

    public void Dispose()
    {
        _consumerChannel?.Dispose();
    }


    public Task StartAsync(CancellationToken cancellationToken)
    {
        _ = Task.Factory.StartNew(() =>
        {
            try
            {
                _logger.LogInformation("Starting RabbitMQ connection on a background thread");

                _rabbitMqConnection = _serviceProvider.GetRequiredService<IConnection>();
                if (!_rabbitMqConnection.IsOpen)
                {
                    _logger.LogWarning("RabbitMQ Connection is not open");
                    return;
                }

                if (_logger.IsEnabled(LogLevel.Trace))
                {
                    _logger.LogTrace("Creating RabbitMQ consumer channel");
                }

                _consumerChannel = _rabbitMqConnection.CreateModel();

                _consumerChannel.CallbackException += (sender, ea) =>
                {
                    _logger.LogWarning(ea.Exception, "Error with RabbitMQ consumer channel");
                };

                _consumerChannel.ExchangeDeclare(exchange: ExchangeName, type: "direct");

                _consumerChannel.QueueDeclare(queue: _queueName, durable: true, exclusive: false, autoDelete: false,
                    arguments: null);

                if (_logger.IsEnabled(LogLevel.Trace))
                {
                    _logger.LogTrace("Starting RabbitMQ basic consume");
                }

                var consumer = new AsyncEventingBasicConsumer(_consumerChannel);

                consumer.Received += OnMessageReceived;

                _consumerChannel.BasicConsume(
                    queue: _queueName,
                    autoAck: false,
                    consumer: consumer
                );

                foreach (var (eventName, _) in _eventBusSubscriptionInfo.Value.EventTypes)
                {
                    _consumerChannel.QueueBind(
                        queue: _queueName,
                        exchange: ExchangeName,
                        routingKey: eventName
                    );
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error starting RabbitMQ connection");
            }
        }, TaskCreationOptions.LongRunning);

        return Task.CompletedTask;
    }
    
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
    
    private static ResiliencePipeline CreateResiliencePipeline(int retryCount)
    {
        var retryOptions = new RetryStrategyOptions
        {
            ShouldHandle = new PredicateBuilder().Handle<BrokerUnreachableException>().Handle<SocketException>(),
            MaxRetryAttempts = retryCount,
            DelayGenerator = (context) => ValueTask.FromResult(GenerateDelay(context.AttemptNumber))
        };

        return new ResiliencePipelineBuilder()
            .AddRetry(retryOptions)
            .Build();

        static TimeSpan? GenerateDelay(int attempt)
        {
            return TimeSpan.FromSeconds(Math.Pow(2, attempt));
        }
    }
}
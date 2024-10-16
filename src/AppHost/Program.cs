var builder = DistributedApplication.CreateBuilder(args);

var rabbitMqUserName = builder.AddParameter("username", true);
var rabbitMqUserPassword = builder.AddParameter("password", true);

var rabbitMq = builder
    .AddRabbitMQ("eventbus", rabbitMqUserName, rabbitMqUserPassword, 6969)
    .WithManagementPlugin();

var testService = builder.AddProject<Projects.TestEventBus>("event-bus-test")
    .WithReference(rabbitMq)
    .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development");


builder.Build().Run();
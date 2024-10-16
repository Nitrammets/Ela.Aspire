using WorkerService1;




var builder = Host.CreateDefaultBuilder(args);


builder.Services.AddHostedService<Worker>();





var host = builder.Build();



host.Run();
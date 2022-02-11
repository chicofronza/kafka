using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace Kakca.Consumer.Handler.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ConsumerController : ControllerBase
    {
        private readonly ILogger<ConsumerController> _logger;
        public ConsumerController(ILogger<ConsumerController> logger)
        {
            _logger = logger;            
        }

        [HttpGet]
        public string Get()
        {
            try
            {
                StartAsync(new CancellationToken());
                return "Mensagem consumida com sucesso";
            }
            catch (System.Exception ex)
            {
                return $"Erro ao consumir a mensagem: {ex.Message}";
            }
        }  
    
        private void StartAsync(CancellationToken cancellationToken)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                //canal de leitura das mensagens
                c.Subscribe("fila_pedido");
                var cts = new CancellationTokenSource();

                try
                {
                    while (true)
                    {
                        var message = c.Consume(cts.Token);
                        var log = $"Mensagem: {message.Value} recebida de {message.TopicPartitionOffset}";
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }
        }
        
    }
}
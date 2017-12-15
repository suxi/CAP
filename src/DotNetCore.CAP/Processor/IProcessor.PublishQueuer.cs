using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Infrastructure;
using DotNetCore.CAP.Models;
using DotNetCore.CAP.Processor.States;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics;

namespace DotNetCore.CAP.Processor
{
    public class PublishQueuer : IProcessor
    {
        public static readonly AutoResetEvent PulseEvent = new AutoResetEvent(true);
        private readonly ILogger _logger;
        private readonly TimeSpan _pollingDelay;
        private readonly IServiceProvider _provider;
        private readonly IStateChanger _stateChanger;

        public PublishQueuer(
            ILogger<PublishQueuer> logger,
            IOptions<CapOptions> options,
            IStateChanger stateChanger,
            IServiceProvider provider)
        {
            _logger = logger;
            _stateChanger = stateChanger;
            _provider = provider;

            var capOptions = options.Value;
            _pollingDelay = TimeSpan.FromSeconds(capOptions.PollingDelay);
        }

        public async Task ProcessAsync(ProcessingContext context)
        {
            _logger.LogDebug("Publish Queuer start calling.");
            using (var scope = _provider.CreateScope())
            {
                CapPublishedMessage sentMessage;
                var provider = scope.ServiceProvider;
                var connection = provider.GetRequiredService<IStorageConnection>();

                while (
                    !context.IsStopping &&
                    (sentMessage = await connection.GetNextPublishedMessageToBeEnqueuedAsync()) != null)

                {
                    var state = new EnqueuedState();
                    using (var transaction = await connection.CreateTransaction())
                    {
                        //_stateChanger.ChangeState(sentMessage, state, transaction);
                        var now = DateTime.Now;
                        if (state.ExpiresAfter != null)
                            sentMessage.ExpiresAt = now.Add(state.ExpiresAfter.Value);
                        else
                            sentMessage.ExpiresAt = null;

                        sentMessage.StatusName = state.Name;
                        transaction.UpdateMessage(sentMessage);
                        await transaction.CommitAsync();
                    }
                    using (var transaction = await connection.CreateTransaction())
                    {
                        var queueExecutors = provider.GetServices<IQueueExecutor>();
                        IQueueExecutor executor = queueExecutors.FirstOrDefault(x => x is BasePublishQueueExecutor);
                        IFetchedMessage fetchedMessage = new InnerFetchMessage
                        {
                            MessageId = sentMessage.Id,
                            MessageType = MessageType.Publish
                        };

                        await executor.ExecuteAsync(connection, fetchedMessage);
                        if (fetchedMessage.Status)
                        {
                            sentMessage.StatusName = new SucceededState().Name;
                            transaction.UpdateMessage(sentMessage);
                            await transaction.CommitAsync();
                        }
                        else
                        {
                            sentMessage.StatusName = new ScheduledState().Name;
                            transaction.UpdateMessage(sentMessage);
                            await transaction.CommitAsync();
                        }
                    }
                }
            }

            context.ThrowIfStopping();

            DefaultDispatcher.PulseEvent.Set();

            await WaitHandleEx.WaitAnyAsync(PulseEvent,
                context.CancellationToken.WaitHandle, _pollingDelay);


        }

        private class InnerFetchMessage : IFetchedMessage
        {
            public int MessageId { get; set; }
            public MessageType MessageType { get; set; }

            public bool Status { get; set; }

            public void Dispose()
            {
                
            }

            public void RemoveFromQueue()
            {
                Status = true;
            }

            public void Requeue()
            {
                Status = false;   
            }
        }
    }
}
namespace NContext.EventHandling
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.ComponentModel.Composition;
    using System.ComponentModel.Composition.Hosting;
    using System.Linq;
    using System.Threading.Tasks;

    using NContext.Configuration;
    using NContext.Extensions;

    /// <summary>
    /// Defines an application component for event handling.
    /// </summary>
    public class EventManager : IManageEvents
    {
        private static readonly ConcurrentDictionary<Type, IEnumerable<Type>> _EventHandlerCache;

        private static IActivationProvider _ActivationProvider;

        private static CompositionContainer _CompositionContainer;

        private static IEnumerable<Type> _ConditionalEventHandlers;

        private Boolean _IsConfigured;

        static EventManager()
        {
            _EventHandlerCache = new ConcurrentDictionary<Type, IEnumerable<Type>>();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="EventManager"/> class.
        /// </summary>
        /// <param name="activationProvider">The activation provider.</param>
        public EventManager(IActivationProvider activationProvider)
        {
            _ActivationProvider = activationProvider;
        }

        /// <summary>
        /// Raises the specified event.
        /// </summary>
        /// <typeparam name="TEvent">The type of the event.</typeparam>
        /// <param name="event">The event.</param>
        /// <returns>Task.</returns>
        public Task Raise<TEvent>(TEvent @event)
        {
            return RaiseEvent(@event);
        }

        /// <summary>
        /// Raises the specified event.
        /// </summary>
        /// <typeparam name="TEvent">The type of the event.</typeparam>
        /// <param name="event">The event.</param>
        /// <returns>Task.</returns>
        public static Task RaiseEvent<TEvent>(TEvent @event)
        {
            return Task.Run(() => RaiseEventInternal(@event));
        }

        private static async Task<Object> RaiseEventInternal<TEvent>(TEvent @event)
        {
            IEnumerable<Type> handlerTypes;
            if (_EventHandlerCache.ContainsKey(typeof(TEvent)))
            {
                handlerTypes = _EventHandlerCache[typeof(TEvent)];
            }
            else
            {
                if (_CompositionContainer == null)
                {
                    // not configured correctly, do not attempt to raise event
                    return null;
                }

                handlerTypes = _EventHandlerCache.GetOrAdd(
                    typeof(TEvent), 
                    _CompositionContainer.GetExportTypesThatImplement<IHandleEvent<TEvent>>()
                        .Concat(_CompositionContainer.GetExportTypesThatImplement<IHandleEventAsync<TEvent>>()).ToList());
            }

            var tcs = new TaskCompletionSource<Object>();
            var exceptions = new ConcurrentBag<Exception>();
            var completeHandlerTypes = handlerTypes.Concat(_ConditionalEventHandlers).ToList();

            await ForEachAsync(
                completeHandlerTypes,
                completeHandlerTypes.Count > Environment.ProcessorCount
                    ? Environment.ProcessorCount
                    : completeHandlerTypes.Count,
                    async handlerType =>
                    {
                        try
                        {
                            var handler = _ActivationProvider.CreateInstance<TEvent>(handlerType);
                            if (handler == null)
                            {
                                throw new Exception(String.Format("Activation provider could not create an instance of event handler: {0}.", handlerType.Name));
                            }

                            try
                            {
                                if (handlerType.Implements<IConditionallyHandleEvents>())
                                {
                                    var conditionalHandler = (IConditionallyHandleEvents)handler;
                                    if (conditionalHandler.CanHandle(@event))
                                    {
                                        await conditionalHandler.Handle(@event);
                                    }
                                }
                                else if (handlerType.Implements<IHandleEventAsync<TEvent>>())
                                {
                                    await ((IHandleEventAsync<TEvent>)handler).Handle(@event);
                                }
                                else
                                {
                                    ((IHandleEvent<TEvent>)handler).Handle(@event);
                                }
                            }
                            catch (Exception ex)
                            {
                                if (handler.GetType().Implements<IGracefullyHandleEvent<TEvent>>())
                                {
                                    ((IGracefullyHandleEvent<TEvent>)handler).HandleException(@event, ex);

                                    return;
                                }

                                exceptions.Add(ex);
                            }
                        }
                        catch (Exception ex)
                        {
                            exceptions.Add(ex);
                        }
                    });

            if (exceptions.Any())
            {
                tcs.SetException(new AggregateException(exceptions));
            }
            else
            {
                tcs.SetResult(null);
            }

            return await tcs.Task;
        }

        /// <summary>
        /// Gets a value indicating whether this instance is configured.
        /// </summary>
        /// <value>The is configured.</value>
        public Boolean IsConfigured
        {
            get { return _IsConfigured; }
        }

        /// <summary>
        /// Configures the component instance. This method should set <see cref="IsConfigured" />.
        /// </summary>
        /// <param name="applicationConfiguration">The application configuration.</param>
        public void Configure(ApplicationConfigurationBase applicationConfiguration)
        {
            if (_IsConfigured)
            {
                return;
            }

            applicationConfiguration.CompositionContainer.ComposeExportedValue<IManageEvents>(this);
            _CompositionContainer = applicationConfiguration.CompositionContainer;
            _ConditionalEventHandlers = _CompositionContainer.GetExportTypesThatImplement<IConditionallyHandleEvents>().ToList();

            _IsConfigured = true;
        }

        private static Task ForEachAsync<T>(IEnumerable<T> source, Int32 degreeOfParallelism, Func<T, Task> body)
        {
            return Task.WhenAll(
                from partition in Partitioner.Create(source).GetPartitions(degreeOfParallelism)
                select Task.Run(async () =>
                {
                    using (partition)
                        while (partition.MoveNext())
                            await body(partition.Current);
                }));
        }
    }
}
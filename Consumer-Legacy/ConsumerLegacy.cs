using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace EventHubs.Sample.Legacy
{
    public static class ConsumerLegacy
    {
        [FunctionName("ConsumerLegacy")]
        public static async Task Run([EventHubTrigger("devicetelemetryhub", Connection = "EventHubConnectionString")] EventData[] events, ILogger log)
        {
            //
            // Bind function with an array of EventData object
            // You can also bind with an array of string (string[] instead of EventData[])
            // https://docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-event-hubs-trigger?tabs=csharp
            // Using Microsoft.Azure.EventHubs ver 4.3.2
            // Ensure to install Microsoft.Azure.WebJobs.Extensions.Http ver 3.0.12 as well.
            //
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"Event From {eventData.SystemProperties["iothub-connection-device-id"]} : {messageBody}");
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}

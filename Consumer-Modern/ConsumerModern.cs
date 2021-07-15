using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace EventHubs.Sample.Modern
{
    public static class ConsumerModern
    {
        [FunctionName("ConsumerModern")]
        public static async Task Run([EventHubTrigger("devicetelemetryhub", Connection = "EventHubConnectionString")]EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            log.LogInformation($"Received EventHubTrigger with {events.GetType()}");

            foreach (EventData eventData in events)
            {
                try
                {
                    // EventData Class
                    // https://docs.microsoft.com/en-us/dotnet/api/azure.messaging.eventhubs.eventdata?view=azure-dotnet
                    log.LogInformation($"Event from {eventData.SystemProperties["iothub-connection-device-id"]} : {eventData.EventBody}");

                    // To display other system properties set by IoT Hub
                    //foreach (KeyValuePair<string, object> prop in eventData.SystemProperties)
                    //{
                    //    log.LogInformation($"\t\t{prop.Key}: {prop.Value}");
                    //}
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
